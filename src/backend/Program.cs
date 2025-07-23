using Dapper;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using Polly;
using Polly.Extensions.Http;
using Polly.Retry;
using StackExchange.Redis;
using System.Data;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.RateLimiting;
[module: DapperAot]

const string defaultProcessorName = "default";
const string fallbackProcessorName = "fallback";

var builder = WebApplication.CreateSlimBuilder(args);

builder.Services.AddSingleton<IConnectionMultiplexer>(sp =>
{
    var configuration = builder.Configuration.GetConnectionString("redis")!;
    var options = ConfigurationOptions.Parse(configuration);

    // Retry every 5 seconds, for up to 10 minutes (120 retries)
    var retryPolicy = Policy
        .Handle<Exception>() // catch ANY exception
        .WaitAndRetry(
            retryCount: 120,
            sleepDurationProvider: _ => TimeSpan.FromSeconds(5),
            onRetry: (exception, timeSpan, retryCount, context) =>
            {
                Console.WriteLine($"[Redis] Retry {retryCount}: {exception.GetType().Name} - {exception.Message}");
            });

    return retryPolicy.Execute(() =>
    {
        Console.WriteLine("[Redis] Attempting connection...");
        var muxer = ConnectionMultiplexer.Connect(options);

        if (!muxer.IsConnected)
            throw new Exception("Redis connection failed (IsConnected = false)");

        Console.WriteLine("[Redis] Connected successfully.");
        return muxer;
    });
});

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.TypeInfoResolverChain.Insert(0, JsonContext.Default);
    options.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
});

builder.Services.AddHttpClient(defaultProcessorName, o =>
    o.BaseAddress = new Uri(builder.Configuration.GetConnectionString(defaultProcessorName)!))
    .ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
    {
        MaxConnectionsPerServer = int.MaxValue, // Remove connection limit
        PooledConnectionLifetime = TimeSpan.FromMinutes(10),
        PooledConnectionIdleTimeout = TimeSpan.FromMinutes(5),
        EnableMultipleHttp2Connections = true, // Helps with HTTP/2
        ConnectTimeout = TimeSpan.FromSeconds(5),
        AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
    })
    .AddHttpMessageHandler<CountingHandler>();

builder.Services.AddHttpClient(fallbackProcessorName, o =>
    o.BaseAddress = new Uri(builder.Configuration.GetConnectionString(fallbackProcessorName)!))
    .ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
    {
        MaxConnectionsPerServer = int.MaxValue, // Remove connection limit
        PooledConnectionLifetime = TimeSpan.FromMinutes(10),
        PooledConnectionIdleTimeout = TimeSpan.FromMinutes(5),
        EnableMultipleHttp2Connections = true, // Helps with HTTP/2
        ConnectTimeout = TimeSpan.FromSeconds(5),
        AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
    })
    .AddHttpMessageHandler<CountingHandler>();

builder.Services.AddTransient<IDbConnection>(sp =>
    new NpgsqlConnection(builder.Configuration.GetConnectionString("postgres")));

builder.Services.AddTransient<CountingHandler>();

builder.Services.AddSingleton(_ =>
{
    return new AdaptativeLimiter(minLimitCount: 1, maxLimitCount: 10);
});

builder.Services.AddSingleton<BusyInstanceTracker>();
builder.Services.AddKeyedSingleton("tracker:postgres", (sp, _) =>
{
    var redis = sp.GetRequiredService<IConnectionMultiplexer>();
    return new BusyInstanceTracker(redis, "postgres");
});

builder.Services.AddSingleton<PaymentService>();
builder.Services.AddSingleton<AsyncBlockingGate>();
builder.Services.AddKeyedSingleton<AsyncBlockingGate>("channel:busy:postgres");
builder.Services.AddKeyedSingleton<AsyncBlockingGate>("channel:busy:http");
builder.Services.AddSingleton<PaymentBatchInserter>();
builder.Services.AddSingleton<RedisQueueWorker>();
builder.Services.AddHostedService(provider => provider.GetRequiredService<RedisQueueWorker>());

if (builder.Environment.IsProduction())
{
    builder.Logging.ClearProviders();
    builder.Logging.SetMinimumLevel(LogLevel.Error);
}

var app = builder.Build();

var redis = app.Services.GetRequiredService<IConnectionMultiplexer>();
var batchInserter = app.Services.GetRequiredService<PaymentBatchInserter>();
var blockingGate = app.Services.GetRequiredService<AsyncBlockingGate>();
var busyPostgresBlockingGate = app.Services.GetRequiredKeyedService<AsyncBlockingGate>("channel:busy:postgres");
var busyHttpBlockingGate = app.Services.GetRequiredKeyedService<AsyncBlockingGate>("channel:busy:http");

var subscriber = redis.GetSubscriber();

subscriber.Subscribe(
    RedisChannel.Literal("channel:busy:http"), (channel, message) =>
    {
        _ = Task.Run(async () =>
        {
            bool allIdle = await AreAllIdleAsync("busy:http").ConfigureAwait(false);

            if (allIdle)
            {
                await busyHttpBlockingGate.SetUnblockedAsync().ConfigureAwait(false);
            }
            else
            {
                await busyHttpBlockingGate.SetBlockedAsync().ConfigureAwait(false);
            }
        }).ConfigureAwait(false);
    });

subscriber.Subscribe(
    RedisChannel.Literal("channel:busy:postgres"), (channel, message) =>
    {
        _ = Task.Run(async () =>
        {
            bool allIdle = await AreAllIdleAsync("busy:postgres").ConfigureAwait(false);

            if (allIdle)
            {
                await busyPostgresBlockingGate.SetUnblockedAsync().ConfigureAwait(false);
            }
            else
            {
                await busyPostgresBlockingGate.SetBlockedAsync().ConfigureAwait(false);
            }
        }).ConfigureAwait(false);
    });



async Task<bool> AreAllIdleAsync(string hashKey)
{
    var values = await redis.GetDatabase().HashGetAllAsync(hashKey).ConfigureAwait(false);
    if (values.Length == 0) return true;

    foreach (var entry in values)
    {
        if (int.TryParse(entry.Value.ToString(), out var count) && count > 0)
            return false;
    }

    return true;
}


subscriber.Subscribe(
    RedisChannel.Literal("payments-summary-gate"),
    (channel, message) =>
    {
        _ = Task.Run(async () =>
        {
            var redisDb = redis.GetDatabase();
            Console.WriteLine($"[Redis] Received message on channel '{channel}': {message}");
            if (message == "1")
            {
                await blockingGate.SetBlockedAsync().ConfigureAwait(false);
                Console.WriteLine("[Redis] Gate blocked.");
                await blockingGate.WaitIfBlockedAsync(
                    whileBlockedLoopDelay: TimeSpan.FromMilliseconds(10),
                    whileBlockedAsync: async () =>
                    {

                        try
                        {
                            var processedCount = await batchInserter.FlushBatchAsync().ConfigureAwait(false);
                            if (processedCount > 0)
                            {
                                Console.WriteLine($"[Redis] Processed batch with {processedCount} records.");
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[Redis][Error] Exception while processing message: {ex}");
                        }
                    }).ConfigureAwait(false);
            }
            else if (message == "0")
            {
                await blockingGate.SetUnblockedAsync().ConfigureAwait(false);
                Console.WriteLine("[Redis] Gate unblocked.");
            }
        }).ConfigureAwait(false);
    });

    var apiGroup = app.MapGroup("/");
    apiGroup.MapGet("/", () => Results.Ok());

    apiGroup.MapPost("payments", async (HttpContext context, IConnectionMultiplexer redis) =>
    {
        using var ms = new MemoryStream();
        await context.Request.Body.CopyToAsync(ms);
        var rawBody = ms.ToArray();
        _ = Task.Run(async () =>
            {
                var db = redis.GetDatabase();
                var sub = redis.GetSubscriber();
                await db.ListRightPushAsync("task-queue", rawBody, flags: StackExchange.Redis.CommandFlags.FireAndForget).ConfigureAwait(false);
            });
        return Results.Accepted();
    });

    apiGroup.MapGet("/payments-summary", async ([FromQuery] DateTimeOffset? from, [FromQuery] DateTimeOffset? to, IDbConnection conn, IConnectionMultiplexer redisConn,
    [FromKeyedServices("channel:busy:http")] AsyncBlockingGate channelBlockingGate,
    [FromKeyedServices("channel:busy:postgres")] AsyncBlockingGate postgresChannelBlockingGate) =>
    {
        var redisDb = redisConn.GetDatabase();
        await redisConn.GetSubscriber().PublishAsync(
            RedisChannel.Literal("payments-summary-gate"),
            "1").ConfigureAwait(false);
        try
        {
            await WaitWithTimeoutAsync(async () =>
            {
                await postgresChannelBlockingGate.WaitIfBlockedAsync().ConfigureAwait(false);
                await channelBlockingGate.WaitIfBlockedAsync().ConfigureAwait(false);
            }, timeout: TimeSpan.FromSeconds(1.3)).ConfigureAwait(false);

            const string sql = @"
            SELECT processor,
                COUNT(*) AS total_requests,
                SUM(amount) AS total_amount
            FROM payments
            WHERE (@from IS NULL OR requested_at >= @from)
            AND (@to IS NULL OR requested_at <= @to)
            GROUP BY processor;
        ";
            List<PaymentSummaryResult> result = [.. await conn.QueryAsync<PaymentSummaryResult>(sql, new { from, to }).ConfigureAwait(false)];

            var defaultResult = result?.FirstOrDefault(r => r.Processor == defaultProcessorName) ?? new PaymentSummaryResult(defaultProcessorName, 0, 0);
            var fallbackResult = result?.FirstOrDefault(r => r.Processor == fallbackProcessorName) ?? new PaymentSummaryResult(fallbackProcessorName, 0, 0);

            var response = new PaymentSummaryResponse(
                new PaymentSummary(defaultResult.TotalRequests, defaultResult.TotalAmount),
                new PaymentSummary(fallbackResult.TotalRequests, fallbackResult.TotalAmount)
            );

            return Results.Ok(response);
        }
        finally
        {
            await redisConn.GetSubscriber().PublishAsync(
                RedisChannel.Literal("payments-summary-gate"),
                "0").ConfigureAwait(false);
        }
    });

    static async Task<bool> WaitWithTimeoutAsync(Func<Task> taskFactory, TimeSpan timeout)
    {
        var task = taskFactory();
        var timeoutTask = Task.Delay(timeout);
        var completedTask = await Task.WhenAny(task, timeoutTask).ConfigureAwait(false);

        if (completedTask == timeoutTask)
        {
            Console.WriteLine($"[Timeout] Task did not complete within {timeout.TotalMilliseconds}ms.");
            return false;
        }

        await task.ConfigureAwait(false);
        return true;
    }


    apiGroup.MapPost("/purge-payments", async (IDbConnection conn, IConnectionMultiplexer redisConn) =>
    {
        var redisDb = redisConn.GetDatabase();
        const string sql = "TRUNCATE TABLE payments";
        await conn.ExecuteAsync(sql).ConfigureAwait(false);
    });

    app.Run();

public sealed record ProcessorPaymentRequest(
    decimal Amount,
    DateTimeOffset RequestedAt,
    Guid CorrelationId
);
public record PaymentInsertParameters(
    Guid CorrelationId,
    string Processor,
    decimal Amount,
    DateTimeOffset RequestedAt
);

public record PaymentSummaryResult(
    string Processor,
    long TotalRequests,
    decimal TotalAmount
);
public record PaymentSummary(
    long TotalRequests,
    decimal TotalAmount
);

public record PaymentSummaryResponse(
    PaymentSummary Default,
    PaymentSummary Fallback
);

public record PaymentRequest(
    Guid CorrelationId,
    decimal Amount
);

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(ProcessorPaymentRequest))]
[JsonSerializable(typeof(PaymentRequest))]
[JsonSerializable(typeof(PaymentSummaryResponse))]
internal partial class JsonContext : JsonSerializerContext
{

}
