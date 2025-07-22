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
using System.Text.Json.Serialization;
using System.Threading.RateLimiting;
[module: DapperAot]

static IAsyncPolicy<HttpResponseMessage> GetRetryPolicy(IDatabase redisDb, AsyncBlockingGate blockingGate)
{
    return HttpPolicyExtensions
        .HandleTransientHttpError()
        .WaitAndRetryAsync(
            7,
            sleepDurationProvider: _ => TimeSpan.Zero,
            onRetryAsync: async (outcome, timespan, retryCount, context) =>
            {
                var sw = Stopwatch.StartNew();
                await LockChecks(redisDb, blockingGate);
                var remaining = TimeSpan.FromMilliseconds(10 * Math.Pow(2, retryCount - 1)) - sw.Elapsed;
                if (remaining > TimeSpan.Zero)
                    await Task.Delay(remaining).ConfigureAwait(false);
                await LockChecks(redisDb, blockingGate);
            });
}

static IAsyncPolicy<HttpResponseMessage> GetFallbackRetryPolicy(IDatabase redisDb, AsyncBlockingGate blockingGate)
{
    var retryPolicy = HttpPolicyExtensions
        .HandleTransientHttpError()
        .WaitAndRetryForeverAsync(
            sleepDurationProvider: _ => TimeSpan.Zero,
            onRetryAsync: async (outcome, timespan, context) =>
            {
                var sw = Stopwatch.StartNew();
                await LockChecks(redisDb, blockingGate);
                var remaining = TimeSpan.FromMilliseconds(10) - sw.Elapsed;
                if (remaining > TimeSpan.Zero)
                    await Task.Delay(remaining).ConfigureAwait(false);
                await LockChecks(redisDb, blockingGate);
            });

    var timeoutPolicy = Policy.TimeoutAsync<HttpResponseMessage>(TimeSpan.FromMinutes(1));

    return Policy.WrapAsync(retryPolicy, timeoutPolicy);
}

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
    .AddPolicyHandler((sp, req) =>
    {
        var redis = sp.GetRequiredService<IConnectionMultiplexer>();
        var gate = sp.GetRequiredService<AsyncBlockingGate>();
        var db = redis.GetDatabase();
        return GetRetryPolicy(db, gate);
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
    .AddPolicyHandler((sp, req) =>
    {
        var redis = sp.GetRequiredService<IConnectionMultiplexer>();
        var gate = sp.GetRequiredService<AsyncBlockingGate>();
        var db = redis.GetDatabase();
        return GetFallbackRetryPolicy(db, gate);
    })
    .AddHttpMessageHandler<CountingHandler>();

builder.Services.AddTransient<IDbConnection>(sp =>
    new NpgsqlConnection(builder.Configuration.GetConnectionString("postgres")));

builder.Services.AddTransient<CountingHandler>();
builder.Services.AddSingleton<BusyInstanceTracker>();

builder.Services.AddSingleton(_ =>
{
    return new AdaptativeLimiter(minLimitCount: 1, maxLimitCount: 10);
});
builder.Services.AddKeyedSingleton("postgres", (_, _) =>
{
    return new AdaptativeLimiter(minLimitCount: 100, maxLimitCount: 500);
});
builder.Services.AddKeyedSingleton("worker", (_, _) =>
{
    return new AdaptativeLimiter(minLimitCount: 500, maxLimitCount: 1000);
});

builder.Services.AddSingleton<BusyMonitor>();
builder.Services.AddSingleton<AsyncBlockingGate>();
builder.Services.AddKeyedSingleton<AsyncBlockingGate>("channel:busy:http");
builder.Services.AddSingleton<PaymentBatchInserter>();
builder.Services.AddSingleton<BackgroundWorkerQueue>();
builder.Services.AddHostedService(provider => provider.GetRequiredService<BackgroundWorkerQueue>());

if (builder.Environment.IsProduction())
{
    builder.Logging.ClearProviders();
    builder.Logging.SetMinimumLevel(LogLevel.Error);
}

var app = builder.Build();

var redis = app.Services.GetRequiredService<IConnectionMultiplexer>();
var busyMonitor = app.Services.GetRequiredService<BusyMonitor>();
var batchInserter = app.Services.GetRequiredService<PaymentBatchInserter>();
var blockingGate = app.Services.GetRequiredService<AsyncBlockingGate>();
var busyHttpBlockingGate = app.Services.GetRequiredKeyedService<AsyncBlockingGate>("channel:busy:http");

var subscriber = redis.GetSubscriber();

await subscriber.SubscribeAsync(
    RedisChannel.Literal("channel:busy:http"), async (channel, message) =>
{
    bool allIdle = await busyMonitor.AreAllIdleAsync();

    if (allIdle)
    {
        await busyHttpBlockingGate.SetUnblockedAsync();
    }
    else
    {
        await busyHttpBlockingGate.SetBlockedAsync();
    }
});

await subscriber.SubscribeAsync(
    RedisChannel.Literal("payments-summary-gate"), 
    async (channel, message) =>
    {
        var redisDb = redis.GetDatabase();
        Console.WriteLine($"[Redis] Received message on channel '{channel}': {message}");
        if (message == "1")
        {
            await blockingGate.SetBlockedAsync();
            Console.WriteLine("[Redis] Gate blocked.");
            await blockingGate.WaitIfBlockedAsync(
                whileBlockedLoopDelay: TimeSpan.FromMilliseconds(10),
                whileBlockedAsync: async () =>
                {

                    try
                    {
                        var processedCount = await batchInserter.FlushBatchAsync();
                        if (processedCount > 0)
                        {
                            Console.WriteLine($"[Redis] Processed batch with {processedCount} records.");

                            const string lockKey = "payments-lock";

                            var newValue = await redisDb.StringDecrementAsync(lockKey, processedCount);
                            Console.WriteLine($"[Redis] Decremented '{lockKey}' by {processedCount}. New value: {newValue}");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[Redis][Error] Exception while processing message: {ex}");
                    }
                });
        }
        else if (message == "0")
        {
            await blockingGate.SetUnblockedAsync();
            Console.WriteLine("[Redis] Gate unblocked.");
        }
    });

var apiGroup = app.MapGroup("/");
apiGroup.MapGet("/", () => Results.Ok());

apiGroup.MapPost("payments", async ([FromBody] PaymentRequest request, BackgroundWorkerQueue queue, IServiceScopeFactory scopeFactory) =>
{

    await queue.EnqueueAsync(async ct =>
    {
        using var scope = scopeFactory.CreateScope();

        var factory = scope.ServiceProvider.GetRequiredService<IHttpClientFactory>();
        var limiter = scope.ServiceProvider.GetRequiredService<AdaptativeLimiter>();
        var redis = scope.ServiceProvider.GetRequiredService<IConnectionMultiplexer>();
        var batchInserter = scope.ServiceProvider.GetRequiredService<PaymentBatchInserter>();
        var blockingGate = scope.ServiceProvider.GetRequiredService<AsyncBlockingGate>();


        var httpDefault = factory.CreateClient(defaultProcessorName);
        var httpFallback = factory.CreateClient(fallbackProcessorName);

        var currentProcessor = defaultProcessorName;
        var success = false;
        var requestedAt = DateTimeOffset.UtcNow;
        var redisDb = redis.GetDatabase();
        await limiter.RunAsync(async (ct) =>
        {
            await blockingGate.WaitIfBlockedAsync();
            var response = await httpDefault.PostAsJsonAsync("/payments", new ProcessorPaymentRequest
            (
                request.Amount,
                requestedAt,
                request.CorrelationId
            ), JsonContext.Default.ProcessorPaymentRequest);
            if (!response.IsSuccessStatusCode)
            {
                
                await blockingGate.WaitIfBlockedAsync();
                requestedAt = DateTimeOffset.UtcNow;
                response = await httpFallback.PostAsJsonAsync("/payments", new ProcessorPaymentRequest
                (
                    request.Amount,
                    requestedAt,
                    request.CorrelationId
                ), JsonContext.Default.ProcessorPaymentRequest);
                if (!response.IsSuccessStatusCode)
                {
                    Console.WriteLine($"[DefaultProcessor] Payment not processed successfully for {request.CorrelationId}");
                }
                else
                {
                    success = true;
                }
                currentProcessor = fallbackProcessorName;
            }
            else
            {
                success = true;
            }
        });

        if (!success)
        {
            return;
        }

        const string lockKey = "payments-lock";
        await redisDb.StringIncrementAsync(lockKey);


        var parameters = new PaymentInsertParameters(
            CorrelationId: request.CorrelationId,
            Processor: currentProcessor,
            Amount: request.Amount,
            RequestedAt: requestedAt
        );
        int processedCount = await batchInserter.AddAsync(parameters);
        if (processedCount > 0)
        {
            await redisDb.StringDecrementAsync(lockKey, processedCount);
        }

    });

    return Results.Accepted();
});

apiGroup.MapGet("/payments-summary", async ([FromQuery] DateTimeOffset? from, [FromQuery] DateTimeOffset? to, IDbConnection conn, AdaptativeLimiter limiter, IConnectionMultiplexer redisConn,
[FromKeyedServices("channel:busy:http")] AsyncBlockingGate channelBlockingGate) =>
{
    var redisDb = redisConn.GetDatabase();
    await redisConn.GetSubscriber().PublishAsync(
        RedisChannel.Literal("payments-summary-gate"),
        "1");
    try
    {
        await WaitWithTimeoutAsync(async () =>
        {
            await WaitForRedisLocksToReleaseAsync(
                redisDb,
                ["payments-lock"]);
            await channelBlockingGate.WaitIfBlockedAsync();
        }, timeout: TimeSpan.FromSeconds(1));

        const string sql = @"
            SELECT processor,
                COUNT(*) AS total_requests,
                SUM(amount) AS total_amount
            FROM payments
            WHERE (@from IS NULL OR requested_at >= @from)
            AND (@to IS NULL OR requested_at <= @to)
            GROUP BY processor;
        ";
        List<PaymentSummaryResult> result = [.. await conn.QueryAsync<PaymentSummaryResult>(sql, new { from, to })];

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
            "0");
    }
});

static async Task<bool> WaitWithTimeoutAsync(Func<Task> taskFactory, TimeSpan timeout)
{
    var task = taskFactory();
    var timeoutTask = Task.Delay(timeout);
    var completedTask = await Task.WhenAny(task, timeoutTask).ConfigureAwait(false);

    if (completedTask == timeoutTask)
        return false; // Timed out

    await task.ConfigureAwait(false); // propagate exceptions if any
    return true; // Completed successfully
}

static async Task WaitForRedisLocksToReleaseAsync(
    IDatabase redisDb,
    IEnumerable<string> lockKeys,
    int delayMs = 10,
    int maxDelayMs = 1000,
    CancellationToken cancellationToken = default)
{
    var startTime = DateTime.UtcNow;
    var lastLogTime = DateTime.MinValue;

    while (true)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var values = await Task.WhenAll(lockKeys.Select(k => redisDb.StringGetAsync(k)));
        var activeLocks = lockKeys.Zip(values, (key, value) =>
        {
            _ = int.TryParse(value, out int count);
            return (key, count);
        }).Where(t => t.count > 0).ToList();

        if (activeLocks.Count == 0)
            break;

        var now = DateTime.UtcNow;
        var elapsedMs = (int)(now - startTime).TotalMilliseconds;

        if (elapsedMs >= maxDelayMs)
        {
            Console.WriteLine($"[RedisLock] Max wait time of {maxDelayMs}ms exceeded, continuing.");
            break;
        }

        if ((now - lastLogTime).TotalSeconds >= 1)
        {
            foreach (var (key, count) in activeLocks)
            {
                Console.WriteLine($"[RedisLock] {key} = {count} (locked)");
            }
            lastLogTime = now;
        }

        int remainingMs = Math.Min(delayMs, maxDelayMs - elapsedMs);
        await Task.Delay(remainingMs, cancellationToken).ConfigureAwait(false);
    }
}



apiGroup.MapPost("/purge-payments", async (IDbConnection conn, IConnectionMultiplexer redisConn) =>
{
    var redisDb = redisConn.GetDatabase();
    await redisDb.KeyDeleteAsync("payments-lock");
    const string sql = "TRUNCATE TABLE payments";
    await conn.ExecuteAsync(sql);
});

app.Run();

static async Task LockChecks(IDatabase redisDb, AsyncBlockingGate blockingGate)
{
    await blockingGate.WaitIfBlockedAsync();
}
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

[JsonSerializable(typeof(ProcessorPaymentRequest))]
[JsonSerializable(typeof(PaymentRequest))]
[JsonSerializable(typeof(PaymentSummaryResponse))]
internal partial class JsonContext : JsonSerializerContext
{

}
