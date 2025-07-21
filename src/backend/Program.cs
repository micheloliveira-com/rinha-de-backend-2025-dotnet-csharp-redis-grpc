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
using System.Net.Sockets;
using System.Text.Json.Serialization;
using System.Threading.RateLimiting;
[module: DapperAot]

static IAsyncPolicy<HttpResponseMessage> GetRetryPolicy(IDatabase redisDb)
{
    return HttpPolicyExtensions
        .HandleTransientHttpError()
        .WaitAndRetryAsync(
            7,
            sleepDurationProvider: _ => TimeSpan.Zero,
            onRetryAsync: async (outcome, timespan, retryCount, context) =>
            {
                var sw = Stopwatch.StartNew();
                await LockChecks(redisDb);
                var remaining = TimeSpan.FromMilliseconds(10 * Math.Pow(2, retryCount - 1)) - sw.Elapsed;
                if (remaining > TimeSpan.Zero)
                    await Task.Delay(remaining).ConfigureAwait(false);
                await LockChecks(redisDb);
            });
}

static IAsyncPolicy<HttpResponseMessage> GetFallbackRetryPolicy(IDatabase redisDb)
{
    var retryPolicy = HttpPolicyExtensions
        .HandleTransientHttpError()
        .WaitAndRetryForeverAsync(
            sleepDurationProvider: _ => TimeSpan.Zero,
            onRetryAsync: async (outcome, timespan, context) =>
            {
                var sw = Stopwatch.StartNew();
                await LockChecks(redisDb);
                var remaining = TimeSpan.FromMilliseconds(10) - sw.Elapsed;
                if (remaining > TimeSpan.Zero)
                    await Task.Delay(remaining).ConfigureAwait(false);
                await LockChecks(redisDb);
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
    .AddPolicyHandler((sp, req) =>
    {
        var redis = sp.GetRequiredService<IConnectionMultiplexer>();
        var db = redis.GetDatabase();
        return GetRetryPolicy(db);
    });

builder.Services.AddHttpClient(fallbackProcessorName, o =>
    o.BaseAddress = new Uri(builder.Configuration.GetConnectionString(fallbackProcessorName)!))
    .AddPolicyHandler((sp, req) =>
    {
        var redis = sp.GetRequiredService<IConnectionMultiplexer>();
        var db = redis.GetDatabase();
        return GetFallbackRetryPolicy(db);
    });

builder.Services.AddTransient<IDbConnection>(sp =>
    new NpgsqlConnection(builder.Configuration.GetConnectionString("postgres")));

builder.Services.AddSingleton(_ =>
{
    return new AdaptativeLimiter(minLimitCount: 1, maxLimitCount: 5);
});
builder.Services.AddKeyedSingleton("postgres", (_, _) =>
{
    return new AdaptativeLimiter(minLimitCount: 100, maxLimitCount: 500);
});
builder.Services.AddKeyedSingleton("worker", (_, _) =>
{
    return new AdaptativeLimiter(minLimitCount: 500, maxLimitCount: 1000);
});

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
var batchInserter = app.Services.GetRequiredService<PaymentBatchInserter>();

var subscriber = redis.GetSubscriber();

await subscriber.SubscribeAsync(
    RedisChannel.Literal("payments-events"), 
    async (channel, message) =>
    {
        Console.WriteLine($"[Redis] Received message on channel '{channel}': {message}");
        
        var redisDb = redis.GetDatabase();
        bool isLocked;
        do
        {
            isLocked = await redisDb.KeyExistsAsync("payments-summary-lock");

            if (isLocked)
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
                await Task.Delay(10).ConfigureAwait(false);
            }

        } while (isLocked);
    });

var apiGroup = app.MapGroup("/");
apiGroup.MapGet("/", () => Results.Ok());

apiGroup.MapPost("payments", async ([FromBody] PaymentRequest request, BackgroundWorkerQueue queue, IServiceScopeFactory scopeFactory) =>
{

    await queue.EnqueueAsync(async ct =>
    {
        using var scope = scopeFactory.CreateScope();

        var factory = scope.ServiceProvider.GetRequiredService<IHttpClientFactory>();
        var conn = scope.ServiceProvider.GetRequiredService<IDbConnection>();
        var limiter = scope.ServiceProvider.GetRequiredService<AdaptativeLimiter>();
        var postgresLimiter = scope.ServiceProvider.GetRequiredKeyedService<AdaptativeLimiter>("postgres");
        var redis = scope.ServiceProvider.GetRequiredService<IConnectionMultiplexer>();
        var batchInserter = scope.ServiceProvider.GetRequiredService<PaymentBatchInserter>();


        var httpDefault = factory.CreateClient(defaultProcessorName);
        var httpFallback = factory.CreateClient(fallbackProcessorName);

        var currentProcessor = defaultProcessorName;
        var success = false;
        var requestedAt = DateTimeOffset.UtcNow;
        var redisDb = redis.GetDatabase();
        await limiter.RunAsync(async (ct) =>
        {
            bool isLocked;
            do
            {
                isLocked = await redisDb.KeyExistsAsync("payments-summary-lock");

                if (isLocked)
                {
                    await Task.Delay(50).ConfigureAwait(false);
                }

            } while (isLocked);

            const string requestsLockKey = "requests-lock";
            await redisDb.StringIncrementAsync(requestsLockKey);

            var response = await httpDefault.PostAsJsonAsync("/payments", new ProcessorPaymentRequest
            (
                request.Amount,
                requestedAt,
                request.CorrelationId
            ), JsonContext.Default.ProcessorPaymentRequest);
            await redisDb.StringDecrementAsync(requestsLockKey);
            if (!response.IsSuccessStatusCode)
            {
                isLocked = false;
                do
                {
                    isLocked = await redisDb.KeyExistsAsync("payments-summary-lock");

                    if (isLocked)
                    {
                        await Task.Delay(50).ConfigureAwait(false);
                    }

                } while (isLocked);
                await redisDb.StringIncrementAsync(requestsLockKey);
                requestedAt = DateTimeOffset.UtcNow;
                response = await httpFallback.PostAsJsonAsync("/payments", new ProcessorPaymentRequest
                (
                    request.Amount,
                    requestedAt,
                    request.CorrelationId
                ), JsonContext.Default.ProcessorPaymentRequest);
                await redisDb.StringDecrementAsync(requestsLockKey);
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
        /*if (processedCount == 0)
        {
            bool wasLocked = false;
            bool isLocked;
            do
            {
                isLocked = await redisDb.KeyExistsAsync("payments-summary-lock");

                if (isLocked)
                {
                    if (!wasLocked)
                    {
                        processedCount = await batchInserter.FlushBatchAsync();
                    }
                    wasLocked = true;
                    await Task.Delay(10).ConfigureAwait(false);
                }

            } while (isLocked);
        }*/
        if (processedCount > 0)
        {
            await redisDb.StringDecrementAsync(lockKey, processedCount);
        }

    });

    return Results.Accepted();
});

apiGroup.MapGet("/payments-summary", async ([FromQuery] DateTimeOffset? from, [FromQuery] DateTimeOffset? to, IDbConnection conn, AdaptativeLimiter limiter, IConnectionMultiplexer redisConn) =>
{
    var redisDb = redisConn.GetDatabase();
    const string lockKey = "payments-summary-lock";
    await redisDb.StringSetAsync(
        lockKey,
        string.Empty,
        when: When.NotExists);
    await redisConn.GetSubscriber().PublishAsync(
        RedisChannel.Literal("payments-events"), 
        "your message here");
    try
    {
        await WaitForRedisLocksToReleaseAsync(
            redisDb,
            ["payments-lock", "requests-lock"]);

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
        await redisDb.KeyDeleteAsync(lockKey);
    }
});

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
    await redisDb.KeyDeleteAsync("payments-summary-lock");
    await redisDb.KeyDeleteAsync("payments-lock");
    await redisDb.KeyDeleteAsync("requests-lock");
    const string sql = "TRUNCATE TABLE payments";
    await conn.ExecuteAsync(sql);
});

app.Run();

static async Task LockChecks(IDatabase redisDb)
{
    const string requestsLockKey = "requests-lock";
    bool wasLocked = false;
    bool isLocked;
    do
    {
        isLocked = await redisDb.KeyExistsAsync("payments-summary-lock");

        if (isLocked)
        {
            if (!wasLocked)
            {
                await redisDb.StringDecrementAsync(requestsLockKey);
            }
            wasLocked = true;
            await Task.Delay(10).ConfigureAwait(false);
        }

    } while (isLocked);
    if (wasLocked)
    {
        await redisDb.StringIncrementAsync(requestsLockKey);
    }
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
