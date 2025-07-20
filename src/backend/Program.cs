using Dapper;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using Polly;
using Polly.Extensions.Http;
using StackExchange.Redis;
using System.Data;
using System.Diagnostics;
using System.Text.Json.Serialization;
using System.Threading.RateLimiting;
[module: DapperAot]

static IAsyncPolicy<HttpResponseMessage> GetRetryPolicy(IDatabase redisDb)
{
    return HttpPolicyExtensions
        .HandleTransientHttpError()
        .WaitAndRetryAsync(
            5,
            retryAttempt => TimeSpan.FromMilliseconds(80 * Math.Pow(2, retryAttempt - 1)),
            onRetryAsync: async (outcome, timespan, retryCount, context) =>
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
                        await Task.Delay(50);
                    }

                } while (isLocked);
                if (wasLocked)
                {
                    await redisDb.StringIncrementAsync(requestsLockKey);
                }
            });
}

static IAsyncPolicy<HttpResponseMessage> GetFallbackRetryPolicy(IDatabase redisDb)
{
    var retryPolicy = HttpPolicyExtensions
        .HandleTransientHttpError()
        .WaitAndRetryForeverAsync(
            sleepDurationProvider: _ => TimeSpan.FromMilliseconds(10),
            onRetryAsync: async (outcome, timespan, context) =>
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
                        await Task.Delay(50);
                    }

                } while (isLocked);
                if (wasLocked)
                {
                    await redisDb.StringIncrementAsync(requestsLockKey);
                }
            });

    var timeoutPolicy = Policy.TimeoutAsync<HttpResponseMessage>(TimeSpan.FromMinutes(1));

    return Policy.WrapAsync(retryPolicy, timeoutPolicy);
}

const string defaultProcessorName = "default";
const string fallbackProcessorName = "fallback";

var builder = WebApplication.CreateSlimBuilder(args);

builder.Services.AddSingleton<IConnectionMultiplexer>(sp =>
{
    var configuration = builder.Configuration.GetConnectionString("redis");
    return ConnectionMultiplexer.Connect(configuration!);
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
    return new AdaptativeLimiter(minLimitCount: 1000, maxLimitCount: 1500);
});
builder.Services.AddKeyedSingleton("postgres", (_, _) =>
{
    return new AdaptativeLimiter(minLimitCount: 200, maxLimitCount: 500);
});
builder.Services.AddKeyedSingleton("worker", (_, _) =>
{
    return new AdaptativeLimiter(minLimitCount: 500, maxLimitCount: 1000);
});

builder.Services.AddSingleton<BackgroundWorkerQueue>();
builder.Services.AddHostedService(provider => provider.GetRequiredService<BackgroundWorkerQueue>());

if (builder.Environment.IsProduction())
{
    builder.Logging.ClearProviders();
    builder.Logging.SetMinimumLevel(LogLevel.Error);
}

var app = builder.Build();

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
                    await Task.Delay(50);
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
                        await Task.Delay(50);
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
        await postgresLimiter.RunAsync(async (ct) =>
        {
            const string sql = @"
            INSERT INTO payments (correlation_id, processor, amount, requested_at)
            VALUES (@CorrelationId, @Processor, @Amount, @RequestedAt);
        ";

            var parameters = new PaymentInsertParameters(
                CorrelationId: request.CorrelationId,
                Processor: currentProcessor,
                Amount: request.Amount,
                RequestedAt: requestedAt
            );

            int affectedRows = await conn.ExecuteAsync(sql, parameters);

            await redisDb.StringDecrementAsync(lockKey);
        });
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
        CancellationToken cancellationToken = default)
{
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

        foreach (var (key, count) in activeLocks)
        {
            Console.WriteLine($"[RedisLock] {key} = {count} (locked)");
        }

        await Task.Delay(delayMs, cancellationToken);
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