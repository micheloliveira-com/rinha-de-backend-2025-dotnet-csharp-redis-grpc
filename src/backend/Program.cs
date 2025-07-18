using Dapper;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using Polly;
using Polly.Extensions.Http;
using System.Data;
using System.Diagnostics;
using System.Text.Json.Serialization;
using System.Threading.RateLimiting;
[module: DapperAot]

static IAsyncPolicy<HttpResponseMessage> GetRetryPolicy()
{
    return HttpPolicyExtensions
        .HandleTransientHttpError()
        .WaitAndRetryAsync(
            3,
            retryAttempt => TimeSpan.FromMilliseconds(300 * Math.Pow(2, retryAttempt - 1))
        );
}

static IAsyncPolicy<HttpResponseMessage> GetFallbackRetryPolicy()
{
    return HttpPolicyExtensions
        .HandleTransientHttpError()
        .WaitAndRetryAsync(
            5,
            retryAttempt => TimeSpan.FromMilliseconds(300 * Math.Pow(2, retryAttempt - 1))
        );
}

const string defaultProcessorName = "default";
const string fallbackProcessorName = "fallback";

var builder = WebApplication.CreateSlimBuilder(args);

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.TypeInfoResolverChain.Insert(0, JsonContext.Default);
});

builder.Services.AddHttpClient(defaultProcessorName, o =>
    o.BaseAddress = new Uri(builder.Configuration.GetConnectionString(defaultProcessorName)!))
        .AddPolicyHandler(GetRetryPolicy());

builder.Services.AddHttpClient(fallbackProcessorName, o =>
    o.BaseAddress = new Uri(builder.Configuration.GetConnectionString(fallbackProcessorName)!))
        .AddPolicyHandler(GetFallbackRetryPolicy());

builder.Services.AddTransient<IDbConnection>(sp =>
    new NpgsqlConnection(builder.Configuration.GetConnectionString("postgres")));

builder.Services.AddSingleton<BackgroundWorkerQueue>();
builder.Services.AddHostedService(provider => provider.GetRequiredService<BackgroundWorkerQueue>());

builder.Services.AddSingleton<AdaptativeLimiter>();

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

        var httpDefault = factory.CreateClient(defaultProcessorName);
        var httpFallback = factory.CreateClient(fallbackProcessorName);

        var currentProcessor = defaultProcessorName;
        var success = false;
        var requestedAt = DateTimeOffset.UtcNow;
        await limiter.RunAsync(async (ct) =>
        {
            var response = await httpDefault.PostAsJsonAsync("/payments", new ProcessorPaymentRequest
            (
                request.Amount,
                requestedAt,
                request.CorrelationId
            ), JsonContext.Default.ProcessorPaymentRequest);
            if (!response.IsSuccessStatusCode)
            {
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
                    return;
                }
                currentProcessor = fallbackProcessorName;
            }
            success = true;
        });

        if (!success)
        {
            return;
        }
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
    });

    return Results.Accepted();
});

apiGroup.MapGet("/payments-summary", async ([FromQuery] DateTimeOffset? from, [FromQuery] DateTimeOffset? to, IDbConnection conn) =>
{
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
});


apiGroup.MapPost("/purge-payments", async (IDbConnection conn) =>
{
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