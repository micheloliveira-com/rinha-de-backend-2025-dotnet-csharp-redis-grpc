using Dapper;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System.Data;
using System.Text.Json.Serialization;
[module: DapperAot]


const string defaultProcessorName = "default";
const string fallbackProcessorName = "fallback";

var builder = WebApplication.CreateSlimBuilder(args);

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.TypeInfoResolverChain.Insert(0, JsonContext.Default);
});

builder.Services.AddHttpClient(defaultProcessorName, o =>
    o.BaseAddress = new Uri(builder.Configuration.GetConnectionString(defaultProcessorName)!));

builder.Services.AddHttpClient(fallbackProcessorName, o =>
    o.BaseAddress = new Uri(builder.Configuration.GetConnectionString(fallbackProcessorName)!));

builder.Services.AddTransient<IDbConnection>(sp =>
    new NpgsqlConnection(builder.Configuration.GetConnectionString("postgres")));

if (builder.Environment.IsProduction())
{
    builder.Logging.ClearProviders();
    builder.Logging.SetMinimumLevel(LogLevel.Error);
}

var app = builder.Build();

var apiGroup = app.MapGroup("/");
apiGroup.MapGet("/", () => Results.Ok());

apiGroup.MapPost("payments", async ([FromBody] PaymentRequest request, IDbConnection conn, IHttpClientFactory factory) =>
{
    var httpDefault = factory.CreateClient(defaultProcessorName);

    var requestedAt = DateTimeOffset.UtcNow;
    try
    {
        var response = await httpDefault.PostAsJsonAsync("/payments", new ProcessorPaymentRequest
        (
            request.Amount,
            requestedAt,
            request.CorrelationId
        ), JsonContext.Default.ProcessorPaymentRequest);

        response.EnsureSuccessStatusCode();
    }
    catch
    {
        return Results.Conflict();
    }

    var sql = @"
        INSERT INTO payments (correlation_id, processor, amount, requested_at)
        VALUES (@CorrelationId, @Processor, @Amount, @RequestedAt)
        ON CONFLICT (correlation_id) DO NOTHING;
    ";

    var parameters = new PaymentInsertParameters(
        CorrelationId: request.CorrelationId,
        Processor: defaultProcessorName,
        Amount: request.Amount,
        RequestedAt: DateTime.UtcNow
    );

    int affectedRows = await conn.ExecuteAsync(sql, parameters);

    if (affectedRows == 0)
    {
        return Results.Conflict();
    }

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

    var rows = (await conn.QueryAsync<PaymentSummaryResult>(sql, new { from, to })).ToList();

    var defaultResult = rows.FirstOrDefault(r => r.Processor == defaultProcessorName) ?? new PaymentSummaryResult(defaultProcessorName, 0, 0);
    var fallbackResult = rows.FirstOrDefault(r => r.Processor == fallbackProcessorName) ?? new PaymentSummaryResult(fallbackProcessorName, 0, 0);

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
    DateTime RequestedAt
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