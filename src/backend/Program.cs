using Dapper;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System.Data;
using System.Text.Json.Serialization;
[module: DapperAot]


var builder = WebApplication.CreateSlimBuilder(args);

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.TypeInfoResolverChain.Insert(0, JsonContext.Default);
});

builder.Services.AddTransient<IDbConnection>(sp =>
    new NpgsqlConnection(builder.Configuration.GetConnectionString("postgres")));

var app = builder.Build();

var apiGroup = app.MapGroup("/");
apiGroup.MapGet("/", () => Results.Ok());


apiGroup.MapPost("payments", async ([FromBody] PaymentRequest request, IDbConnection conn) =>
{
    var sql = @"
        INSERT INTO payments (correlation_id, processor, amount, requested_at)
        VALUES (@CorrelationId, @Processor, @Amount, @RequestedAt);
    ";

    var parameters = new PaymentInsertParameters(
        CorrelationId: request.CorrelationId,
        Processor: "default",
        Amount: request.Amount,
        RequestedAt: DateTime.UtcNow
    );

    await conn.ExecuteAsync(sql, parameters);

    return Results.Accepted();
});

apiGroup.MapGet("/payments-summary", async ([FromQuery] DateTimeOffset? from, [FromQuery] DateTimeOffset? to, IDbConnection conn) =>
{
    const string defaultProcessorName = "default";
    const string fallbackProcessorName = "fallback";
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

[JsonSerializable(typeof(PaymentRequest))]
[JsonSerializable(typeof(PaymentSummaryResponse))]
internal partial class JsonContext : JsonSerializerContext
{

}