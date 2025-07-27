using System.Data;
using System.Text.Json;
using Dapper;
using MichelOliveira.Com.ReactiveLock.Core;
using MichelOliveira.Com.ReactiveLock.DependencyInjection;
using StackExchange.Redis;

public class PaymentService
{
    private IDbConnection Conn { get; }
    private IConnectionMultiplexer Redis { get; }
    private PaymentBatchInserterService BatchInserter { get; }
    private IReactiveLockTrackerState ReactiveLockTrackerState { get; }
    private HttpClient HttpDefault { get; }

    public PaymentService(
        IHttpClientFactory factory,
        IConnectionMultiplexer redis,
        PaymentBatchInserterService batchInserter,
        IDbConnection conn,
        IReactiveLockTrackerFactory reactiveLockTrackerFactory
    )
    {
        Conn = conn;
        Redis = redis;
        BatchInserter = batchInserter;
        ReactiveLockTrackerState = reactiveLockTrackerFactory.GetTrackerState(Constant.REACTIVELOCK_API_PAYMENTS_SUMMARY_NAME);

        HttpDefault = factory.CreateClient(Constant.DEFAULT_PROCESSOR_NAME);
    }


    public async Task<IResult> EnqueuePaymentAsync(HttpContext context)
    {
        using var ms = new MemoryStream();
        await context.Request.Body.CopyToAsync(ms);
        var rawBody = ms.ToArray();

        _ = Task.Run(async () =>
        {
            var db = Redis.GetDatabase();
            await db.ListRightPushAsync(Constant.REDIS_QUEUE_KEY, rawBody, flags: StackExchange.Redis.CommandFlags.FireAndForget).ConfigureAwait(false);
        });

        return Results.Accepted();
    }


    public async Task<IResult> PurgePaymentsAsync()
    {
        const string sql = "TRUNCATE TABLE payments";
        await Conn.ExecuteAsync(sql).ConfigureAwait(false);
        return Results.Ok("Payments table truncated.");
    }

    public async Task ProcessPaymentAsync(string message)
    {
        var request = JsonSerializer.Deserialize(message, JsonContext.Default.ProcessorPaymentRequest);
        if (request == null)
        {
            return;
        }
        var requestedAt = DateTimeOffset.UtcNow;
        var redisDb = Redis.GetDatabase();
        await ReactiveLockTrackerState.WaitIfBlockedAsync().ConfigureAwait(false);
        var response = await HttpDefault.PostAsJsonAsync("/payments", new ProcessorPaymentRequest
        (
            request.Amount,
            requestedAt,
            request.CorrelationId
        ), JsonContext.Default.ProcessorPaymentRequest).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            await redisDb.ListRightPushAsync(Constant.REDIS_QUEUE_KEY, message, flags: StackExchange.Redis.CommandFlags.FireAndForget).ConfigureAwait(false);
            return;
        }
        var parameters = new PaymentInsertParameters(
            CorrelationId: request.CorrelationId,
            Processor: Constant.DEFAULT_PROCESSOR_NAME,
            Amount: request.Amount,
            RequestedAt: requestedAt
        );
        await BatchInserter.AddAsync(parameters).ConfigureAwait(false);
    }   
}