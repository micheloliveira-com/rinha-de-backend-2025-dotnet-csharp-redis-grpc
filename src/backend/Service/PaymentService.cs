using System.Text.Json;
using StackExchange.Redis;

public class PaymentService
{
    const string defaultProcessorName = "default";
    private IConnectionMultiplexer Redis { get; }
    private PaymentBatchInserter BatchInserter { get; }
    private IReactiveLockTrackerState ReactiveLockTrackerState { get; }
    private HttpClient HttpDefault { get; }

    public PaymentService(
        IHttpClientFactory factory,
        IConnectionMultiplexer redis,
        PaymentBatchInserter batchInserter,
        IReactiveLockTrackerFactory reactiveLockTrackerFactory
    )
    {
        Redis = redis;
        BatchInserter = batchInserter;
        ReactiveLockTrackerState = reactiveLockTrackerFactory.GetTrackerState("api:payments-summary");

        HttpDefault = factory.CreateClient(defaultProcessorName);
    }
    public async Task ProcessPaymentAsync(string message)
    {
        var request = JsonSerializer.Deserialize(message, JsonContext.Default.ProcessorPaymentRequest);
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
            await redisDb.ListRightPushAsync("task-queue", message, flags: CommandFlags.FireAndForget).ConfigureAwait(false);
            return;
        }
        var parameters = new PaymentInsertParameters(
            CorrelationId: request.CorrelationId,
            Processor: defaultProcessorName,
            Amount: request.Amount,
            RequestedAt: requestedAt
        );
        await BatchInserter.AddAsync(parameters).ConfigureAwait(false);
    }   
}