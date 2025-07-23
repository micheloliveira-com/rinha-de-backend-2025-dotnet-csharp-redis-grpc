using System.Text.Json;
using StackExchange.Redis;

public class PaymentService
{
    const string defaultProcessorName = "default";
    const string fallbackProcessorName = "fallback";
    private IHttpClientFactory Factory { get; }
    private AdaptativeLimiter Limiter { get; }
    private IConnectionMultiplexer Redis { get; }
    private PaymentBatchInserter BatchInserter { get; }
    private AsyncBlockingGate BlockingGate { get; }
    private HttpClient HttpDefault { get; }
    private HttpClient HttpFallback { get; }

    public PaymentService(
        IHttpClientFactory factory,
        AdaptativeLimiter limiter,
        IConnectionMultiplexer redis,
        PaymentBatchInserter batchInserter,
        AsyncBlockingGate blockingGate,
        IConfiguration configuration
    )
    {
        Factory = factory;
        Limiter = limiter;
        Redis = redis;
        BatchInserter = batchInserter;
        BlockingGate = blockingGate;

        HttpDefault = factory.CreateClient(defaultProcessorName);
        HttpFallback = factory.CreateClient(fallbackProcessorName);
    }
    public async Task ProcessPaymentAsync(string message)
    {
        var request = JsonSerializer.Deserialize(message, JsonContext.Default.ProcessorPaymentRequest);

        var currentProcessor = defaultProcessorName;
        var success = false;
        var requestedAt = DateTimeOffset.UtcNow;
        var redisDb = Redis.GetDatabase();
        var sub = Redis.GetSubscriber();
        await Limiter.RunAsync(async (ct) =>
        {
            await BlockingGate.WaitIfBlockedAsync().ConfigureAwait(false);
            var response = await HttpDefault.PostAsJsonAsync("/payments", new ProcessorPaymentRequest
            (
                request.Amount,
                requestedAt,
                request.CorrelationId
            ), JsonContext.Default.ProcessorPaymentRequest).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode)
            {
                await redisDb.ListRightPushAsync("task-queue", message, flags: StackExchange.Redis.CommandFlags.FireAndForget).ConfigureAwait(false);
                await sub.PublishAsync(RedisChannel.Literal("task-notify"), "", StackExchange.Redis.CommandFlags.FireAndForget).ConfigureAwait(false);

/*
                                                await BlockingGate.WaitIfBlockedAsync().ConfigureAwait(false);
                                                requestedAt = DateTimeOffset.UtcNow;
                                                response = await HttpFallback.PostAsJsonAsync("/payments", new ProcessorPaymentRequest
                                                (
                                                    request.Amount,
                                                    requestedAt,
                                                    request.CorrelationId
                                                ), JsonContext.Default.ProcessorPaymentRequest).ConfigureAwait(false);
                                                if (!response.IsSuccessStatusCode)
                                                {
                                                    Console.WriteLine($"[DefaultProcessor] Payment not processed successfully for {request.CorrelationId}");
                                                }
                                                else
                                                {
                                                    success = true;
                                                }
                                                currentProcessor = fallbackProcessorName;*/
            }
            else
            {
                success = true;
            }
        }).ConfigureAwait(false);

        if (!success)
        {
            return;
        }
        var parameters = new PaymentInsertParameters(
            CorrelationId: request.CorrelationId,
            Processor: currentProcessor,
            Amount: request.Amount,
            RequestedAt: requestedAt
        );
        await BatchInserter.AddAsync(parameters).ConfigureAwait(false);
    }   
}