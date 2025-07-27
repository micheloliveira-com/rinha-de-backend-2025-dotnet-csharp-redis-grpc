using System.Data;
using Dapper;
using StackExchange.Redis;
using MichelOliveira.Com.ReactiveLock.Core;
using MichelOliveira.Com.ReactiveLock.DependencyInjection;

public class PaymentSummaryService
{
    private IDbConnection Conn { get; }
    private IReactiveLockTrackerFactory LockFactory { get; }
    private PaymentBatchInserterService BatchInserter { get; }

    private const string DefaultProcessorName = "default";
    private const string FallbackProcessorName = "fallback";

    public PaymentSummaryService(
        IDbConnection conn,
        IReactiveLockTrackerFactory lockFactory,
        PaymentBatchInserterService batchInserter)
    {
        Conn = conn;
        LockFactory = lockFactory;
        BatchInserter = batchInserter;
    }

    public async Task<IResult> GetPaymentsSummaryAsync(DateTimeOffset? from, DateTimeOffset? to)
    {
        var paymentsLock = LockFactory.GetTrackerController(Constant.REACTIVELOCK_API_PAYMENTS_SUMMARY_NAME);
        await paymentsLock.IncrementAsync().ConfigureAwait(false);

        var postgresChannelBlockingGate = LockFactory.GetTrackerState(Constant.REACTIVELOCK_POSTGRES_NAME);
        var channelBlockingGate = LockFactory.GetTrackerState(Constant.REACTIVELOCK_HTTP_NAME);

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
            List<PaymentSummaryResult> result = [.. await Conn.QueryAsync<PaymentSummaryResult>(sql, new { from, to }).ConfigureAwait(false)];

            var defaultResult = result?.FirstOrDefault(r => r.Processor == DefaultProcessorName) ?? new PaymentSummaryResult(DefaultProcessorName, 0, 0);
            var fallbackResult = result?.FirstOrDefault(r => r.Processor == FallbackProcessorName) ?? new PaymentSummaryResult(FallbackProcessorName, 0, 0);

            var response = new PaymentSummaryResponse(
                new PaymentSummary(defaultResult.TotalRequests, defaultResult.TotalAmount),
                new PaymentSummary(fallbackResult.TotalRequests, fallbackResult.TotalAmount)
            );

            return Results.Ok(response);
        }
        finally
        {
            await paymentsLock.DecrementAsync().ConfigureAwait(false);
        }
    }


    public async Task FlushWhileGateBlockedAsync()
    {
        Console.WriteLine("[Redis] Gate blocked.");
        var state = LockFactory.GetTrackerState(Constant.REACTIVELOCK_API_PAYMENTS_SUMMARY_NAME);

        await state.WaitIfBlockedAsync(
            whileBlockedLoopDelay: TimeSpan.FromMilliseconds(10),
            whileBlockedAsync: async () =>
            {
                try
                {
                    var processedCount = await BatchInserter.FlushBatchAsync().ConfigureAwait(false);
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

    private static async Task<bool> WaitWithTimeoutAsync(Func<Task> taskFactory, TimeSpan timeout)
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
}
