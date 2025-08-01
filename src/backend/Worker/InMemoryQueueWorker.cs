using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

public class InMemoryQueueWorker : BackgroundService
{
    private ConcurrentQueue<string> Queue { get; } = new();
    private SemaphoreSlim Signal { get; } = new(0);
    private IServiceScopeFactory ScopeFactory { get; }
    private DefaultOptions Options { get; }

    public InMemoryQueueWorker(
        IServiceScopeFactory scopeFactory,
        IOptions<DefaultOptions> options)
    {
        ScopeFactory = scopeFactory;
        Options = options.Value;
    }

    public void Enqueue(string msg)
    {
        Queue.Enqueue(msg);
        Signal.Release();
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var workers = new Task[Options.WORKER_SIZE];

        for (int i = 0; i < Options.WORKER_SIZE; i++)
        {
            workers[i] = Task.Run(() => WorkerLoopAsync(stoppingToken), stoppingToken);
        }

        return Task.WhenAll(workers);
    }

    private async Task WorkerLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Signal.WaitAsync(cancellationToken);

                if (Queue.TryDequeue(out var msg))
                {
                    using var scope = ScopeFactory.CreateScope();
                    var paymentService = scope.ServiceProvider.GetRequiredService<PaymentService>();
                    await paymentService.ProcessPaymentAsync(msg!).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Worker Error] {ex}");
            }
        }
    }
}
