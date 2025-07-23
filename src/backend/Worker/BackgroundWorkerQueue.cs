using System.Threading.Channels;
using System.Threading.RateLimiting;

public class BackgroundWorkerQueue : BackgroundService
{
    private Channel<Func<CancellationToken, Task>> Channel { get; }
    public BackgroundWorkerQueue()
    {
        Channel = System.Threading.Channels.Channel.CreateUnbounded<Func<CancellationToken, Task>>(new UnboundedChannelOptions
        {
            SingleReader = false,
            SingleWriter = false
        });
    }

    public async ValueTask EnqueueAsync(Func<CancellationToken, Task> workItem)
    {
        if (workItem is null)
        {
            throw new ArgumentNullException(nameof(workItem));
        }

        await Channel.Writer.WriteAsync(workItem).ConfigureAwait(false);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var concurrencyLimiter = new SemaphoreSlim(25);

        await foreach (var workItem in Channel.Reader.ReadAllAsync(stoppingToken).ConfigureAwait(false))
        {
            await concurrencyLimiter.WaitAsync(stoppingToken).ConfigureAwait(false);
            _ = Task.Run(async () =>
            {
                try
                {
                    await workItem(stoppingToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Background task error: {ex}");
                }
                finally
                {
                    concurrencyLimiter.Release();
                }
            }, stoppingToken);
        }
    }
}
