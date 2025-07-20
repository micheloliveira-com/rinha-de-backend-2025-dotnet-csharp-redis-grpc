using System.Threading.Channels;
using System.Threading.RateLimiting;

public class BackgroundWorkerQueue : BackgroundService
{
    private Channel<Func<CancellationToken, Task>> Channel { get; }
    public AdaptativeLimiter AdaptativeLimiter { get; }
    public BackgroundWorkerQueue([FromKeyedServices("worker")] AdaptativeLimiter limiter)
    {
        AdaptativeLimiter = limiter;
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

        await Channel.Writer.WriteAsync(workItem);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var workItem in Channel.Reader.ReadAllAsync(stoppingToken))
        {

            _ = Task.Run(async () =>
            {
                //await AdaptativeLimiter.RunAsync(async (ct) =>
                //{
                    try
                    {
                        await workItem(stoppingToken);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Background task error: {ex}");
                    }
                //});
            }, stoppingToken);
        }
    }
}
