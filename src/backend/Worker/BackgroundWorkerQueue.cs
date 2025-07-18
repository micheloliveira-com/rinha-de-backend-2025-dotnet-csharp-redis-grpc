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

        await Channel.Writer.WriteAsync(workItem);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        /*var limiter = new TokenBucketRateLimiter(new TokenBucketRateLimiterOptions
        {
            TokenLimit = 100,
            TokensPerPeriod = 100,
            ReplenishmentPeriod = TimeSpan.FromSeconds(1),
            QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
            QueueLimit = int.MaxValue
        });*/

        await foreach (var workItem in Channel.Reader.ReadAllAsync(stoppingToken))
        {

            _ = Task.Run(async () =>
            {
                /*var lease = limiter.AttemptAcquire(1);

                if (!lease.IsAcquired)
                {
                    Console.WriteLine("Rate limiter is full: waiting for tokens...");
                    lease.Dispose();
                    lease = await limiter.AcquireAsync(1, stoppingToken);
                }*/

                //using (lease)
                {
                    try
                    {
                        await workItem(stoppingToken);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Background task error: {ex}");
                    }
                }
            }, stoppingToken);
        }
    }
}
