using StackExchange.Redis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;

public class RedisQueueWorker : BackgroundService
{
    private readonly IDatabase _db;
    private readonly ISubscriber _sub;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly string _queueKey = "task-queue";
    private readonly string _notifyChannel = "task-notify";
    private readonly int _parallelism = 10;
    private readonly SemaphoreSlim _notifySignal;
    private readonly SemaphoreSlim _notifySignalLock = new SemaphoreSlim(1, 1);
    private int _pendingWorkCount = 0;

    public RedisQueueWorker(IConnectionMultiplexer redis, IServiceScopeFactory scopeFactory)
    {
        _db = redis.GetDatabase();
        _sub = redis.GetSubscriber();
        _scopeFactory = scopeFactory;
        _notifySignal = new SemaphoreSlim(_parallelism, _parallelism);
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _sub.Subscribe(RedisChannel.Literal(_notifyChannel), (_, _) =>
        {
            _ = Task.Run(async () =>
            {
                var semaphoreNotReleased = await RunLockedAsync(() =>
                {
                    return !ReleaseAtLeastOneSemaphoreIfPossible();
                }).ConfigureAwait(false);

                if (semaphoreNotReleased)
                {
                    Interlocked.Increment(ref _pendingWorkCount);
                }
            });
        });

        var workers = new Task[_parallelism];
        for (int i = 0; i < _parallelism; i++)
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
                RedisValue msg;

                while ((msg = await _db.ListLeftPopAsync(_queueKey).ConfigureAwait(false)).HasValue)
                {
                    using var scope = _scopeFactory.CreateScope();
                    var paymentService = scope.ServiceProvider.GetRequiredService<PaymentService>();
                    await paymentService.ProcessPaymentAsync(msg!).ConfigureAwait(false);
                }

                await RunLockedAsync(ReleasePendingSemaphoreIfPossible).ConfigureAwait(false);
                int currentPending = Volatile.Read(ref _pendingWorkCount);
                if (currentPending == 0)
                {
                    await _notifySignal.WaitAsync(TimeSpan.FromSeconds(1), CancellationToken.None).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Worker Error] {ex}");
            }
        }
    }

    private async Task<bool> RunLockedAsync(Func<bool> action)
    {
        await _notifySignalLock.WaitAsync().ConfigureAwait(false);
        try
        {
            return action();
        }
        finally
        {
            _notifySignalLock.Release();
        }
    }

    private int GetAvailableSemaphoreCapacity()
    {
        return _parallelism - _notifySignal.CurrentCount;
    }

    private bool ReleaseAtLeastOneSemaphoreIfPossible()
    {
        int capacityLeft = GetAvailableSemaphoreCapacity();
        if (capacityLeft > 0)
        {
            _notifySignal.Release();
            return true;
        }
        return false;
    }

    private bool ReleasePendingSemaphoreIfPossible()
    {
        int capacityLeft = GetAvailableSemaphoreCapacity();
        if (capacityLeft <= 0)
            return false;
        bool releasedAny = false;

        while (capacityLeft > 0)
        {
            int currentPending = Volatile.Read(ref _pendingWorkCount);
            if (currentPending == 0)
                break;

            int toRelease = Math.Min(capacityLeft, currentPending);
            int newPending = currentPending - toRelease;

            int original = Interlocked.CompareExchange(ref _pendingWorkCount, newPending, currentPending);
            if (original == currentPending)
            {
                _notifySignal.Release(toRelease);
                releasedAny = true;
                break;
            }
        }

        return releasedAny;
    }

}
