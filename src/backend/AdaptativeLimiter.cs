using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

public class AdaptativeLimiter : IDisposable
{
    private readonly SemaphoreSlim SyncLock = new(1, 1);
    private readonly SemaphoreSlim LatencyLock = new(1, 1);
    private int UpThrottleInMs { get; set; } = 50;
    private int DownThrottleMs { get; set; } = 40;
    private int UpDownStepCount { get; } = 5;
    private int LatencyAvgQueueMaxCount { get; } = 10;
    private Queue<long> LatestLatencies { get; } = new();
    private SemaphoreSlim Semaphore { get; set; }
    private int CurrentLimitCount { get; set; }
    private int MinLimitCount { get; }
    private int MaxLimitCount { get; }

    public AdaptativeLimiter(int minLimitCount, int maxLimitCount)
    {
        MinLimitCount = minLimitCount;
        MaxLimitCount = maxLimitCount;
        CurrentLimitCount = MinLimitCount;
        Semaphore = new SemaphoreSlim(CurrentLimitCount);
    }

    public async Task RunAsync(Func<CancellationToken, Task> operation, CancellationToken cancellationToken = default)
    {
        await Semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        var stopWatch = Stopwatch.StartNew();
        var cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var progressTask = Task.Run(async () =>
        {
            try
            {
                while (!cancellationTokenSource.Token.IsCancellationRequested)
                {
                    await Task.Delay(51, cancellationTokenSource.Token).ConfigureAwait(false);
                    await RecordLatencyAsync(stopWatch.ElapsedMilliseconds).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { }
        }, cancellationTokenSource.Token);

        try
        {
            await operation(cancellationTokenSource.Token).ConfigureAwait(false);
        }
        finally
        {
            stopWatch.Stop();
            cancellationTokenSource.Cancel();
            try { await progressTask.ConfigureAwait(false); } catch { }

            await RecordLatencyAsync(stopWatch.ElapsedMilliseconds).ConfigureAwait(false);
            Semaphore.Release();
        }
    }

    private async Task RecordLatencyAsync(long ms)
    {
        await LatencyLock.WaitAsync().ConfigureAwait(false);
        try
        {
            LatestLatencies.Enqueue(ms);
            if (LatestLatencies.Count > LatencyAvgQueueMaxCount)
                LatestLatencies.Dequeue();
        }
        finally
        {
            LatencyLock.Release();
        }

        await AdjustLimitAsync().ConfigureAwait(false);
    }

    private async Task AdjustLimitAsync()
    {
        double avgLatency;

        await LatencyLock.WaitAsync().ConfigureAwait(false);
        try
        {
            if (LatestLatencies.Count == 0) return;
            avgLatency = LatestLatencies.Average();
        }
        finally
        {
            LatencyLock.Release();
        }

        await SyncLock.WaitAsync().ConfigureAwait(false);
        try
        {
            int newLimit = CurrentLimitCount;

            if (avgLatency > UpThrottleInMs && newLimit > MinLimitCount)
                newLimit = Math.Max(MinLimitCount, newLimit - UpDownStepCount);
            else if (avgLatency < DownThrottleMs && newLimit < MaxLimitCount)
                newLimit = Math.Min(MaxLimitCount, newLimit + UpDownStepCount);

            if (newLimit != CurrentLimitCount)
            {
                //Console.WriteLine($"[Limiter] New concurrency limit: {newLimit} | Avg latency: {avgLatency:0.0}ms");

                int used = CurrentLimitCount - Semaphore.CurrentCount;
                int available = Math.Max(0, newLimit - used);

                var newSemaphore = new SemaphoreSlim(available);
                
                Semaphore = newSemaphore;
                CurrentLimitCount = newLimit;
            }
        }
        finally
        {
            SyncLock.Release();
        }
    }

    public void Dispose()
    {
        Semaphore.Dispose();
        SyncLock.Dispose();
        LatencyLock.Dispose();
    }
}
