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
    private int UpDownStepCount { get; } = 1;
    private int LatencyAvgQueueMaxCount { get; } = 10;
    private Queue<long> LatestLatencies { get; } = new();
    private SemaphoreSlim _semaphore;
    private int CurrentLimitCount { get; set; }
    private int MinLimitCount { get; }
    private int MaxLimitCount { get; }

    public AdaptativeLimiter(int minLimitCount, int maxLimitCount)
    {
        MinLimitCount = minLimitCount;
        MaxLimitCount = maxLimitCount;
        CurrentLimitCount = MinLimitCount;
        _semaphore = new SemaphoreSlim(CurrentLimitCount);
    }
    public async Task<bool> TryWaitAsync(CancellationToken cancellationToken, int maxRetries = 10)
    {
        if (maxRetries <= 0)
            return false;

        //using var logCts = new CancellationTokenSource();
        //using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, logCts.Token);

        /*var logTask = Task.Run(async () =>
        {
            try
            {
                while (!linkedCts.Token.IsCancellationRequested)
                {
                    await Task.Delay(1000, linkedCts.Token).ConfigureAwait(false);
                    int used = CurrentLimitCount - _semaphore.CurrentCount;
                    Console.WriteLine($"[Limiter] Waiting to acquire semaphore... Current: {_semaphore.CurrentCount} / Used: {used} / Limit: {CurrentLimitCount}");
                }
            }
            catch (OperationCanceledException) { }
        }, linkedCts.Token);*/

        try
        {
            while (true)
            {
                await SyncLock.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    if (_semaphore.Wait(0))
                        return true;
                }
                finally
                {
                    SyncLock.Release();
                }

                await Task.Delay(10, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (ObjectDisposedException)
        {
            if (maxRetries <= 0)
                return false;

            return await TryWaitAsync(cancellationToken, maxRetries - 1).ConfigureAwait(false);
        }
        finally
        {
            //logCts.Cancel();
            //try { await logTask.ConfigureAwait(false); } catch { }
        }
    }


    public async Task<bool> TryReleaseAsync(int maxRetries = 10)
    {
        if (maxRetries <= 0)
            return false;

        try
        {
            await SyncLock.WaitAsync().ConfigureAwait(false);
            try
            {
                _semaphore.Release();
                return true;
            }
            finally
            {
                SyncLock.Release();
            }
        }
        catch (ObjectDisposedException)
        {
            return await TryReleaseAsync(maxRetries - 1).ConfigureAwait(false);
        }
    }


    public async Task RunAsync(Func<CancellationToken, Task> operation, CancellationToken cancellationToken = default)
    {
        await TryWaitAsync(cancellationToken).ConfigureAwait(false);
        var stopWatch = Stopwatch.StartNew();
        var cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var progressTask = Task.Run(async () =>
        {
            try
            {
                while (!cancellationTokenSource.Token.IsCancellationRequested)
                {
                    await Task.Delay(UpThrottleInMs + 1, cancellationTokenSource.Token).ConfigureAwait(false);
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
            await TryReleaseAsync().ConfigureAwait(false);
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

                int used = CurrentLimitCount - _semaphore.CurrentCount;
                int available = Math.Max(0, newLimit - used);

                var newSemaphore = new SemaphoreSlim(available);
                var oldSemaphore = Interlocked.Exchange(ref _semaphore, newSemaphore);
                oldSemaphore.Dispose();
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
        _semaphore.Dispose();
        SyncLock.Dispose();
        LatencyLock.Dispose();
    }
}
