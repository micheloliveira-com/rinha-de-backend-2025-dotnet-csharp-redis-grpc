using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

public class AdaptativeLimiter : IDisposable
{
    private readonly SemaphoreSlim _syncLock = new(1, 1);
    private readonly SemaphoreSlim _latencyLock = new(1, 1);

    private int _currentLimit;
    private readonly int _minLimit = 10;
    private readonly int _maxLimit = 50;
    private readonly int _step = 5;
    private readonly int _windowSize = 10;

    private SemaphoreSlim _semaphore;
    private readonly Queue<long> _latencies = new();

    public AdaptativeLimiter()
    {
        _currentLimit = 30;
        _semaphore = new SemaphoreSlim(_currentLimit);
    }

    public async Task RunAsync(Func<CancellationToken, Task> operation, CancellationToken cancellationToken = default)
    {
        await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        var sw = Stopwatch.StartNew();
        var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var progressTask = Task.Run(async () =>
        {
            try
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    await Task.Delay(51, cts.Token).ConfigureAwait(false);
                    await RecordLatencyAsync(sw.ElapsedMilliseconds).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { }
        }, cts.Token);

        try
        {
            await operation(cts.Token).ConfigureAwait(false);
        }
        finally
        {
            sw.Stop();
            cts.Cancel();
            try { await progressTask.ConfigureAwait(false); } catch { }

            await RecordLatencyAsync(sw.ElapsedMilliseconds).ConfigureAwait(false);
            _semaphore.Release();
        }
    }

    private async Task RecordLatencyAsync(long ms)
    {
        await _latencyLock.WaitAsync().ConfigureAwait(false);
        try
        {
            _latencies.Enqueue(ms);
            if (_latencies.Count > _windowSize)
                _latencies.Dequeue();
        }
        finally
        {
            _latencyLock.Release();
        }

        await AdjustLimitAsync().ConfigureAwait(false);
    }

    private async Task AdjustLimitAsync()
    {
        double avgLatency;

        await _latencyLock.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_latencies.Count == 0) return;
            avgLatency = _latencies.Average();
        }
        finally
        {
            _latencyLock.Release();
        }

        await _syncLock.WaitAsync().ConfigureAwait(false);
        try
        {
            int newLimit = _currentLimit;

            if (avgLatency > 50 && newLimit > _minLimit)
                newLimit = Math.Max(_minLimit, newLimit - _step);
            else if (avgLatency < 40 && newLimit < _maxLimit)
                newLimit = Math.Min(_maxLimit, newLimit + _step);

            if (newLimit != _currentLimit)
            {
                Console.WriteLine($"[Limiter] New concurrency limit: {newLimit} | Avg latency: {avgLatency:0.0}ms");

                int used = _currentLimit - _semaphore.CurrentCount;
                int available = Math.Max(0, newLimit - used);

                var newSemaphore = new SemaphoreSlim(available);

                _semaphore.Dispose();
                _semaphore = newSemaphore;
                _currentLimit = newLimit;
            }
        }
        finally
        {
            _syncLock.Release();
        }
    }

    public void Dispose()
    {
        _semaphore.Dispose();
        _syncLock.Dispose();
        _latencyLock.Dispose();
    }
}
