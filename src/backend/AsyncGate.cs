using System.Threading;
using System.Threading.Tasks;

public class AsyncBlockingGate
{
    private TaskCompletionSource _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly SemaphoreSlim _mutex = new(1, 1);

    public async Task<bool> IsBlockedAsync()
    {
        await _mutex.WaitAsync().ConfigureAwait(false);
        try
        {
            return !_tcs.Task.IsCompleted;
        }
        finally
        {
            _mutex.Release();
        }
    }
    
    public async Task<bool> WaitIfBlockedAsync(
        Func<Task>? onBlockedAsync = null,
        TimeSpan? whileBlockedLoopDelay = null,
        Func<Task>? whileBlockedAsync = null)
    {
        await _mutex.WaitAsync().ConfigureAwait(false);
        Task taskToWait;
        bool isBlocked;
        try
        {
            taskToWait = _tcs.Task;
            isBlocked = !taskToWait.IsCompleted;
        }
        finally
        {
            _mutex.Release();
        }

        if (isBlocked)
        {
            if (onBlockedAsync != null)
            {
                await onBlockedAsync().ConfigureAwait(false);
            }

            if (whileBlockedAsync != null)
            {
                var delay = whileBlockedLoopDelay ?? TimeSpan.FromMilliseconds(10);
                while (!taskToWait.IsCompleted)
                {
                    await whileBlockedAsync().ConfigureAwait(false);
                    await Task.Delay(delay).ConfigureAwait(false);
                }
            }
        }

        await taskToWait.ConfigureAwait(false);
        return isBlocked;
    }



    /// <summary>
    /// Blocks the gate.
    /// Future calls to WaitIfBlockedAsync will asynchronously wait until unblocked.
    /// </summary>
    public async Task SetBlockedAsync()
    {
        await _mutex.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_tcs.Task.IsCompleted)
            {
                _tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }
        }
        finally
        {
            _mutex.Release();
        }
    }

    /// <summary>
    /// Unblocks the gate.
    /// All waiting calls to WaitIfBlockedAsync will resume.
    /// </summary>
    public async Task SetUnblockedAsync()
    {
        await _mutex.WaitAsync().ConfigureAwait(false);
        try
        {
            if (!_tcs.Task.IsCompleted)
            {
                _tcs.TrySetResult();
            }
        }
        finally
        {
            _mutex.Release();
        }
    }
}
