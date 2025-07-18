public class PauseTokenSource
{
    private volatile TaskCompletionSource<bool> _paused = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public bool IsPaused => !_paused.Task.IsCompleted;

    public PauseToken Token => new PauseToken(this);

    public void Pause()
    {
        if (IsPaused) return;
        _paused = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public void Resume()
    {
        if (!IsPaused) return;
        _paused.TrySetResult(true);
    }

    internal Task WaitWhilePausedAsync() => _paused.Task;
}

public readonly struct PauseToken
{
    private readonly PauseTokenSource _source;
    public PauseToken(PauseTokenSource source) => _source = source;
    public bool IsPaused => _source?.IsPaused ?? false;
    public Task WaitWhilePausedAsync() => _source?.WaitWhilePausedAsync() ?? Task.CompletedTask;
}
