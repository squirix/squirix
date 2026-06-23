using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Pooled void durability waiter backed by <see cref="ManualResetValueTaskSourceCore{TResult}"/>.</summary>
internal sealed class JournalDurabilityWaiter : IValueTaskSource
{
    private static readonly ConcurrentBag<JournalDurabilityWaiter> Pool = [];

    private ManualResetValueTaskSourceCore<bool> _core;
    private int _leased;

    private JournalDurabilityWaiter() => _core = default;

    public static JournalDurabilityWaiter Rent()
    {
        while (Pool.TryTake(out var waiter))
        {
            if (Interlocked.CompareExchange(ref waiter._leased, 1, 0) is 0)
                return waiter;
        }

        return new JournalDurabilityWaiter { _leased = 1 };
    }

    public ValueTask AwaitAsync(CancellationToken cancellationToken)
    {
        _core.Reset();
        var pending = new ValueTask(this, _core.Version);
        return cancellationToken.CanBeCanceled ? AwaitWithCancellationAsync(pending, cancellationToken) : pending;
    }

    public void SetResult() => _core.SetResult(true);

    public bool TrySetResult()
    {
        try
        {
            _core.SetResult(true);
            return true;
        }
        catch (InvalidOperationException)
        {
            return false;
        }
    }

    public void SetException(Exception error) => _core.SetException(error);

    public bool TrySetException(Exception error)
    {
        try
        {
            _core.SetException(error);
            return true;
        }
        catch (InvalidOperationException)
        {
            return false;
        }
    }

    public void SetCanceled(CancellationToken cancellationToken) => _core.SetException(new OperationCanceledException(cancellationToken));

    public bool TrySetCanceled(CancellationToken cancellationToken)
    {
        try
        {
            _core.SetException(new OperationCanceledException(cancellationToken));
            return true;
        }
        catch (InvalidOperationException)
        {
            return false;
        }
    }

    public void ReturnToPool()
    {
        if (Interlocked.CompareExchange(ref _leased, 0, 1) is not 1)
            return;

        _core.Reset();
        Pool.Add(this);
    }

    void IValueTaskSource.GetResult(short token) => _core.GetResult(token);

    ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _core.GetStatus(token);

    void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
        _core.OnCompleted(continuation, state, token, flags);

    private static async ValueTask AwaitWithCancellationAsync(ValueTask pending, CancellationToken cancellationToken) =>
        await pending.AsTask().WaitAsync(cancellationToken).ConfigureAwait(false);
}
