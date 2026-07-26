using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Pooled void durability waiter backed by <see cref="ManualResetValueTaskSourceCore{TResult}" />.</summary>
internal sealed class JournalDurabilityWaiter : IValueTaskSource
{
    private static readonly ConcurrentBag<JournalDurabilityWaiter> Pool = [];
    private int _abandonedByCaller;

    private ManualResetValueTaskSourceCore<bool> _core;
    private int _leased;

    private JournalDurabilityWaiter()
    {
        _core = default;
    }

    void IValueTaskSource.GetResult(short token) => _core.GetResult(token);

    ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _core.GetStatus(token);

    void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
        _core.OnCompleted(continuation, state, token, flags);

    internal static JournalDurabilityWaiter Rent()
    {
        while (Pool.TryTake(out var waiter))
            if (Interlocked.CompareExchange(ref waiter._leased, 1, 0) is 0)
                return waiter;

        return new JournalDurabilityWaiter { _leased = 1 };
    }

    internal ValueTask AwaitAsync(CancellationToken cancellationToken)
    {
        _core.Reset();
        var pending = new ValueTask(this, _core.Version);
        return cancellationToken.CanBeCanceled ? AwaitWithCancellationAsync(pending, cancellationToken) : pending;
    }

    internal bool IsAbandonedByCaller() => Volatile.Read(ref _abandonedByCaller) is not 0;

    internal void MarkAbandonedByCaller() => Volatile.Write(ref _abandonedByCaller, 1);

    internal void ReturnToPool()
    {
        if (Interlocked.CompareExchange(ref _leased, 0, 1) is not 1)
            return;

        Volatile.Write(ref _abandonedByCaller, 0);
        _core.Reset();
        Pool.Add(this);
    }

    internal void SetCanceled(CancellationToken cancellationToken) => _core.SetException(new OperationCanceledException(cancellationToken));

    internal void SetException(Exception error) => _core.SetException(error);

    internal void SetResult() => _core.SetResult(true);

    internal bool TrySetCanceled(CancellationToken cancellationToken)
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

    internal bool TrySetException(Exception error)
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

    internal bool TrySetResult()
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

    private static ValueTask AwaitWithCancellationAsync(ValueTask pending, CancellationToken cancellationToken) => new(pending.AsTask().WaitAsync(cancellationToken));
}
