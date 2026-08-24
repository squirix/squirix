using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Pooled void durability ack backed by <see cref="ManualResetValueTaskSourceCore{TResult}" />.</summary>
internal sealed class PooledAck : IValueTaskSource
{
    private static readonly ConcurrentBag<PooledAck> Pool = [];

    private ManualResetValueTaskSourceCore<bool> _core;
    private int _leased;
    private CancellationTokenRegistration _registration;
    private short _registrationVersion;

    private PooledAck()
    {
        _core = default;
    }

    void IValueTaskSource.GetResult(short token) => _core.GetResult(token);

    ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _core.GetStatus(token);

    void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
        _core.OnCompleted(continuation, state, token, flags);

    internal static PooledAck Rent()
    {
        while (Pool.TryTake(out var ack))
        {
            if (Interlocked.CompareExchange(ref ack._leased, 1, 0) == 0)
                return ack;
        }

        return new PooledAck { _leased = 1 };
    }

    internal ValueTask WaitAsync(CancellationToken cancellationToken)
    {
        _core.Reset();
        _registrationVersion = _core.Version;
        var pending = new ValueTask(this, _core.Version);
        if (!cancellationToken.CanBeCanceled)
            return pending;

        // The version captured at registration is the guard against a stale cancellation reaching a
        // re-rented ack: every WaitAsync advances the core version, so a callback registered for a
        // previous lease never matches the current lease's version.
        _registration = cancellationToken.Register(
            static (state, ct) =>
            {
                if (state is PooledAck ack && ack._core.Version == ack._registrationVersion)
                    _ = ack.TrySetCanceled(ct);
            },
            this);

        return pending;
    }

    internal void Return()
    {
        // Only the owner of the current lease may return the ack; the CAS makes Return idempotent so a
        // duplicate return (e.g. caller and journal both attempting) is a no-op rather than a double-pool.
        if (Interlocked.CompareExchange(ref _leased, 0, 1) != 1)
            return;

        // Remove the cancellation callback from its token so a late fire cannot reach a re-rented ack.
        // Disposal here is intentionally synchronous; MA0045 is suppressed via ExcludeFromBlockingCallAnalysis on
        // CancellationTokenRegistration.Dispose in AssemblyInfo.
        _registration.Dispose();

        // The core is intentionally NOT reset here. The journal completes and pools the ack before the caller's
        // awaiter necessarily attaches; bumping the version here would make a late GetStatus/GetResult throw
        // InvalidOperationException on the stale token. The version advances at the next lease's WaitAsync, by
        // which point the previous awaiter has observed completion.
        Pool.Add(this);
    }

    internal bool TrySetCanceled(CancellationToken cancellationToken) => TrySetException(new OperationCanceledException(cancellationToken));

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
}
