using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;

namespace Squirix.Server.Node.Services;

/// <summary>The follower slots of the owned replica group waiting for repair, consumed by <see cref="ReplicaGroupReadinessService" />.</summary>
/// <remarks>
/// A slot is queued when the commit path demotes its follower, or when a follower out of the write quorum answers an elected leader, at
/// most once until the readiness service takes it, so the queue never
/// holds more entries than the group has slots. Enqueueing never waits and never throws: it runs on the commit and observation paths.
/// The readiness service is the single reader; it verifies and catches up the queued followers on its own loop.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaRepairQueue
{
    private readonly bool[] _queued;
    private readonly Channel<int> _slots;
    private readonly Lock _sync = new();
    private Task? _slotWait;

    /// <summary>Initializes a new instance of the <see cref="ReplicaRepairQueue" /> class.</summary>
    /// <param name="replicaCount">The replica count of the group, including the leader.</param>
    internal ReplicaRepairQueue(int replicaCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(replicaCount);
        _queued = new bool[replicaCount];
        _slots = Channel.CreateBounded<int>(new BoundedChannelOptions(replicaCount) { SingleReader = true, SingleWriter = false });
    }

    /// <summary>Queues a follower slot for repair unless it is already queued.</summary>
    /// <param name="replicaIndex">Zero-based follower slot.</param>
    internal void Enqueue(int replicaIndex)
    {
        lock (_sync)
        {
            if (_queued[replicaIndex])
                return;

            _queued[replicaIndex] = true;
        }

        // Each slot is in the channel at most once, and its capacity is the slot count, so the write always fits.
        _ = _slots.Writer.TryWrite(replicaIndex);
    }

    /// <summary>Wakes the reader without naming a follower, as a queued slot does.</summary>
    /// <remarks>The reader only learns that something was queued, so the wake occupies the first slot until the reader takes it.</remarks>
    internal void Wake() => Enqueue(0);

    /// <summary>Waits until a slot is queued or <paramref name="delay" /> elapses, then takes every queued slot.</summary>
    /// <param name="delay">The longest wait when nothing is queued.</param>
    /// <param name="timeProvider">The time source of the delay.</param>
    /// <param name="cancellationToken">Cancellation token; its cancellation ends the wait with <see cref="TimedWaitResult.Canceled" />, not an exception.</param>
    /// <returns>
    /// <see cref="TimedWaitResult.Signaled" /> when slots were taken; <see cref="TimedWaitResult.TimedOut" /> when the delay elapsed first or
    /// nothing was left to take; <see cref="TimedWaitResult.Canceled" /> when the token was canceled with nothing queued.
    /// </returns>
    internal async Task<TimedWaitResult> WaitAsync(TimeSpan delay, TimeProvider timeProvider, CancellationToken cancellationToken)
    {
        if (TakeAll())
            return TimedWaitResult.Signaled;

        // The channel wait itself is never canceled, since that would throw inside the channel on every timeout: it stays pending across
        // calls and only the wrapper that waits for it is canceled. The channel has one reader at a time, so the lock only orders the
        // hand-over between a retiring loop and its successor.
        Task slotWait;
        lock (_sync)
        {
            _slotWait = _slotWait is { IsCompleted: false } ? _slotWait : _slots.Reader.WaitToReadAsync(CancellationToken.None).AsTask();
            slotWait = _slotWait;
        }

        var result = await TimedWait.WaitAsync(
            slotWait,
            static (wait, token) => wait.WaitAsync(token),
            static wait => wait.IsCompleted,
            delay,
            timeProvider,
            cancellationToken).ConfigureAwait(false);

        // A wake that finds nothing left to take is a timeout, as before.
        return result != TimedWaitResult.Signaled || TakeAll() ? result : TimedWaitResult.TimedOut;
    }

    private bool TakeAll()
    {
        var any = false;
        while (_slots.Reader.TryRead(out var replicaIndex))
        {
            lock (_sync)
                _queued[replicaIndex] = false;

            any = true;
        }

        return any;
    }
}
