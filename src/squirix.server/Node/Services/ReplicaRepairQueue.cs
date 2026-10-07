using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Services;

/// <summary>The follower slots of the owned replica group waiting for repair, consumed by <see cref="ReplicaGroupReadinessService" />.</summary>
/// <remarks>
/// A slot is queued when the commit path demotes its follower, at most once until the readiness service takes it, so the queue never
/// holds more entries than the group has slots. Enqueueing never waits and never throws: it runs on the commit and observation paths.
/// The readiness service is the single reader; it verifies and catches up the queued followers on its own loop.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaRepairQueue
{
    private readonly bool[] _queued;
    private readonly Channel<int> _slots;
    private readonly Lock _sync = new();

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

    /// <summary>Waits until a slot is queued or <paramref name="delay" /> elapses, then takes every queued slot.</summary>
    /// <param name="delay">The longest wait when nothing is queued.</param>
    /// <param name="timeProvider">The time source of the delay.</param>
    /// <param name="cancellationToken">Cancellation token; its cancellation ends the wait by throwing.</param>
    /// <returns><see langword="true" /> when slots were queued; <see langword="false" /> when the delay elapsed first.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken" /> was canceled.</exception>
    internal async Task<bool> WaitAsync(TimeSpan delay, TimeProvider timeProvider, CancellationToken cancellationToken)
    {
        if (TakeAll())
            return true;

        using var timeout = new CancellationTokenSource(delay, timeProvider);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
        try
        {
            _ = await _slots.Reader.WaitToReadAsync(linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return false;
        }

        return TakeAll();
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
