using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Records which peers called this node, and wakes the first start of the group this node owns when one does.</summary>
/// <remarks>
/// A status or an offer call of a peer proves nothing but that the peer is up, so the first start reads the senders it has seen since its
/// iteration began and runs again at once only for the one member it waits for. The wake is a shortcut of the schedule of the first start: a
/// sender recorded before the iteration began is already visible to it, and one recorded while it ran is read after it. Recording never
/// waits and never throws: it runs on the replication RPC paths. The first start of the group is the single reader.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaPeerContact
{
    private readonly Lock _gate = new();
    private readonly HashSet<string> _senders = [with(StringComparer.Ordinal)];
    private readonly ReplicaApplySignal _signal = new();

    /// <summary>Records a call of a peer and wakes the wait of the first start.</summary>
    /// <param name="senderNodeId">The peer, as the transport verified it.</param>
    internal void Notify(string senderNodeId)
    {
        lock (_gate)
            _ = _senders.Add(senderNodeId);

        _signal.Notify();
    }

    /// <summary>Tells whether a peer called since the last <see cref="Reset" />.</summary>
    /// <param name="nodeId">The peer.</param>
    /// <returns><see langword="true" /> when the peer called.</returns>
    internal bool Contacted(string nodeId)
    {
        lock (_gate)
            return _senders.Contains(nodeId);
    }

    /// <summary>Forgets the peers seen so far, as an iteration of the first start begins and reads their state itself.</summary>
    internal void Reset()
    {
        lock (_gate)
            _senders.Clear();
    }

    /// <summary>Waits until a call of a peer woke the wait or <paramref name="delay" /> elapses, then takes the wake.</summary>
    /// <param name="delay">The longest wait when nothing woke it.</param>
    /// <param name="timeProvider">The time source of the delay.</param>
    /// <param name="cancellationToken">Cancellation token; its cancellation ends the wait with <see cref="TimedWaitResult.Canceled" />.</param>
    /// <returns>
    /// <see cref="TimedWaitResult.Signaled" /> when a call was pending; <see cref="TimedWaitResult.TimedOut" /> when the delay elapsed first;
    /// <see cref="TimedWaitResult.Canceled" /> when the token was canceled.
    /// </returns>
    internal Task<TimedWaitResult> WaitAsync(TimeSpan delay, TimeProvider timeProvider, CancellationToken cancellationToken) =>
        _signal.WaitAsync(delay, timeProvider, cancellationToken);
}
