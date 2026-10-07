using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Wakes the apply loop of one replica group when its log may hold committed entries memory has not received yet.</summary>
/// <remarks>
/// Notifications coalesce: at most one is pending, and any number raised before the loop takes it count as one. A notification raised
/// while the loop runs a pass stays pending, so the loop runs one more pass and never misses an entry. Notifying never waits and never
/// throws: it runs on the replication RPC paths. The apply loop of the group is the single reader.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaApplySignal
{
    private readonly Channel<bool> _pending = Channel.CreateBounded<bool>(
        new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropWrite, SingleReader = true, SingleWriter = false });

    /// <summary>Marks the group as having entries to apply; a notification already pending absorbs this one.</summary>
    internal void Notify() => _ = _pending.Writer.TryWrite(true);

    /// <summary>Waits until a notification is pending or <paramref name="delay" /> elapses, then takes the pending notification.</summary>
    /// <param name="delay">The longest wait when nothing is pending.</param>
    /// <param name="timeProvider">The time source of the delay.</param>
    /// <param name="cancellationToken">Cancellation token; its cancellation ends the wait by throwing.</param>
    /// <returns><see langword="true" /> when a notification was pending; <see langword="false" /> when the delay elapsed first.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken" /> was canceled.</exception>
    internal async Task<bool> WaitAsync(TimeSpan delay, TimeProvider timeProvider, CancellationToken cancellationToken)
    {
        if (_pending.Reader.TryRead(out _))
            return true;

        using var timeout = new CancellationTokenSource(delay, timeProvider);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
        try
        {
            _ = await _pending.Reader.WaitToReadAsync(linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return false;
        }

        return _pending.Reader.TryRead(out _);
    }
}
