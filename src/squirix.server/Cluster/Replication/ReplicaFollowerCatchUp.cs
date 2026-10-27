using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>A lease on a paused <see cref="ReplicaFollowerSender" /> that lets a catch-up send to the follower in the sender's place.</summary>
/// <remarks>
/// While the lease is held the sender's live loop is stopped and the entries enqueued meanwhile wait. Disposing the lease resumes the
/// sender: the waiting entries at or below the index the catch-up marked as held are acknowledged without a request, since the follower
/// already holds them, and the rest are sent by the resumed loop in order.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaFollowerCatchUp : IDisposable
{
    private readonly ReplicaFollowerSender _sender;
    private ulong _heldThrough;
    private int _disposed;

    internal ReplicaFollowerCatchUp(ReplicaFollowerSender sender, string nodeId)
    {
        _sender = sender;
        NodeId = nodeId;
    }

    /// <summary>Gets a value indicating whether the sender behind the lease is closed, so no request can be sent any more.</summary>
    internal bool IsClosed => _sender.IsClosed;

    /// <summary>Gets the identifier of the follower the lease sends to.</summary>
    internal string NodeId { get; }

    /// <summary>Resumes the sender's live sends; idempotent.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        _sender.EndCatchUp(Volatile.Read(ref _heldThrough));
    }

    /// <summary>Records the highest log index the follower is known to hold after a catch-up request it accepted.</summary>
    /// <param name="heldThrough">The log index; never lowered.</param>
    internal void MarkHeld(ulong heldThrough)
    {
        if (heldThrough > Volatile.Read(ref _heldThrough))
            Volatile.Write(ref _heldThrough, heldThrough);
    }

    /// <summary>Sends one append request to the follower with the sender's identity and per-request timeout.</summary>
    /// <param name="batch">The request.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// The follower's answer. The task fails with <see cref="InvalidOperationException" /> when a request of this lease is still in
    /// flight, with <see cref="ObjectDisposedException" /> when the lease was disposed or the sender is closed or draining, and with
    /// <see cref="OperationCanceledException" /> when the request timed out, the sender closed meanwhile, or
    /// <paramref name="cancellationToken" /> was canceled.
    /// </returns>
    internal Task<FollowerLogAppendResult> SendAsync(in FollowerBatch batch, CancellationToken cancellationToken) =>
        Volatile.Read(ref _disposed) != 0
            ? Task.FromException<FollowerLogAppendResult>(new ObjectDisposedException(nameof(ReplicaFollowerCatchUp)))
            : _sender.SendCatchUpAsync(in batch, cancellationToken);
}
