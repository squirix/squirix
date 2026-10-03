using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Entries and envelopes shared by the follower sender tests.</summary>
internal static class ReplicaSenderTestKit
{
    /// <summary>Hang protection only: no test waits for this on its success path.</summary>
    internal static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>The replication envelope of the leader n1.</summary>
    internal static readonly ReplicaRpcHeader Header = new("n1", new byte[] { 9, 8, 7 }, 1, 1, "n1", "n1");

    /// <summary>Creates a sender for the follower n2 that starts after an empty log.</summary>
    /// <param name="gateway">The follower transport double.</param>
    /// <param name="appendTimeout">The append timeout; five seconds when <see langword="null" />.</param>
    /// <returns>The sender.</returns>
    internal static ReplicaFollowerSender CreateSender(IReplicaRpcGateway gateway, TimeSpan? appendTimeout = null) =>
        new(gateway, "n2", in Header, 0, 0, appendTimeout ?? TimeSpan.FromSeconds(5));

    /// <summary>Queues the entry at <paramref name="index" /> as the commit fan-out does; its commit index is the index before it.</summary>
    /// <param name="sender">The sender.</param>
    /// <param name="index">The log index of the entry.</param>
    /// <param name="term">The term of the entry.</param>
    /// <param name="payloadBytes">The canonical payload size of the entry.</param>
    /// <param name="prevIndex">The predecessor index; the entry before <paramref name="index" /> when <see langword="null" />.</param>
    /// <returns>The task of the acknowledgement.</returns>
    internal static Task<ReplicaDurableAcknowledgement> EnqueueAsync(ReplicaFollowerSender sender, ulong index, ulong term = 1, int payloadBytes = 1, ulong? prevIndex = null)
    {
        var identity = new ReplicaOperationIdentity("group-a", "client", $"op-{index}", new byte[] { 1 });
        var mutation = new PreparedReplicaMutation(identity, term, index, new ReplicaMutationPayload(new byte[payloadBytes], new byte[] { 3 }, 4));
        var record = new ReplicaLogRecord(index, term, $"op-{index}", "client", new byte[] { 1 }, "UserMutation", "cache", new byte[] { 1 }, "Set", new byte[] { 1 }, ReadOnlyMemory<byte>.Empty, 0, 0, 0, 4);
        return sender.EnqueueAsync(mutation, in record, prevIndex ?? (index - 1), term, index - 1);
    }

    /// <summary>Waits for a task with the hang guard, so a missing step fails the test instead of hanging it.</summary>
    /// <typeparam name="T">The task result type.</typeparam>
    /// <param name="task">The task to wait for.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The task bounded by the guard.</returns>
    internal static Task<T> BoundedAsync<T>(Task<T> task, CancellationToken cancellationToken) => task.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
}
