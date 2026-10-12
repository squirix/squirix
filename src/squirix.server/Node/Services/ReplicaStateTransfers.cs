using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>Repairs a follower that cannot be caught up from the log of the leader by sending it the state of the group.</summary>
/// <remarks>
/// <para>
/// Runs inside the catch-up pass of the follower, under the lease of its sender, when the entry session ended because the leader no longer
/// retains the entries the follower needs, or because the follower answered that a transfer to it was interrupted. The state is captured under the
/// commit gate at the applied index, without asking for authority, and the log is pinned at it; the stream then runs outside the gate. A
/// transfer that reached the end is followed by a fresh entry session from the next index, which replays what was written meanwhile.
/// </para>
/// <para>
/// Every accepted reply of the follower is a contact of the leader in its term and holds the authority deadline of the group; a reply of a
/// higher term is observed by the election state. The pin is released by the caller, whatever way the pass ends. Each attempt is logged when it
/// starts and when it ends, and counted by outcome.
/// </para>
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaStateTransfers
{
    private readonly IJournalDurabilityCoordinator _durability;
    private readonly ReplicaGroupEntries _entries;
    private readonly IReplicaTransferGateway _gateway;
    private readonly ILogger _log;
    private readonly ReplicaCatchUpMetrics? _metrics;
    private readonly ReplicaTransferOptions _options;

    /// <summary>Initializes a new instance of the <see cref="ReplicaStateTransfers" /> class.</summary>
    /// <param name="gateway">The transfer RPCs to the followers.</param>
    /// <param name="entries">The walk over the stored entries of the node.</param>
    /// <param name="durability">The node cache journal whose frames the applies appended.</param>
    /// <param name="options">The message limits and the step timeout.</param>
    /// <param name="log">Logger of the transfers.</param>
    /// <param name="metrics">Counts the transfers; none are counted when <see langword="null" />.</param>
    internal ReplicaStateTransfers(
        IReplicaTransferGateway gateway,
        ReplicaGroupEntries entries,
        IJournalDurabilityCoordinator durability,
        ReplicaTransferOptions options,
        ILogger<ReplicaStateTransfers> log,
        ReplicaCatchUpMetrics? metrics)
    {
        ArgumentNullException.ThrowIfNull(gateway);
        ArgumentNullException.ThrowIfNull(entries);
        ArgumentNullException.ThrowIfNull(durability);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(log);
        _gateway = gateway;
        _entries = entries;
        _durability = durability;
        _options = options;
        _log = log;
        _metrics = metrics;
    }

    /// <summary>Sends the state of the group to a follower and runs a fresh entry session from the transferred index.</summary>
    /// <param name="committer">The committer of the led group.</param>
    /// <param name="target">The follower and the pipeline its catch-up runs in.</param>
    /// <param name="lease">The lease of the follower's sender, which the pass holds.</param>
    /// <param name="pin">The hold of this attempt on the log; taken by the capture, released by the caller.</param>
    /// <param name="cancellationToken">Cancellation token of the pass; its cancellation ends the attempt by throwing.</param>
    /// <returns>The result of the entry session that followed the transfer; <see langword="null" /> when nothing was transferred, the reason being logged and counted.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken" /> was canceled.</exception>
    internal async Task<ReplicaCatchUpResult?> RepairAsync(
        ReplicaGroupCommitter committer,
        ReplicaCatchUpTarget target,
        ReplicaFollowerCatchUp lease,
        ReplicaTransferPin.Session pin,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(committer);
        ArgumentNullException.ThrowIfNull(lease);
        var (captured, snapshot) = await committer.CaptureForTransferAsync(target, _durability, pin, cancellationToken).ConfigureAwait(false);
        if (captured != ReplicaTransferOutcome.Transferred)
        {
            ReportDeclined(committer, lease.NodeId, captured);
            return null;
        }

        var sent = await SendStateAsync(committer, (target, lease), snapshot, cancellationToken).ConfigureAwait(false);
        return sent == ReplicaTransferOutcome.Transferred
            ? await new ReplicaEntryCatchUpSession(target.Log, committer.GroupId, target.Term).RunAsync(lease, cancellationToken).ConfigureAwait(false)
            : null;
    }

    /// <summary>Posts a reply of a follower to the election state of the group.</summary>
    /// <param name="election">The election state of the group.</param>
    /// <param name="replicaIndex">The slot of the follower.</param>
    /// <param name="reply">The reply.</param>
    /// <remarks>
    /// An accepted reply is a contact of the follower in the led term and proves the transfer makes progress; a refusal posts the term it
    /// carries, which the election state observes when it is higher. Neither counts toward a commit or a read index confirmation.
    /// </remarks>
    private static void Post(ReplicaGroupState election, int replicaIndex, in ReplicaTransferReply reply)
    {
        if (!reply.Success)
        {
            election.ObserveHigherTerm(reply.Term);
            return;
        }

        election.RecordFollowerContact(replicaIndex, reply.Term);
        election.TransferProgress.Record();
    }

    private void ReportDeclined(ReplicaGroupCommitter committer, string nodeId, ReplicaTransferOutcome outcome)
    {
        var name = ReplicaTransferOutcomeNames.Of(outcome);
        _metrics?.ReportTransfer(committer.Probe.SelfId, committer.GroupId, name, string.Empty);
        ReplicaTransferLog.Declined(_log, committer.GroupId, nodeId, name);
    }

    /// <summary>Streams the captured state to the follower, logging when it starts and when it ends and counting how it ended.</summary>
    /// <param name="committer">The committer of the led group.</param>
    /// <param name="catchUp">The follower's target and the lease of its sender.</param>
    /// <param name="snapshot">The captured group snapshot.</param>
    /// <param name="cancellationToken">Cancellation token of the pass.</param>
    /// <returns>How the stream ended.</returns>
    private async Task<ReplicaTransferOutcome> SendStateAsync(
        ReplicaGroupCommitter committer,
        (ReplicaCatchUpTarget Target, ReplicaFollowerCatchUp Lease) catchUp,
        GroupSnapshot snapshot,
        CancellationToken cancellationToken)
    {
        var (target, lease) = catchUp;
        var clock = committer.BudgetTimeProvider;
        var started = clock.GetTimestamp();
        var (_, header) = committer.Probe.BuildMembership(target.Term);
        ReplicaTransferLog.Sending(_log, committer.GroupId, lease.NodeId, snapshot.LastIncludedIndex, target.Term);
        var election = committer.Election;
        var slot = target.ReplicaIndex;
        var stream = new ReplicaTransferStream(_gateway, _options, in header, lease.NodeId, clock)
        {
            Draining = target.Sender.DrainStarted,
            Replies = election == null ? null : reply => Post(election, slot, in reply),
        };
        var result = await stream.RunAsync(snapshot, _entries, cancellationToken).ConfigureAwait(false);
        var name = ReplicaTransferOutcomeNames.Of(result.Outcome);
        var elapsed = clock.GetElapsedTime(started);
        var volume = (stream.Entries, stream.Bytes);
        _metrics?.ReportTransfer(committer.Probe.SelfId, committer.GroupId, name, result.Reason);
        ReplicaTransferLog.Sent(_log, committer.GroupId, lease.NodeId, snapshot.LastIncludedIndex, name, elapsed, volume);
        return result.Outcome;
    }
}
