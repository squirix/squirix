using System;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;
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
/// <para>
/// A journal-full refusal delays the next transfer by 30 seconds, doubling up to 10 minutes. A checksum-mismatch refusal instead follows
/// the readiness retry intervals, so recovery remains automatic within the normal verification window. A completed transfer clears the wait;
/// a different refusal policy starts at its first interval. A skipped attempt captures and counts nothing and does not delay other followers.
/// </para>
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaStateTransfers
{
    private static readonly TimeSpan RefusalCooldownMax = TimeSpan.FromMinutes(10);
    private static readonly TimeSpan RefusalCooldownStart = TimeSpan.FromSeconds(30);

    private readonly ConcurrentDictionary<(string GroupId, string NodeId), Cooldown> _cooldowns = new();
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

    /// <summary>Initializes the reader of the term ceiling; a follower that lost its disk is repaired only when it is set (#1126).</summary>
    internal ReplicaTermCeiling? TermCeiling { private get; init; }

    /// <summary>Initializes the checksum retry intervals, shared with follower readiness verification.</summary>
    internal ReplicaReadinessOptions ChecksumRetryOptions { private get; init; } = new();

    /// <summary>Posts a reply of a follower to the election state of the group.</summary>
    /// <param name="election">The election state of the group.</param>
    /// <param name="replicaIndex">The slot of the follower.</param>
    /// <param name="reply">The reply.</param>
    /// <remarks>
    /// An accepted reply is a contact of the follower in the led term and proves the transfer makes progress; a refusal posts the term it
    /// carries, which the election state observes when it is higher. A journal-busy refusal is also a contact, and the transfer is alive while
    /// it lasts; the stall budget of the stream bounds it. None counts toward a commit or a read index confirmation.
    /// </remarks>
    internal static void Post(ReplicaGroupState election, int replicaIndex, in ReplicaTransferReply reply)
    {
        if (!reply.Success)
        {
            election.ObserveHigherTerm(reply.Term);
            if (!string.Equals(reply.RefusalCode, RefusalCodes.JournalBusy, StringComparison.Ordinal))
                return;
        }

        _ = election.RecordFollowerContact(replicaIndex, reply.Term);
        election.TransferProgress.Record();
    }

    /// <summary>Sends the state of the group to a follower and runs a fresh entry session from the transferred index.</summary>
    /// <param name="committer">The committer of the led group.</param>
    /// <param name="target">The follower and the pipeline its catch-up runs in.</param>
    /// <param name="lease">The lease of the follower's sender, which the pass holds.</param>
    /// <param name="pin">The hold of this attempt on the log; taken by the capture, released by the caller.</param>
    /// <param name="unseeded">Whether the follower lost its disk: the term ceiling is read first, and the transfer carries it.</param>
    /// <param name="othersWait">Whether other followers wait for their repair behind this one; a transfer that stalls on a busy journal then yields at once.</param>
    /// <param name="cancellationToken">Cancellation token of the pass; its cancellation ends the attempt by throwing.</param>
    /// <returns>The result of the entry session that followed the transfer; <see langword="null" /> when nothing was transferred, the reason being logged and counted.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken" /> was canceled.</exception>
    /// <remarks>
    /// For a follower that lost its disk, a term above the leader's among the answers is observed by the election state, which steps the leader down,
    /// and nothing is sent; so is nothing when fewer members than the ceiling needs answered.
    /// </remarks>
    internal async Task<ReplicaCatchUpResult?> RepairAsync(
        ReplicaGroupCommitter committer,
        ReplicaCatchUpTarget target,
        ReplicaFollowerCatchUp lease,
        ReplicaTransferPin.Session pin,
        bool unseeded,
        bool othersWait,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(committer);
        ArgumentNullException.ThrowIfNull(lease);
        if (IsCoolingDown(committer.GroupId, lease.NodeId, committer.BudgetTimeProvider))
        {
            ReplicaTransferLog.Declined(_log, committer.GroupId, lease.NodeId, "refusal_cooldown");
            return null;
        }

        var ceiling = unseeded ? await CheckCeilingAsync(committer, target, cancellationToken).ConfigureAwait(false) : ReplicaTransferOutcome.Transferred;
        var (captured, snapshot) = ceiling == ReplicaTransferOutcome.Transferred
            ? await committer.CaptureForTransferAsync(target, _durability, pin, cancellationToken).ConfigureAwait(false)
            : (ceiling, default);
        if (captured != ReplicaTransferOutcome.Transferred)
        {
            ReportDeclined(committer, lease.NodeId, captured);
            return null;
        }

        var sent = await SendStateAsync(committer, (target, lease), (snapshot, unseeded, othersWait), cancellationToken).ConfigureAwait(false);
        return sent == ReplicaTransferOutcome.Transferred
            ? await new ReplicaEntryCatchUpSession(target.Log, committer.Probe.SelfId, target.Term).RunAsync(lease, cancellationToken).ConfigureAwait(false)
            : null;
    }

    /// <summary>Reads the term ceiling before a follower that lost its disk is repaired.</summary>
    /// <param name="committer">The committer of the led group.</param>
    /// <param name="target">The follower that lost its disk.</param>
    /// <param name="cancellationToken">Cancellation token of the pass.</param>
    /// <returns>
    /// <see cref="ReplicaTransferOutcome.Transferred" /> when the repair may go on; <see cref="ReplicaTransferOutcome.Superseded" /> when a member holds a
    /// higher term, which the election state observes; <see cref="ReplicaTransferOutcome.CeilingUnavailable" /> when too few members answered or no
    /// reader is set.
    /// </returns>
    private async Task<ReplicaTransferOutcome> CheckCeilingAsync(ReplicaGroupCommitter committer, ReplicaCatchUpTarget target, CancellationToken cancellationToken)
    {
        if (TermCeiling == null)
            return ReplicaTransferOutcome.CeilingUnavailable;

        var (members, header) = committer.Probe.BuildMembership(target.Term);
        var read = await TermCeiling.ReadAsync(members, header, target.ReplicaIndex, committer.Probe.LeaderReplicaIndex, cancellationToken).ConfigureAwait(false);
        ReplicaTransferLog.CeilingRead(_log, committer.GroupId, members[target.ReplicaIndex], target.Term, read.Answers, read.Needed, read.Highest);
        if (read.Highest <= target.Term)
            return read.Suffices ? ReplicaTransferOutcome.Transferred : ReplicaTransferOutcome.CeilingUnavailable;
        committer.Election?.ObserveHigherTerm(read.Highest);
        return ReplicaTransferOutcome.Superseded;
    }

    private bool IsCoolingDown(string groupId, string nodeId, TimeProvider clock) =>
        _cooldowns.TryGetValue((groupId, nodeId), out var cooldown) && clock.GetElapsedTime(cooldown.Started) < cooldown.Length;

    /// <summary>Starts or lengthens the wait after a journal-full or checksum-mismatch refusal, or ends it after a completed transfer.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="nodeId">The follower.</param>
    /// <param name="result">How the stream ended.</param>
    /// <param name="clock">The time source of the wait.</param>
    private void UpdateCooldown(string groupId, string nodeId, in ReplicaTransferStream.Result result, TimeProvider clock)
    {
        var key = (groupId, nodeId);
        if (result.Outcome == ReplicaTransferOutcome.Transferred)
        {
            _ = _cooldowns.TryRemove(key, out _);
            return;
        }

        if (result.Outcome != ReplicaTransferOutcome.FollowerRefused || result.Reason is not (RefusalCodes.JournalFull or RefusalCodes.ChecksumMismatch))
            return;

        var length = NextCooldown(key, result.Reason);
        _cooldowns[key] = new Cooldown(clock.GetTimestamp(), length, result.Reason);
        ReplicaTransferLog.CoolingDown(_log, groupId, nodeId, result.Reason, length);
    }

    private TimeSpan NextCooldown((string GroupId, string NodeId) key, string reason)
    {
        var checksum = string.Equals(reason, RefusalCodes.ChecksumMismatch, StringComparison.Ordinal);
        var maximum = checksum ? ChecksumRetryOptions.MaxDelay : RefusalCooldownMax;
        var initial = checksum ? TimeSpan.FromTicks(Math.Min(ChecksumRetryOptions.InitialDelay.Ticks, maximum.Ticks)) : RefusalCooldownStart;
        return _cooldowns.TryGetValue(key, out var previous) && string.Equals(previous.Reason, reason, StringComparison.Ordinal)
            ? TimeSpan.FromTicks(Math.Min(maximum.Ticks, previous.Length.Ticks * 2))
            : initial;
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
    /// <param name="state">The captured group snapshot, and whether the leader checked the term ceiling for a follower that lost its disk.</param>
    /// <param name="cancellationToken">Cancellation token of the pass.</param>
    /// <returns>How the stream ended.</returns>
    private async Task<ReplicaTransferOutcome> SendStateAsync(
        ReplicaGroupCommitter committer,
        (ReplicaCatchUpTarget Target, ReplicaFollowerCatchUp Lease) catchUp,
        (GroupSnapshot Snapshot, bool CeilingChecked, bool OthersWait) state,
        CancellationToken cancellationToken)
    {
        var (snapshot, ceilingChecked, othersWait) = state;
        var ((slot, _, replicaFollowerSender, _, term), lease) = catchUp;
        var clock = committer.BudgetTimeProvider;
        var started = clock.GetTimestamp();
        var (_, header) = committer.Probe.BuildMembership(term);
        ReplicaTransferLog.Sending(_log, committer.GroupId, lease.NodeId, snapshot.LastIncludedIndex, term);
        var election = committer.Election;
        var stream = new ReplicaTransferStream(_gateway, _options, in header, lease.NodeId, clock)
        {
            Draining = replicaFollowerSender.DrainStarted,
            CeilingChecked = ceilingChecked,
            Replies = election == null ? null : reply => Post(election, slot, in reply),
            Paused = () => ReplicaTransferLog.Paused(_log, committer.GroupId, lease.NodeId),
            OthersWait = othersWait ? static () => true : null,
        };
        var result = await stream.RunAsync(snapshot, _entries, cancellationToken).ConfigureAwait(false);
        UpdateCooldown(committer.GroupId, lease.NodeId, in result, clock);
        var name = ReplicaTransferOutcomeNames.Of(result.Outcome);
        var elapsed = clock.GetElapsedTime(started);
        var volume = (stream.Entries, stream.Bytes);
        _metrics?.ReportTransfer(committer.Probe.SelfId, committer.GroupId, name, result.Reason);
        ReplicaTransferLog.Sent(_log, committer.GroupId, lease.NodeId, snapshot.LastIncludedIndex, name, elapsed, volume);
        return result.Outcome;
    }

    /// <summary>The wait before the next transfer to a follower that refused as journal-full or checksum-mismatch.</summary>
    /// <param name="Started">The timestamp of the refusal on the clock of the group.</param>
    /// <param name="Length">How long the wait lasts.</param>
    /// <param name="Reason">The refusal policy whose consecutive failures lengthen the wait.</param>
    [Immutable]
    [StructLayout(LayoutKind.Auto)]
    private readonly record struct Cooldown(long Started, TimeSpan Length, string Reason);
}
