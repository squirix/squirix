using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Leader-side Log Matching verification that lets a restarted replica slot rejoin the write quorum.</summary>
/// <remarks>
/// The probe is an empty append naming the leader's last log entry as its predecessor. A follower accepts it only
/// when it holds an entry at that index with the same term, and by the Log Matching Property its whole prefix is
/// then identical to the leader's. The probe never truncates or appends, and only advances the follower commit
/// index to <c language="csharp">min(leaderCommit, prevIndex)</c>. Applied index and state checksum are not
/// carried on the wire and never advance in production, so both sides of the readiness comparison fix them at zero.
/// A follower whose probe mismatched is marked catching up here and repaired by an entry catch-up session through its sender.
/// </remarks>
internal static class ReplicaReadinessProbe
{
    /// <summary>Records every follower that answered a probe from its log as in contact with an elected leadership, like a heartbeat reply.</summary>
    /// <param name="election">The election state of the group, or <see langword="null" /> for a group led statically.</param>
    /// <param name="results">Per-slot probe outcomes.</param>
    /// <param name="term">The led term the probes carried; a follower answers from its log only at or below it.</param>
    /// <remarks>The contact is recorded when the probes end; a probe given up before it answered records none.</remarks>
    internal static void RecordContacts(ReplicaGroupState? election, ReplicaProbeResult[] results, ulong term)
    {
        ArgumentNullException.ThrowIfNull(results);
        if (election == null)
            return;

        for (var i = 0; i < results.Length; i++)
        {
            if (results[i].Kind is ReplicaProbeKind.Accepted or ReplicaProbeKind.LogMismatch)
                election.RecordFollowerContact(i, term);
        }
    }

    /// <summary>Applies the probe verdict of every follower slot.</summary>
    /// <param name="eligibility">Participation gates of the owned group.</param>
    /// <param name="leaderReplicaIndex">Zero-based slot of the leader, which no probe verdict touches.</param>
    /// <param name="results">Per-slot probe outcomes; slots that were not probed carry the default unreachable verdict.</param>
    /// <param name="leader">Leader log status the probes were built from.</param>
    /// <param name="fingerprint">Static topology fingerprint.</param>
    /// <param name="generation">Static configuration generation.</param>
    /// <param name="coordinator">Running coordinator to re-base, or <see langword="null" /> before it exists.</param>
    internal static void ApplyAll(
        ReplicaEligibility eligibility,
        int leaderReplicaIndex,
        ReplicaProbeResult[] results,
        in FollowerLogStatus leader,
        ReadOnlyMemory<byte> fingerprint,
        ulong generation,
        ReplicaCommitCoordinator? coordinator)
    {
        ArgumentNullException.ThrowIfNull(results);
        var slots = new ReplicaSlots(leaderReplicaIndex);
        for (var i = 0; i < results.Length; i++)
        {
            if (slots.IsFollower(i))
                Apply(eligibility, i, in results[i], in leader, fingerprint, generation, coordinator);
        }
    }

    /// <summary>Admits a catching-up follower slot that a catch-up session verified to hold the leader log exactly through an index.</summary>
    /// <param name="eligibility">Participation gates of the owned group.</param>
    /// <param name="replicaIndex">Zero-based follower slot.</param>
    /// <param name="result">The session result; its held index and term are the verified prefix.</param>
    /// <param name="leaderCommit">The leader commit index read under the commit gate.</param>
    /// <param name="fingerprint">Static topology fingerprint.</param>
    /// <param name="generation">Static configuration generation.</param>
    /// <param name="coordinator">Running coordinator whose quorum is raised to the held index before the slot may count.</param>
    /// <remarks>Runs under the commit gate. A slot that left the catching-up state meanwhile, verified by a probe or quarantined, is left alone.</remarks>
    internal static void AdmitCaughtUp(
        ReplicaEligibility eligibility,
        int replicaIndex,
        in ReplicaCatchUpResult result,
        ulong leaderCommit,
        ReadOnlyMemory<byte> fingerprint,
        ulong generation,
        ReplicaCommitCoordinator coordinator)
    {
        ArgumentNullException.ThrowIfNull(eligibility);
        ArgumentNullException.ThrowIfNull(coordinator);
        if (result.Outcome != ReplicaCatchUpOutcome.CaughtUp || eligibility.StateFor(replicaIndex) != ReplicaParticipantState.CatchingUp)
            return;

        var held = result.HeldThrough;
        var progress = new ReplicaProgress(held + 1, held, Math.Min(leaderCommit, held), 0, result.HeldTerm, fingerprint, generation, 0);

        // Order matters: the quorum match index must be raised before the slot can count.
        coordinator.AdmitReplica(replicaIndex, held);
        _ = eligibility.TryMarkReady(replicaIndex, in progress, in progress);
    }

    /// <summary>Marks the leader's own slot ready from its durable log tail.</summary>
    /// <param name="eligibility">Participation gates of the owned group.</param>
    /// <param name="leaderReplicaIndex">Zero-based slot of the leader.</param>
    /// <param name="leader">Leader log status; an uncommitted tail is part of the leader's durable log and counts toward its slot.</param>
    /// <param name="fingerprint">Static topology fingerprint.</param>
    /// <param name="generation">Static configuration generation.</param>
    internal static void MarkLeaderReady(ReplicaEligibility eligibility, int leaderReplicaIndex, in FollowerLogStatus leader, ReadOnlyMemory<byte> fingerprint, ulong generation)
    {
        ArgumentNullException.ThrowIfNull(eligibility);
        if (leader.Readiness != FollowerLogReadiness.Ready)
            return;

        var progress = TailProgress(in leader, fingerprint, generation);
        _ = eligibility.TryMarkReady(leaderReplicaIndex, in progress, in progress);
    }

    /// <summary>Takes every ready follower slot out of the write quorum, so it counts again only after a probe or a catch-up session verifies it.</summary>
    /// <param name="eligibility">Participation gates of the owned group.</param>
    /// <param name="leaderReplicaIndex">Zero-based slot of the leader, which stays as it is.</param>
    /// <remarks>
    /// A new coordinator starts every slot at the commit index. A follower can stay ready behind it: a slower follower is not demoted,
    /// and a failure of its append that the retired coordinator never observed (a call that hangs past its cancellation) is lost.
    /// </remarks>
    internal static void UnverifyFollowers(ReplicaEligibility eligibility, int leaderReplicaIndex)
    {
        ArgumentNullException.ThrowIfNull(eligibility);

        // No verified progress reaches the maximum index, so every ready follower slot is demoted.
        var slots = new ReplicaSlots(leaderReplicaIndex);
        for (var i = 0; i < eligibility.ReplicaCount; i++)
        {
            if (slots.IsFollower(i))
                _ = eligibility.TryDemote(i, ulong.MaxValue);
        }
    }

    /// <summary>Selects the follower slots that still need verification.</summary>
    /// <param name="eligibility">Participation gates of the owned group.</param>
    /// <param name="leaderReplicaIndex">Zero-based slot of the leader, which is never selected.</param>
    /// <returns>A per-slot flag array.</returns>
    internal static bool[] NonReadyFollowers(ReplicaEligibility eligibility, int leaderReplicaIndex)
    {
        ArgumentNullException.ThrowIfNull(eligibility);
        var candidates = new bool[eligibility.ReplicaCount];
        var slots = new ReplicaSlots(leaderReplicaIndex);
        for (var i = 0; i < candidates.Length; i++)
            candidates[i] = slots.IsFollower(i) && !eligibility.CanCountInWriteQuorum(i);

        return candidates;
    }

    /// <summary>Probes the selected follower slots in parallel.</summary>
    /// <param name="gateway">Follower replication RPCs.</param>
    /// <param name="candidates">Per-slot flags selecting the slots to probe; the leader slot is never selected.</param>
    /// <param name="members">Ordered group members, in slot order.</param>
    /// <param name="header">Replication envelope identity.</param>
    /// <param name="leader">Leader log status naming the entry the followers must hold.</param>
    /// <param name="budget">Per-probe timeout and the answers after which the probes still pending are given up.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Per-slot outcomes; unselected and given-up slots carry the default unreachable verdict.</returns>
    /// <remarks>No probe outlives the call: the ones given up are canceled and awaited.</remarks>
    internal static async Task<ReplicaProbeResult[]> ProbeAllAsync(
        IReplicaRpcGateway gateway,
        bool[] candidates,
        string[] members,
        ReplicaRpcHeader header,
        FollowerLogStatus leader,
        ReplicaProbeBudget budget,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(candidates);
        ArgumentNullException.ThrowIfNull(members);
        var results = new ReplicaProbeResult[candidates.Length];
        var slots = new List<int>(candidates.Length);
        var probes = new List<Task<ReplicaProbeResult>>(candidates.Length);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        for (var i = 0; i < candidates.Length; i++)
        {
            if (!candidates[i])
                continue;

            slots.Add(i);
            probes.Add(ProbeAsync(gateway, members[i], in header, in leader, budget.Timeout, cancellationToken, linked.Token));
        }

        var pending = new List<Task<ReplicaProbeResult>>(probes);
        var answers = 0;
        while (pending.Count > 0 && answers < budget.AnswersNeeded)
        {
            var finished = await Task.WhenAny(pending).ConfigureAwait(false);
            _ = pending.Remove(finished);
            if (finished.IsFaulted)
                break;

            if (finished.IsCompletedSuccessfully && await finished.ConfigureAwait(false) is { Kind: ReplicaProbeKind.Accepted or ReplicaProbeKind.LogMismatch })
                answers++;
        }

        // Probes given up are canceled and awaited, so none outlives the call; a faulted one rethrows here.
        await linked.CancelAsync().ConfigureAwait(false);
        var probed = await Task.WhenAll(probes).ConfigureAwait(false);
        for (var k = 0; k < slots.Count; k++)
            results[slots[k]] = probed[k];

        return results;
    }

    /// <summary>Applies a probe verdict to one follower slot.</summary>
    /// <param name="eligibility">Participation gates of the owned group.</param>
    /// <param name="replicaIndex">Zero-based follower slot.</param>
    /// <param name="result">Probe outcome.</param>
    /// <param name="leader">Leader log status the probe was built from.</param>
    /// <param name="fingerprint">Static topology fingerprint.</param>
    /// <param name="generation">Static configuration generation.</param>
    /// <param name="coordinator">Running coordinator whose quorum must be re-based before the slot may count, or <see langword="null" /> before it exists.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="result" /> carries an unsupported verdict.</exception>
    private static void Apply(
        ReplicaEligibility eligibility,
        int replicaIndex,
        in ReplicaProbeResult result,
        in FollowerLogStatus leader,
        ReadOnlyMemory<byte> fingerprint,
        ulong generation,
        ReplicaCommitCoordinator? coordinator)
    {
        ArgumentNullException.ThrowIfNull(eligibility);
        switch (result.Kind)
        {
            case ReplicaProbeKind.Accepted when result.LastLogIndex == leader.LastLogIndex:
                var expected = TailProgress(in leader, fingerprint, generation);

                // Order matters: the quorum match index must be raised before the slot can count.
                coordinator?.AdmitReplica(replicaIndex, leader.LastLogIndex);
                _ = eligibility.TryMarkReady(replicaIndex, in expected, in expected);
                return;
            case ReplicaProbeKind.Accepted:
            case ReplicaProbeKind.LogMismatch:
                // Behind, ahead, or diverged: never ready. Only a prefix the accepted probe verified is recorded as
                // progress; a reported last index that may hold divergent entries must not become a monotonic floor.
                var verified = result.Kind == ReplicaProbeKind.Accepted ? leader.LastLogIndex : 0UL;
                var hint = new ReplicaProgress(verified + 1, verified, 0, 0, 0, fingerprint, generation, 0);
                _ = eligibility.TryMarkCatchingUp(replicaIndex, in hint);
                return;
            case ReplicaProbeKind.Unreachable:
            case ReplicaProbeKind.Refused:
                return;
            default:
                throw new ArgumentOutOfRangeException(nameof(result), result.Kind, "Unsupported probe outcome.");
        }
    }

    private static ReplicaProbeResult Classify(in FollowerLogAppendResult response) =>
        (response.Success, string.Equals(response.RefusalCode, RefusalCodes.LogMismatch, StringComparison.Ordinal)) switch
        {
            (true, _) => new ReplicaProbeResult(ReplicaProbeKind.Accepted, response.LastLogIndex),
            (false, true) => new ReplicaProbeResult(ReplicaProbeKind.LogMismatch, response.LastLogIndex),
            _ => new ReplicaProbeResult(ReplicaProbeKind.Refused, 0),
        };

    /// <summary>Sends one empty Log Matching append to a follower.</summary>
    /// <param name="gateway">Follower replication RPCs.</param>
    /// <param name="nodeId">Target follower node identifier.</param>
    /// <param name="header">Replication envelope identity.</param>
    /// <param name="leader">Leader log status naming the entry the follower must hold.</param>
    /// <param name="timeout">Per-probe budget.</param>
    /// <param name="cancellationToken">Cancellation token; its cancellation propagates.</param>
    /// <param name="giveUp">Token linked to <paramref name="cancellationToken" /> that also ends the probe when its outcome is no longer needed; the probe is then reported as unreachable.</param>
    /// <returns>The probe outcome; transport failures, timeouts and given-up probes are reported as <see cref="ReplicaProbeKind.Unreachable" />.</returns>
    private static Task<ReplicaProbeResult> ProbeAsync(
        IReplicaRpcGateway gateway,
        string nodeId,
        in ReplicaRpcHeader header,
        in FollowerLogStatus leader,
        TimeSpan timeout,
        CancellationToken cancellationToken,
        CancellationToken giveUp)
    {
        ArgumentNullException.ThrowIfNull(gateway);
        var batch = new FollowerBatch([], header.LeaderNodeId, header.Term, leader.LastLogIndex, leader.LastLogTerm, leader.CommitIndex);
        return SendAsync(gateway, nodeId, header, batch, timeout, cancellationToken, giveUp);
    }

    private static async Task<ReplicaProbeResult> SendAsync(
        IReplicaRpcGateway gateway,
        string nodeId,
        ReplicaRpcHeader header,
        FollowerBatch batch,
        TimeSpan timeout,
        CancellationToken cancellationToken,
        CancellationToken giveUp)
    {
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(giveUp);
        bounded.CancelAfter(timeout);
        try
        {
            var response = await gateway.AppendEntriesAsync(nodeId, header, batch, bounded.Token).ConfigureAwait(false);
            return Classify(in response);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return new ReplicaProbeResult(ReplicaProbeKind.Unreachable, 0);
        }
        catch (Exception exception) when (exception is RpcException or IOException or HttpRequestException or TimeoutException or InvalidOperationException)
        {
            // A dead or slow follower is not a fault of the leader: the slot simply stays out of the quorum.
            return new ReplicaProbeResult(ReplicaProbeKind.Unreachable, 0);
        }
    }

    private static ReplicaProgress TailProgress(in FollowerLogStatus leader, ReadOnlyMemory<byte> fingerprint, ulong generation) =>
        new(leader.LastLogIndex + 1, leader.LastLogIndex, leader.CommitIndex, 0, leader.LastLogTerm, fingerprint, generation, 0);
}
