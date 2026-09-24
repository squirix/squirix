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
/// </remarks>
internal static class ReplicaReadinessProbe
{
    /// <summary>Applies a probe verdict to one follower slot.</summary>
    /// <param name="eligibility">Participation gates of the owned group.</param>
    /// <param name="replicaIndex">Zero-based follower slot.</param>
    /// <param name="result">Probe outcome.</param>
    /// <param name="leader">Leader log status the probe was built from.</param>
    /// <param name="fingerprint">Static topology fingerprint.</param>
    /// <param name="generation">Static configuration generation.</param>
    /// <param name="coordinator">Running coordinator whose quorum must be re-based before the slot may count, or <see langword="null" /> before it exists.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="result" /> carries an unsupported verdict.</exception>
    internal static void Apply(
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
                // Behind, ahead, or diverged: never ready. The follower's own last index is advisory only.
                var hint = new ReplicaProgress(result.LastLogIndex + 1, result.LastLogIndex, 0, 0, 0, fingerprint, generation, 0);
                _ = eligibility.TryMarkCatchingUp(replicaIndex, in hint);
                return;
            case ReplicaProbeKind.Unreachable:
            case ReplicaProbeKind.Refused:
                return;
            default:
                throw new ArgumentOutOfRangeException(nameof(result), result.Kind, "Unsupported probe outcome.");
        }
    }

    /// <summary>Applies the probe verdict of every follower slot.</summary>
    /// <param name="eligibility">Participation gates of the owned group.</param>
    /// <param name="results">Per-slot probe outcomes; slots that were not probed carry the default unreachable verdict.</param>
    /// <param name="leader">Leader log status the probes were built from.</param>
    /// <param name="fingerprint">Static topology fingerprint.</param>
    /// <param name="generation">Static configuration generation.</param>
    /// <param name="coordinator">Running coordinator to re-base, or <see langword="null" /> before it exists.</param>
    internal static void ApplyAll(
        ReplicaEligibility eligibility,
        ReplicaProbeResult[] results,
        in FollowerLogStatus leader,
        ReadOnlyMemory<byte> fingerprint,
        ulong generation,
        ReplicaCommitCoordinator? coordinator)
    {
        ArgumentNullException.ThrowIfNull(results);
        for (var i = 1; i < results.Length; i++)
            Apply(eligibility, i, in results[i], in leader, fingerprint, generation, coordinator);
    }

    /// <summary>Marks the leader's own slot ready from its durable log tail.</summary>
    /// <param name="eligibility">Participation gates of the owned group.</param>
    /// <param name="leader">Leader log status; the tail must be fully committed.</param>
    /// <param name="fingerprint">Static topology fingerprint.</param>
    /// <param name="generation">Static configuration generation.</param>
    internal static void MarkLeaderReady(ReplicaEligibility eligibility, in FollowerLogStatus leader, ReadOnlyMemory<byte> fingerprint, ulong generation)
    {
        ArgumentNullException.ThrowIfNull(eligibility);
        if (leader.Readiness != FollowerLogReadiness.Ready || leader.LastLogIndex != leader.CommitIndex)
            return;

        var progress = TailProgress(in leader, fingerprint, generation);
        _ = eligibility.TryMarkReady(0, in progress, in progress);
    }

    /// <summary>Selects the follower slots that still need verification.</summary>
    /// <param name="eligibility">Participation gates of the owned group.</param>
    /// <returns>A per-slot flag array; slot zero (the leader) is never selected.</returns>
    internal static bool[] NonReadyFollowers(ReplicaEligibility eligibility)
    {
        ArgumentNullException.ThrowIfNull(eligibility);
        var candidates = new bool[eligibility.ReplicaCount];
        for (var i = 1; i < candidates.Length; i++)
            candidates[i] = !eligibility.CanCountInWriteQuorum(i);

        return candidates;
    }

    /// <summary>Probes the selected follower slots in parallel.</summary>
    /// <param name="gateway">Follower replication RPCs.</param>
    /// <param name="candidates">Per-slot flags selecting the slots to probe.</param>
    /// <param name="members">Ordered group members; index zero is the leader.</param>
    /// <param name="header">Replication envelope identity.</param>
    /// <param name="leader">Leader log status naming the entry the followers must hold.</param>
    /// <param name="timeout">Per-probe budget.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Per-slot outcomes; unselected slots carry the default unreachable verdict.</returns>
    internal static async Task<ReplicaProbeResult[]> ProbeAllAsync(
        IReplicaRpcGateway gateway,
        bool[] candidates,
        string[] members,
        ReplicaRpcHeader header,
        FollowerLogStatus leader,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(candidates);
        ArgumentNullException.ThrowIfNull(members);
        var results = new ReplicaProbeResult[candidates.Length];
        var slots = new List<int>(candidates.Length);
        var probes = new List<Task<ReplicaProbeResult>>(candidates.Length);
        for (var i = 1; i < candidates.Length; i++)
        {
            if (!candidates[i])
                continue;

            slots.Add(i);
            probes.Add(ProbeAsync(gateway, members[i], header, leader, timeout, cancellationToken));
        }

        var probed = await Task.WhenAll(probes).ConfigureAwait(false);
        for (var k = 0; k < slots.Count; k++)
            results[slots[k]] = probed[k];

        return results;
    }

    /// <summary>Sends one empty Log Matching append to a follower.</summary>
    /// <param name="gateway">Follower replication RPCs.</param>
    /// <param name="nodeId">Target follower node identifier.</param>
    /// <param name="header">Replication envelope identity.</param>
    /// <param name="leader">Leader log status naming the entry the follower must hold.</param>
    /// <param name="timeout">Per-probe budget.</param>
    /// <param name="cancellationToken">Cancellation token; its cancellation propagates.</param>
    /// <returns>The probe outcome; transport failures and timeouts are reported as <see cref="ReplicaProbeKind.Unreachable" />.</returns>
    internal static async Task<ReplicaProbeResult> ProbeAsync(
        IReplicaRpcGateway gateway,
        string nodeId,
        ReplicaRpcHeader header,
        FollowerLogStatus leader,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(gateway);
        var batch = new FollowerBatch([], header.LeaderNodeId, header.Term, leader.LastLogIndex, leader.LastLogTerm, leader.CommitIndex);
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
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

    private static ReplicaProbeResult Classify(in FollowerLogAppendResult response) =>
        (response.Success, string.Equals(response.RefusalCode, RefusalCodes.LogMismatch, StringComparison.Ordinal)) switch
        {
            (true, _) => new ReplicaProbeResult(ReplicaProbeKind.Accepted, response.LastLogIndex),
            (false, true) => new ReplicaProbeResult(ReplicaProbeKind.LogMismatch, response.LastLogIndex),
            _ => new ReplicaProbeResult(ReplicaProbeKind.Refused, 0),
        };

    private static ReplicaProgress TailProgress(in FollowerLogStatus leader, ReadOnlyMemory<byte> fingerprint, ulong generation) =>
        new(leader.LastLogIndex + 1, leader.LastLogIndex, leader.CommitIndex, 0, leader.LastLogTerm, fingerprint, generation, 0);
}
