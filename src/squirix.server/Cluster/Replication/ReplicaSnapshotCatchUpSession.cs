using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Installs a snapshot, replays its retained tail, and performs final participation verification.</summary>
internal sealed class ReplicaSnapshotCatchUpSession
{
    private readonly ReplicaEligibility _eligibility;
    private readonly IFollowerLog _follower;
    private readonly ReplicaRepairPlanner _planner;
    private readonly int _replicaIndex;

    /// <summary>Initializes a new instance of the <see cref="ReplicaSnapshotCatchUpSession" /> class.</summary>
    /// <param name="planner">Bounded repair planner.</param>
    /// <param name="follower">Target durable follower log.</param>
    /// <param name="eligibility">Participation gate for the replica group.</param>
    /// <param name="replicaIndex">Target replica slot.</param>
    internal ReplicaSnapshotCatchUpSession(ReplicaRepairPlanner planner, IFollowerLog follower, ReplicaEligibility eligibility, int replicaIndex)
    {
        ArgumentNullException.ThrowIfNull(planner);
        ArgumentNullException.ThrowIfNull(follower);
        ArgumentNullException.ThrowIfNull(eligibility);
        _planner = planner;
        _follower = follower;
        _eligibility = eligibility;
        _replicaIndex = replicaIndex;
    }

    /// <summary>Runs snapshot installation, ordered replay, and exact final verification.</summary>
    /// <param name="request">Leader-authoritative catch-up inputs.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true" /> only when the participant becomes ready.</returns>
    internal async Task<bool> RunAsync(ReplicaSnapshotCatchUpRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        var transfer = request.Snapshot;
        var expected = request.Expected;
        if (!transfer.IsValidFor(_follower.GroupId) || !transfer.TopologyFingerprint.Span.SequenceEqual(expected.TopologyFingerprint.Span) ||
            transfer.ConfigurationGeneration != expected.ConfigurationGeneration || transfer.CommitIndex > expected.CommitIndex)
        {
            _eligibility.Quarantine(_replicaIndex);
            return false;
        }

        // A malformed readiness target can never match, so reject it before any storage I/O.
        // Unlike a topology mismatch it proves nothing about this follower, hence no quarantine.
        if (!expected.IsValid)
            return false;

        var status = await _follower.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        var initial = Progress(in status, transfer.TopologyFingerprint, transfer.ConfigurationGeneration, 0U);
        if (!_eligibility.TryMarkCatchingUp(_replicaIndex, in initial))
            return false;

        var installed = await _follower.InstallSnapshotAsync(transfer.Snapshot, cancellationToken).ConfigureAwait(false);
        if (!installed.Success)
        {
            if (string.Equals(installed.RefusalCode, FollowerLogRefusal.TopologyMismatch, StringComparison.Ordinal))
                _eligibility.Quarantine(_replicaIndex);
            return false;
        }

        status = await _follower.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        var afterSnapshot = Progress(in status, transfer.TopologyFingerprint, transfer.ConfigurationGeneration, 0U);
        if (!_eligibility.TryMarkCatchingUp(_replicaIndex, in afterSnapshot))
            return false;

        if (!await ReplayTailAsync(request, transfer, cancellationToken).ConfigureAwait(false))
            return false;

        var checksum = await request.FinalizeStateAsync(cancellationToken).ConfigureAwait(false);
        var applied = await _follower.AdvanceAppliedAsync(expected.AppliedIndex, cancellationToken).ConfigureAwait(false);
        if (!applied.Success)
            return false;

        status = await _follower.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        var observed = Progress(in status, transfer.TopologyFingerprint, transfer.ConfigurationGeneration, checksum);
        return _eligibility.TryMarkReady(_replicaIndex, in observed, in expected);
    }

    private static ReplicaProgress Progress(in FollowerLogStatus status, ReadOnlyMemory<byte> fingerprint, ulong generation, uint checksum) => new(
        status.LastLogIndex + 1UL,
        status.LastLogIndex,
        status.CommitIndex,
        status.LastAppliedIndex,
        status.LastLogTerm,
        fingerprint,
        generation,
        checksum);

    private async Task<bool> ReplayTailAsync(ReplicaSnapshotCatchUpRequest request, ReplicaSnapshotTransfer transfer, CancellationToken cancellationToken)
    {
        var nextIndex = transfer.LastIncludedIndex + 1UL;
        var baseline = new SnapshotBaseline(transfer.LastIncludedIndex, transfer.LastIncludedTerm);
        while (nextIndex <= request.Expected.MatchIndex)
        {
            ReplicaRepairBatch batch;
            try
            {
                batch = _planner.SelectBatch(request.TailEntries, nextIndex, baseline);
            }
            catch (InvalidOperationException)
            {
                // The leader compacted or revised the tail mid-session: the retained entries no longer cover
                // the probe index. Fail the session without quarantine so a later session can retry from a
                // fresh snapshot boundary.
                return false;
            }

            if (batch.Entries.IsEmpty)
                return false;

            var append = new FollowerLogAppendRequest(
                request.LeaderNodeId,
                request.LeaderTerm,
                batch.PrevLogIndex,
                batch.PrevLogTerm,
                request.Expected.CommitIndex,
                batch.Entries);
            var result = await _follower.AppendAsync(append, cancellationToken).ConfigureAwait(false);
            if (!result.Success)
                return false;

            nextIndex = batch.Entries.Span[^1].LogIndex + 1UL;
            var status = await _follower.GetStatusAsync(cancellationToken).ConfigureAwait(false);
            var progress = Progress(in status, transfer.TopologyFingerprint, transfer.ConfigurationGeneration, 0U);
            if (!_eligibility.TryMarkCatchingUp(_replicaIndex, in progress))
                return false;
        }

        return nextIndex == request.Expected.MatchIndex + 1UL;
    }
}
