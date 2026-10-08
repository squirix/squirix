using System;
using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Follower-side replication RPC logic over this node's replica group logs.</summary>
/// <remarks>
/// The transport adapter maps wire messages to these domain calls. Every call first resolves the group
/// and checks topology agreement (fingerprint and generation mirror the snapshot-install rules: an empty
/// durable fingerprint adopts nothing here but never conflicts, and an older generation is refused);
/// term validation stays inside the log, which persists higher terms durably before responding. An accepted append, commit advance, or
/// snapshot install wakes the group's apply loop, so committed entries reach memory without waiting for its fallback interval; votes
/// and pre-votes apply nothing and wake nothing.
/// </remarks>
[Immutable]
internal sealed class ReplicaFollower
{
    private readonly ReplicaGroupRegistry _groups;

    /// <summary>Initializes a new instance of the <see cref="ReplicaFollower" /> class.</summary>
    /// <param name="groups">Replica group registry of this node.</param>
    internal ReplicaFollower(ReplicaGroupRegistry groups)
    {
        ArgumentNullException.ThrowIfNull(groups);
        _groups = groups;
    }

    /// <summary>Advances a group commit index after agreement checks.</summary>
    /// <param name="id">Replica group identifier.</param>
    /// <param name="fp">Leader topology fp.</param>
    /// <param name="gen">Leader configuration gen.</param>
    /// <param name="commit">Target commit index.</param>
    /// <param name="leader">Leader term authorizing the advance; stale terms are refused.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The commit advance outcome.</returns>
    internal async Task<FollowerLogCommitResult> AdvanceCommitAsync(string id, ReadOnlyMemory<byte> fp, ulong gen, ulong commit, ulong leader, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        if (!TryGetLog(id, out var log))
            return new FollowerLogCommitResult(false, FollowerLogRefusal.NotMember, 0);

        var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        if (status.IsTopologyMismatch(fp, gen))
            return new FollowerLogCommitResult(false, FollowerLogRefusal.TopologyMismatch, status.CommitIndex);

        var result = await log.AdvanceCommitAsync(commit, leader, cancellationToken).ConfigureAwait(false);
        NotifyApplyWhen(result.Success, id);
        return result;
    }

    /// <summary>Appends leader entries to a group log after agreement checks.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="fingerprint">Leader topology fingerprint.</param>
    /// <param name="generation">Leader configuration generation.</param>
    /// <param name="batch">Leader batch with identity, consistency, and commit positions.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The append outcome.</returns>
    internal async Task<FollowerLogAppendResult> AppendAsync(
        string groupId,
        ReadOnlyMemory<byte> fingerprint,
        ulong generation,
        FollowerBatch batch,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        if (!TryGetLog(groupId, out var log))
            return new FollowerLogAppendResult(false, FollowerLogRefusal.NotMember, 0, 0);

        var records = batch.Records;
        var entries = new FollowerLogEntry[records.Count];
        for (var i = 0; i < records.Count; i++)
        {
            var record = records[i];
            entries[i] = new FollowerLogEntry(record.LogIndex, record.Term, ReplicaLogCodec.Encode(in record));
        }

        var request = new FollowerLogAppendRequest(
            batch.LeaderNodeId,
            batch.LeaderTerm,
            batch.PrevLogIndex,
            batch.PrevLogTerm,
            batch.LeaderCommitIndex,
            new ReadOnlyMemory<FollowerLogEntry>(entries));
        var result = await log.AppendAsync(request, fingerprint, generation, cancellationToken).ConfigureAwait(false);
        NotifyApplyWhen(result.Success, groupId);
        return result;
    }

    /// <summary>Gets a group log status.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The log status, or <see langword="null" /> when this node does not serve the group.</returns>
    internal async ValueTask<FollowerLogStatus?> GetStatusAsync(string groupId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        return !TryGetLog(groupId, out var log) ? null : await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Installs a leader snapshot into a group log after agreement checks.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="fingerprint">Leader topology fingerprint.</param>
    /// <param name="generation">Leader configuration generation.</param>
    /// <param name="snapshot">Snapshot to install.</param>
    /// <param name="leaderTerm">Leader term authorizing the install.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The installation outcome.</returns>
    internal async Task<GroupSnapshotInstallResult> InstallSnapshotAsync(
        string groupId,
        ReadOnlyMemory<byte> fingerprint,
        ulong generation,
        GroupSnapshot snapshot,
        ulong leaderTerm,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        if (!TryGetLog(groupId, out var log))
            return GroupSnapshotInstallResult.Refused(FollowerLogRefusal.NotMember);

        var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        return status.IsTopologyMismatch(fingerprint, generation) ? GroupSnapshotInstallResult.Refused(FollowerLogRefusal.TopologyMismatch)
            : await InstallAsync(groupId, log, snapshot, leaderTerm, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Installs a transferred snapshot file into a group log after agreement checks.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="fingerprint">Leader topology fingerprint.</param>
    /// <param name="generation">Leader configuration generation.</param>
    /// <param name="upload">Reassembled snapshot file with declared framing.</param>
    /// <param name="leaderTerm">Leader term authorizing the installation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The installation outcome.</returns>
    internal async Task<GroupSnapshotInstallResult> InstallSnapshotUploadAsync(
        string groupId,
        ReadOnlyMemory<byte> fingerprint,
        ulong generation,
        ReplicaSnapshotUpload upload,
        ulong leaderTerm,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        if (!TryGetLog(groupId, out var log))
            return GroupSnapshotInstallResult.Refused(FollowerLogRefusal.NotMember);

        var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        if (status.IsTopologyMismatch(fingerprint, generation))
            return GroupSnapshotInstallResult.Refused(FollowerLogRefusal.TopologyMismatch);

        // Malformed transfers are refused without touching storage, mirroring the malformed-snapshot
        // refusals of the install path.
        if (!GroupSnapshotStore.TryDecodePublished(upload.FileBytes.Span, GroupSnapshotStore.DefaultMaxSnapshotBytes, out var snapshot))
            return GroupSnapshotInstallResult.Refused(FollowerLogRefusal.NotReady);

        if (snapshot.LastIncludedIndex != upload.DeclaredLastIncludedIndex || snapshot.LastIncludedTerm != upload.DeclaredLastIncludedTerm)
            return GroupSnapshotInstallResult.Refused(FollowerLogRefusal.NotReady);

        var storedChecksum = BinaryPrimitives.ReadUInt32LittleEndian(upload.FileBytes.Span[^4..]);
        return storedChecksum != upload.DeclaredChecksum ? GroupSnapshotInstallResult.Refused(FollowerLogRefusal.NotReady)
            : await InstallAsync(groupId, log, snapshot, leaderTerm, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Evaluates a pre-vote probe against a group log after agreement checks, without changing the log.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="fingerprint">Candidate topology fingerprint.</param>
    /// <param name="generation">Candidate configuration generation.</param>
    /// <param name="ballot">The probe; its term is the term the candidate proposes to start.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The probe outcome; a refusal before the log is reached reports term zero.</returns>
    internal async Task<FollowerLogVoteResult> PreVoteAsync(
        string groupId,
        ReadOnlyMemory<byte> fingerprint,
        ulong generation,
        ElectionVoteRequest ballot,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        if (!TryGetLog(groupId, out var log))
            return new FollowerLogVoteResult(false, FollowerLogRefusal.NotMember, 0UL);

        var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        return status.IsTopologyMismatch(fingerprint, generation) ? new FollowerLogVoteResult(false, FollowerLogRefusal.TopologyMismatch, 0UL)
            : await log.CheckPreVoteAsync(ballot, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Evaluates a vote request against a group log after agreement checks.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="fingerprint">Candidate topology fingerprint.</param>
    /// <param name="generation">Candidate configuration generation.</param>
    /// <param name="ballot">The vote request; its term is the candidate durable term.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The vote outcome; a refusal before the log is reached reports term zero and changes nothing.</returns>
    /// <remarks>The log persists a higher term and a granted vote before this call reports them.</remarks>
    internal async Task<FollowerLogVoteResult> RequestVoteAsync(
        string groupId,
        ReadOnlyMemory<byte> fingerprint,
        ulong generation,
        ElectionVoteRequest ballot,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        if (!TryGetLog(groupId, out var log))
            return new FollowerLogVoteResult(false, FollowerLogRefusal.NotMember, 0UL);

        var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        return status.IsTopologyMismatch(fingerprint, generation) ? new FollowerLogVoteResult(false, FollowerLogRefusal.TopologyMismatch, 0UL)
            : await log.RequestVoteAsync(ballot, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Installs a validated snapshot into a group log and wakes the group's apply loop when it was installed.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="log">The group log.</param>
    /// <param name="snapshot">Snapshot to install.</param>
    /// <param name="leaderTerm">Leader term authorizing the install.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The installation outcome.</returns>
    private async Task<GroupSnapshotInstallResult> InstallAsync(string groupId, IFollowerLog log, GroupSnapshot snapshot, ulong leaderTerm, CancellationToken cancellationToken)
    {
        var result = await log.InstallSnapshotAsync(snapshot, leaderTerm, cancellationToken).ConfigureAwait(false);
        NotifyApplyWhen(result.Success, groupId);
        return result;
    }

    /// <summary>Wakes the apply loop of a group after a change that may let it apply more committed entries.</summary>
    /// <param name="changed">Whether the log accepted the change.</param>
    /// <param name="groupId">Replica group identifier.</param>
    private void NotifyApplyWhen(bool changed, string groupId)
    {
        if (changed)
            _groups.ApplySignalFor(groupId).Notify();
    }

    private bool TryGetLog(string groupId, [NotNullWhen(true)] out IFollowerLog? log) => _groups.TryGetLog(groupId, out log);
}
