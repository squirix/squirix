using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Replication;

/// <summary>Durable, ordered follower log for one replica group.</summary>
internal interface IFollowerLog : IAsyncDisposable
{
    /// <summary>Gets the replica group identifier.</summary>
    /// <returns>The replica group identifier.</returns>
    string GroupId { get; }

    /// <summary>Gets the idempotency state of the replica group, which durable truncation releases pins from.</summary>
    /// <returns>The group idempotency state.</returns>
    GroupIdempotencyState Idempotency { get; }

    /// <summary>Gets a value indicating whether the copy is known to be complete, which is read without the log gate.</summary>
    /// <returns>
    /// <see langword="true" /> while the copy is complete; a state transfer clears it and its completion sets it again. <see langword="false" /> proves nothing: the copy may have just
    /// become complete, and <see langword="true" /> may have just ended. A caller that acts on it relies on the log, which checks the member state again under its gate.
    /// </returns>
    bool IsComplete { get; }

    /// <summary>Gets the readiness of the log, which is read without the log gate.</summary>
    /// <returns>
    /// <see cref="FollowerLogReadiness.Failed" /> once the log latched a failure, which only a restart clears; the value may have just changed, and a
    /// caller that acts on it relies on the log, which checks its readiness again under its gate.
    /// </returns>
    FollowerLogReadiness Readiness { get; }

    /// <summary>
    /// Advances the applied index monotonically, never beyond the committed index, and releases the applied
    /// entry payloads from memory. The byte offsets of applied entries are retained, so a later divergence at or
    /// above the committed index can still be truncated durably.
    /// </summary>
    /// <param name="appliedIndex">The target applied index.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The outcome of the applied advance.</returns>
    Task<FollowerLogAppliedResult> AdvanceAppliedAsync(ulong appliedIndex, CancellationToken cancellationToken);

    /// <summary>
    /// Publishes a snapshot covering the log through <paramref name="index" /> and drops the covered entries from the log, as one
    /// step under the log gate.
    /// </summary>
    /// <param name="index">The index to compact through; it must be the durable applied index of the log, at or below its commit index.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The compaction outcome; only <see cref="GroupCompactionOutcome.Compacted" /> drops entries from the log.</returns>
    /// <remarks>
    /// Every entry above <paramref name="index" />, committed or not, stays in the log; a retained tail that misses an index is refused.
    /// The refusals leave readiness untouched. A failure after the snapshot is published leaves a log that recovers to the same state,
    /// with or without the covered prefix.
    /// </remarks>
    Task<GroupCompactionOutcome> CompactThroughAsync(ulong index, CancellationToken cancellationToken);

    /// <summary>Advances the committed index monotonically and never beyond the durable last index.</summary>
    /// <param name="commitIndex">The target committed index.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The outcome of the commit advance.</returns>
    /// <remarks>Trusted local path: the owner advances its own log without a term gate.</remarks>
    Task<FollowerLogCommitResult> AdvanceCommitAsync(ulong commitIndex, CancellationToken cancellationToken);

    /// <summary>Appends an ordered batch of entries following the consistency checks of the replication protocol.</summary>
    /// <param name="request">The appending request.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The outcome of the appending attempt.</returns>
    Task<FollowerLogAppendResult> AppendAsync(FollowerLogAppendRequest request, CancellationToken cancellationToken);

    /// <summary>
    /// Appends an ordered batch of entries after checking the leader topology, with the check and the append under one log gate
    /// acquisition.
    /// </summary>
    /// <param name="request">The appending request.</param>
    /// <param name="topologyFingerprint">Leader topology fingerprint.</param>
    /// <param name="configurationGeneration">Leader configuration generation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The outcome of the appending attempt; a topology disagreement is refused before anything else.</returns>
    /// <remarks>
    /// Taking the gate once keeps the arrival order of concurrent appends: a separate status read followed by an append would let two
    /// queued appends swap between the two acquisitions.
    /// </remarks>
    Task<FollowerLogAppendResult> AppendAsync(
        FollowerLogAppendRequest request,
        ReadOnlyMemory<byte> topologyFingerprint,
        ulong configurationGeneration,
        CancellationToken cancellationToken);

    /// <summary>
    /// Returns the committed entries in the exclusive <c language="csharp">LastAppliedIndex</c> to inclusive <c language="csharp">CommitIndex</c>
    /// range, rather than the full committed prefix, because applied payloads are released from memory.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The committed entries not yet applied.</returns>
    ValueTask<IReadOnlyList<FollowerLogEntry>> GetCommittedEntriesAsync(CancellationToken cancellationToken);

    /// <summary>Returns up to <paramref name="maxCount" /> committed entries that follow <paramref name="afterIndex" /> densely, in log order.</summary>
    /// <param name="afterIndex">The exclusive index to read after.</param>
    /// <param name="maxCount">The largest number of entries to return.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The retained entries from <c language="csharp">afterIndex + 1</c> up to the commit index, <paramref name="maxCount" />, or the first index that is not retained; empty when none is.</returns>
    ValueTask<IReadOnlyList<FollowerLogEntry>> GetCommittedEntriesAsync(ulong afterIndex, int maxCount, CancellationToken cancellationToken);

    /// <summary>Reads back from disk the newest committed entries the log retains, applied or not, one at a time.</summary>
    /// <param name="maxCount">The largest number of entries to read.</param>
    /// <param name="visit">Called with each entry, newest first; the entry is dropped once it returns, and returning <see langword="false" /> stops the read.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of entries read.</returns>
    /// <exception cref="InvalidOperationException">The log is disposed or not ready.</exception>
    /// <exception cref="System.IO.InvalidDataException">A retained committed frame is torn or corrupt.</exception>
    Task<int> ReadRecentCommittedAsync(int maxCount, Func<FollowerLogEntry, bool> visit, CancellationToken cancellationToken);

    /// <summary>
    /// Reads back from disk the retained entries from <paramref name="fromIndex" /> on, at most <paramref name="maxCount" />, in index order,
    /// with the position that precedes them and the log bounds observed in the same read.
    /// </summary>
    /// <param name="fromIndex">The one-based index of the first entry to read.</param>
    /// <param name="maxCount">The largest number of entries to read.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The entries read, or a result that is not retained when the position before <paramref name="fromIndex" /> cannot be verified.</returns>
    /// <exception cref="InvalidOperationException">The log is disposed or not ready.</exception>
    /// <exception cref="System.IO.InvalidDataException">A retained frame is torn, corrupt, or missing inside the retained range.</exception>
    /// <remarks>
    /// Reads under the log gate, so no frame moves meanwhile; applied payloads released from memory are read back from their retained
    /// frames.
    /// </remarks>
    Task<FollowerLogEntriesRead> ReadEntriesAsync(ulong fromIndex, int maxCount, CancellationToken cancellationToken);

    /// <summary>Reads how much of the log is retained on disk and in memory.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The retained log bytes, entries, payloads, and the snapshot index.</returns>
    ValueTask<FollowerLogRetention> GetRetentionAsync(CancellationToken cancellationToken);

    /// <summary>Reads the durable log status, its uncommitted tail, and the term at its commit index as one consistent view.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The status with exactly the durable entries above its commit index.</returns>
    /// <exception cref="System.IO.InvalidDataException">The log retains no term for its commit index.</exception>
    /// <remarks>
    /// A leader reads its tail through this call instead of a status followed by separate tail reads: a commit that advances between
    /// separate reads would pair a status with a shorter tail than it reports.
    /// </remarks>
    ValueTask<FollowerLogTail> GetLeaderTailAsync(CancellationToken cancellationToken);

    /// <summary>Gets a snapshot of the durable log state.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A snapshot of the durable log state.</returns>
    ValueTask<FollowerLogStatus> GetStatusAsync(CancellationToken cancellationToken);

    /// <summary>Returns the term of the entry at a log index, or of the installed snapshot baseline at that index.</summary>
    /// <param name="logIndex">The log index; zero is the log origin.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The term at <paramref name="logIndex" />; zero at the log origin.</returns>
    /// <exception cref="System.IO.InvalidDataException">The log retains no term for a non-zero <paramref name="logIndex" />.</exception>
    ValueTask<ulong> GetTermAtAsync(ulong logIndex, CancellationToken cancellationToken);

    /// <summary>Returns the durable entries above the committed index, in index order.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The uncommitted tail entries.</returns>
    ValueTask<IReadOnlyList<FollowerLogEntry>> GetUncommittedTailAsync(CancellationToken cancellationToken);

    /// <summary>Marks the copy incomplete at the start of a state transfer, durably in both metadata slots.</summary>
    /// <param name="leader">
    /// The leader that sends the state. A term below the durable term is refused, a higher one is adopted with the vote cleared. An unseeded copy
    /// is refused unless the leader checked the term ceiling.
    /// </param>
    /// <param name="index">The applied index the transferred state is taken at; an index below the durable commit index is refused.</param>
    /// <param name="fingerprint">Leader topology fingerprint.</param>
    /// <param name="generation">Leader configuration generation.</param>
    /// <param name="snapshot">The snapshot the transfer will complete at; it is checked now, so a snapshot the completion would refuse leaves the copy untouched.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// <see langword="null" /> once the incomplete record is durable in both metadata slots; otherwise the refusal: <see cref="FollowerLogRefusal.NotReady" />
    /// when the log is disposed or not ready, the index is below the durable commit index, or the snapshot is malformed or above the size limit of the log;
    /// <see cref="FollowerLogRefusal.TopologyMismatch" />; <see cref="FollowerLogRefusal.NotMember" /> for a snapshot of another group;
    /// <see cref="FollowerLogRefusal.StaleTerm" />; <see cref="FollowerLogRefusal.Unseeded" /> for an unseeded copy and a leader that did not check the
    /// term ceiling.
    /// </returns>
    /// <remarks>
    /// Nothing but the member state, and the term and the vote when the leader term is higher, changes: the log, the commit index and the applied
    /// index stay. A copy that is incomplete already accepts again and publishes the record again, which also repairs a half-written earlier record.
    /// A crash between the two slot writes leaves a legal state either way: complete as before, or incomplete. An unseeded copy accepted for a leader
    /// that checked the term ceiling writes nothing and stays unseeded until the completion.
    /// </remarks>
    Task<string?> BeginStateInstallAsync(StateInstallLeader leader, ulong index, ReadOnlyMemory<byte> fingerprint, ulong generation, GroupSnapshot snapshot, CancellationToken cancellationToken);

    /// <summary>Captures the group snapshot at the durable applied index without publishing it or compacting the log.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The snapshot at the applied index, or the reason it was not captured.</returns>
    Task<GroupSnapshotCapture> CaptureSnapshotAsync(CancellationToken cancellationToken);

    /// <summary>Completes a state transfer: publishes the snapshot, replaces the log by its baseline and marks the copy complete at the snapshot index.</summary>
    /// <param name="snapshot">The group snapshot at the index the state was transferred at.</param>
    /// <param name="leader">
    /// The leader that sent the state. A term below the durable term is refused, a higher one is adopted with the vote cleared. An unseeded copy
    /// completes only for a leader that checked the term ceiling, and takes the leader's term with a vote for the leader.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// The outcome; a complete copy, an unseeded copy and a leader that did not check the term ceiling, a snapshot below the durable commit index and a
    /// malformed snapshot are refused before anything is written.
    /// </returns>
    /// <remarks>
    /// Each step is durable before the next: the snapshot, the log that holds nothing above the snapshot baseline, then the complete record in both
    /// metadata slots with the commit and the applied index at the snapshot index. A crash before the complete record is read leaves the copy incomplete,
    /// or unseeded.
    /// </remarks>
    Task<GroupSnapshotInstallResult> CompleteStateInstallAsync(GroupSnapshot snapshot, StateInstallLeader leader, CancellationToken cancellationToken);

    /// <summary>Evaluates a pre-vote probe against the durable log without changing it.</summary>
    /// <param name="request">The probe; its term is the term the candidate proposes to start.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Whether a vote would be granted, with the durable term, which the probe never changes.</returns>
    /// <remarks>A term at or below one, the static provisional leader term, is refused as stale.</remarks>
    Task<FollowerLogVoteResult> CheckPreVoteAsync(ElectionVoteRequest request, CancellationToken cancellationToken);

    /// <summary>Evaluates a vote request, persisting a higher term and a granted vote before reporting them.</summary>
    /// <param name="request">The vote request; its term is the candidate durable term.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Whether the vote was granted, with the durable term after the request.</returns>
    /// <remarks>
    /// At most one candidate is granted per term, across restarts; a replay for the recorded candidate is granted again. A term at or
    /// below one, the static provisional leader term, is refused as stale and changes nothing.
    /// </remarks>
    Task<FollowerLogVoteResult> RequestVoteAsync(ElectionVoteRequest request, CancellationToken cancellationToken);

    /// <summary>Adopts a higher term seen in a reply, persisting it with the vote cleared before it is reported.</summary>
    /// <param name="term">The observed term.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The durable term after the call: <paramref name="term" /> once adopted, otherwise the unchanged durable term.</returns>
    /// <remarks>
    /// The term never goes down and the log entries are never touched: a term at or below the durable one, or a log that is not ready,
    /// changes nothing. A failed metadata write fails the log, like every other durable term step.
    /// </remarks>
    Task<ulong> ObserveTermAsync(ulong term, CancellationToken cancellationToken);

    /// <summary>Records that the owner of the group offered this unseeded copy a place in the first start of the group.</summary>
    /// <param name="topologyFingerprint">Owner topology fingerprint.</param>
    /// <param name="configurationGeneration">Owner configuration generation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// <see langword="null" /> once the offer is durable in both metadata slots, which an unseeded copy accepts again and publishes again;
    /// <see cref="FollowerLogRefusal.NotReady" /> when the log is disposed, not ready, or not unseeded;
    /// <see cref="FollowerLogRefusal.TopologyMismatch" /> when the topology disagrees with the log.
    /// </returns>
    /// <remarks>The record is published to both metadata slots; a failed write fails readiness like every other metadata write.</remarks>
    Task<string?> AcceptBootstrapOfferAsync(ReadOnlyMemory<byte> topologyFingerprint, ulong configurationGeneration, CancellationToken cancellationToken);

    /// <summary>Completes the copy when it holds an offer, which a call from a member of the group proves was acted on.</summary>
    /// <param name="topologyFingerprint">Caller topology fingerprint.</param>
    /// <param name="configurationGeneration">Caller configuration generation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// <see langword="null" /> when the copy is complete, now or already; <see cref="FollowerLogRefusal.Unseeded" /> for an unseeded copy without an offer;
    /// <see cref="FollowerLogRefusal.Incomplete" /> for an incomplete one; <see cref="FollowerLogRefusal.TopologyMismatch" /> when the topology disagrees with the log;
    /// <see cref="FollowerLogRefusal.NotReady" /> when the log is disposed or not ready.
    /// </returns>
    /// <remarks>
    /// Only the call of a member of the group may reach this: only complete members lead or campaign, so such a call proves the group was
    /// started, and the offer proves this copy acknowledged and voted for nothing since. The record is published to both metadata slots. A
    /// crash between the two slot writes leaves a legal state either way: after the first copy of the complete record a rollback to the
    /// offered record completes again on the next member call, and nothing was acknowledged in between, because the call is processed only
    /// after both copies are durable.
    /// </remarks>
    Task<string?> CompleteIfOfferedAsync(ReadOnlyMemory<byte> topologyFingerprint, ulong configurationGeneration, CancellationToken cancellationToken);

    /// <summary>Completes the unseeded copy of the owner of the group, the step of its first start of the group.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes once the complete record is durable in both metadata slots.</returns>
    /// <exception cref="InvalidOperationException">The log is disposed, not ready, or not unseeded.</exception>
    /// <remarks>
    /// A crash between the two slot writes leaves a legal state either way: unseeded as before, or complete. Nothing was acknowledged before
    /// both copies are durable, because the owner starts the group only after this call returns.
    /// </remarks>
    Task CompleteBootstrapAsync(CancellationToken cancellationToken);
}
