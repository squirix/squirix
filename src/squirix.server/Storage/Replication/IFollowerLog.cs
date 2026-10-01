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
    /// <param name="index">The index to compact through; it must be both the commit index and the applied index of the log.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The compaction outcome; only <see cref="GroupCompactionOutcome.Compacted" /> drops entries from the log.</returns>
    /// <remarks>
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

    /// <summary>Advances the committed index for a leader request, refusing stale terms.</summary>
    /// <param name="commitIndex">The target committed index.</param>
    /// <param name="leaderTerm">Leader term authorizing the advance; stale terms are refused.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The outcome of the commit advance.</returns>
    Task<FollowerLogCommitResult> AdvanceCommitAsync(ulong commitIndex, ulong leaderTerm, CancellationToken cancellationToken);

    /// <summary>Appends an ordered batch of entries following the consistency checks of the replication protocol.</summary>
    /// <param name="request">The appending request.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The outcome of the appending attempt.</returns>
    Task<FollowerLogAppendResult> AppendAsync(FollowerLogAppendRequest request, CancellationToken cancellationToken);

    /// <summary>
    /// Returns the committed entries in the exclusive <c language="csharp">LastAppliedIndex</c> to inclusive <c language="csharp">CommitIndex</c>
    /// range, rather than the full committed prefix, because applied payloads are released from memory.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The committed entries not yet applied.</returns>
    ValueTask<IReadOnlyList<FollowerLogEntry>> GetCommittedEntriesAsync(CancellationToken cancellationToken);

    /// <summary>Reads back from disk the newest committed entries the log retains, applied or not, one at a time.</summary>
    /// <param name="maxCount">The largest number of entries to read.</param>
    /// <param name="visit">Called with each entry, newest first; the entry is dropped once it returns.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of entries read.</returns>
    /// <exception cref="InvalidOperationException">The log is disposed or not ready.</exception>
    /// <exception cref="System.IO.InvalidDataException">A retained committed frame is torn or corrupt.</exception>
    Task<int> ReadRecentCommittedAsync(int maxCount, Action<FollowerLogEntry> visit, CancellationToken cancellationToken);

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

    /// <summary>Installs a validated snapshot through atomic storage publication.</summary>
    /// <param name="snapshot">Snapshot to install.</param>
    /// <param name="leaderTerm">Leader term authorizing the installation; stale terms are refused.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The installation outcome.</returns>
    Task<GroupSnapshotInstallResult> InstallSnapshotAsync(GroupSnapshot snapshot, ulong leaderTerm, CancellationToken cancellationToken);
}
