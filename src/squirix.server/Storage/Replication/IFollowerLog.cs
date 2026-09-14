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

    /// <summary>
    /// Advances the applied index monotonically, never beyond the committed index, and releases the applied
    /// entry payloads from memory. The byte offsets of applied entries are retained, so a later divergence at or
    /// above the committed index can still be truncated durably.
    /// </summary>
    /// <param name="appliedIndex">The target applied index.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The outcome of the applied advance.</returns>
    Task<FollowerLogAppliedResult> AdvanceAppliedAsync(ulong appliedIndex, CancellationToken cancellationToken);

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

    /// <summary>Gets a snapshot of the durable log state.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A snapshot of the durable log state.</returns>
    ValueTask<FollowerLogStatus> GetStatusAsync(CancellationToken cancellationToken);

    /// <summary>Installs a validated snapshot through atomic storage publication.</summary>
    /// <param name="snapshot">Snapshot to install.</param>
    /// <param name="leaderTerm">Leader term authorizing the installation; stale terms are refused.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The installation outcome.</returns>
    Task<GroupSnapshotInstallResult> InstallSnapshotAsync(GroupSnapshot snapshot, ulong leaderTerm, CancellationToken cancellationToken);
}
