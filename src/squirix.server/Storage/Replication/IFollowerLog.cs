using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Replication;

/// <summary>Durable, ordered follower log for one replica group.</summary>
/// <remarks>
/// Only committed entries that are not yet applied are exposed through the storage contract. Uncommitted
/// entries are retained on disk but are never applied to memory and never surfaced to callers. Advancing the
/// applied index releases applied entry payloads from memory.
/// </remarks>
internal interface IFollowerLog : IAsyncDisposable
{
    /// <summary>Gets the replica group identifier.</summary>
    /// <returns>The replica group identifier.</returns>
    string GroupId { get; }

    /// <summary>Gets the current durability readiness state.</summary>
    /// <returns>The durability readiness state.</returns>
    FollowerLogReadiness Readiness { get; }

    /// <summary>Gets a snapshot of the durable log state.</summary>
    /// <returns>A snapshot of the durable log state.</returns>
    FollowerLogStatus GetStatus();

    /// <summary>Appends an ordered batch of entries following the consistency checks of the replication protocol.</summary>
    /// <param name="request">The append request.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The outcome of the append attempt.</returns>
    Task<FollowerLogAppendResult> AppendAsync(FollowerLogAppendRequest request, CancellationToken cancellationToken);

    /// <summary>Advances the committed index monotonically and never beyond the durable last index.</summary>
    /// <param name="commitIndex">The target committed index.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The outcome of the commit advance.</returns>
    Task<FollowerLogCommitResult> AdvanceCommitAsync(ulong commitIndex, CancellationToken cancellationToken);

    /// <summary>
    /// Advances the applied index monotonically, never beyond the committed index, and releases the applied
    /// entry payloads from memory. The byte offsets of applied entries are retained so a later divergence at or
    /// above the committed index can still be truncated durably.
    /// </summary>
    /// <param name="appliedIndex">The target applied index.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The outcome of the applied advance.</returns>
    Task<FollowerLogAppliedResult> AdvanceAppliedAsync(ulong appliedIndex, CancellationToken cancellationToken);

    /// <summary>Returns the committed prefix of the log.</summary>
    /// <returns>The committed entries of the log.</returns>
    IReadOnlyList<FollowerLogEntry> GetCommittedEntries();

    /// <summary>Returns the uncommitted tail of the log, used to rebuild pending operations after a restart.</summary>
    /// <returns>The uncommitted tail entries of the log.</returns>
    IReadOnlyList<FollowerLogEntry> GetUncommittedTail();
}
