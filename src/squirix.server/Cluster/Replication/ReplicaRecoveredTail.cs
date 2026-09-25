using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Uncommitted leader log tail recovered at coordinator start, with the current-term commit rule that guards it.</summary>
/// <remarks>
/// Every entry was locally appended by an earlier process whose commit reported an unknown outcome or none at all, so a
/// majority may or may not hold it. The coordinator retains the entries and their idempotency pins until a recorded
/// majority covers them; an index is committable by counting replicas only once a current-term entry reaches it
/// (<see cref="ElectionCommitRule.HasCurrentTermEntryThrough" />).
/// </remarks>
[Immutable]
internal sealed class ReplicaRecoveredTail
{
    private readonly ulong _currentTerm;
    private readonly IReadOnlyList<FollowerLogEntry> _entries;
    private readonly IReplicaTailRebuilder _rebuilder;

    /// <summary>Initializes a new instance of the <see cref="ReplicaRecoveredTail" /> class.</summary>
    /// <param name="entries">The uncommitted leader log entries, contiguous and in index order.</param>
    /// <param name="currentTerm">The leader's current term.</param>
    /// <param name="rebuilder">Rebuilds the prepared form and the outcome of each entry.</param>
    /// <exception cref="ArgumentException"><paramref name="entries" /> is empty or not contiguous.</exception>
    /// <exception cref="System.IO.InvalidDataException">An entry payload is not a canonical replica log record.</exception>
    internal ReplicaRecoveredTail(IReadOnlyList<FollowerLogEntry> entries, ulong currentTerm, IReplicaTailRebuilder rebuilder)
    {
        ArgumentNullException.ThrowIfNull(entries);
        ArgumentNullException.ThrowIfNull(rebuilder);
        if (entries.Count == 0)
            throw new ArgumentException("A recovered tail holds at least one entry.", nameof(entries));

        var mutations = new PreparedReplicaMutation[entries.Count];
        for (var i = 0; i < entries.Count; i++)
        {
            if (i > 0 && entries[i].LogIndex != entries[i - 1].LogIndex + 1)
                throw new ArgumentException("Recovered tail entries must be contiguous.", nameof(entries));

            mutations[i] = rebuilder.Rebuild(entries[i]);
        }

        _entries = entries;
        _rebuilder = rebuilder;
        _currentTerm = currentTerm;
        Mutations = mutations;
    }

    /// <summary>Gets the first recovered log index.</summary>
    internal ulong FirstIndex => _entries[0].LogIndex;

    /// <summary>Gets the last recovered log index.</summary>
    internal ulong LastIndex => _entries[^1].LogIndex;

    /// <summary>Gets the prepared form of every recovered entry, in index order, with empty outcome payloads.</summary>
    internal IReadOnlyList<PreparedReplicaMutation> Mutations { get; }

    /// <summary>Determines whether a majority-backed index may be committed under the current-term rule.</summary>
    /// <param name="index">Commit candidate index.</param>
    /// <returns>
    /// <see langword="true" /> when a current-term entry reaches <paramref name="index" />; always <see langword="true" /> above the
    /// recovered tail, whose entries this leader appends in its current term.
    /// </returns>
    internal bool CanCommitThrough(ulong index) => index > LastIndex || ElectionCommitRule.HasCurrentTermEntryThrough(_entries, _currentTerm, index);

    /// <summary>Determines whether a log index belongs to the recovered tail.</summary>
    /// <param name="index">Log index.</param>
    /// <returns><see langword="true" /> when the index is recovered.</returns>
    internal bool Covers(ulong index) => index >= FirstIndex && index <= LastIndex;

    /// <summary>Reads the outcome of a recovered entry from live memory, right before its apply.</summary>
    /// <param name="entry">Recovered entry about to be applied.</param>
    /// <returns>The canonical outcome payload.</returns>
    internal ValueTask<ReadOnlyMemory<byte>> ReadOutcomeAsync(PreparedReplicaMutation entry) => _rebuilder.ReadOutcomeAsync(entry, CancellationToken.None);
}
