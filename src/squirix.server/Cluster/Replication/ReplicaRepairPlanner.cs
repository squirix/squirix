using System;
using System.Collections.Generic;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Selects bounded sequential repair batches and backs up follower progress after mismatches.</summary>
internal sealed class ReplicaRepairPlanner
{
    /// <summary>Initializes a new instance of the <see cref="ReplicaRepairPlanner" /> class.</summary>
    /// <param name="maxBatchEntries">Maximum entries in one append request.</param>
    internal ReplicaRepairPlanner(int maxBatchEntries)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxBatchEntries);

        MaxBatchEntries = maxBatchEntries;
    }

    /// <summary>Gets the maximum number of entries returned in a repair batch.</summary>
    internal int MaxBatchEntries { get; }

    /// <summary>Backs up the next index after a previous-entry mismatch without crossing the log origin.</summary>
    /// <param name="currentNextIndex">Current optimistic next index.</param>
    /// <param name="followerLastIndex">Last index reported by the follower.</param>
    /// <returns>The next earlier probe index.</returns>
    internal static ulong BackUpNextIndex(ulong currentNextIndex, ulong followerLastIndex)
    {
        if (currentNextIndex <= 1UL)
            return 1UL;

        var precedingProbe = currentNextIndex - 1UL;
        var followerProbe = followerLastIndex == ulong.MaxValue ? ulong.MaxValue : followerLastIndex + 1UL;
        return Math.Max(1UL, Math.Min(precedingProbe, followerProbe));
    }

    /// <summary>Selects a sequential batch beginning at <paramref name="nextIndex" />.</summary>
    /// <param name="leaderEntries">Leader entries in strictly consecutive index order.</param>
    /// <param name="nextIndex">Follower's verified next index.</param>
    /// <param name="baseline">Optional installed snapshot boundary supplying the predecessor term.</param>
    /// <returns>A bounded batch, or an empty batch when the follower is at the leader tail.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="leaderEntries" /> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="nextIndex" /> is zero.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the leader log is not consecutive or does not retain the required previous index.</exception>
    internal ReplicaRepairBatch SelectBatch(IReadOnlyList<FollowerLogEntry> leaderEntries, ulong nextIndex, SnapshotBaseline? baseline = null)
    {
        ArgumentNullException.ThrowIfNull(leaderEntries);
        ArgumentOutOfRangeException.ThrowIfEqual(nextIndex, 0UL);

        var start = leaderEntries.Count;
        for (var index = 0; index < leaderEntries.Count; index++)
        {
            if (leaderEntries[index].LogIndex == nextIndex)
            {
                start = index;
                break;
            }

            if (leaderEntries[index].LogIndex > nextIndex)
                throw new InvalidOperationException($"Leader log does not retain requested repair index '{nextIndex}'.");
        }

        if (start == leaderEntries.Count && leaderEntries.Count > 0 && nextIndex <= leaderEntries[^1].LogIndex)
            throw new InvalidOperationException($"Leader log does not retain requested repair index '{nextIndex}'.");
        if (start == leaderEntries.Count)
            return new ReplicaRepairBatch(nextIndex - 1UL, PreviousTerm(leaderEntries, start, nextIndex, baseline), ReadOnlyMemory<FollowerLogEntry>.Empty);

        var count = Math.Min(MaxBatchEntries, leaderEntries.Count - start);
        var selected = new FollowerLogEntry[count];
        for (var index = 0; index < count; index++)
        {
            var entry = leaderEntries[start + index];
            if (entry.LogIndex != nextIndex + Convert.ToUInt64(index))
                throw new InvalidOperationException("Leader repair entries must be strictly consecutive.");
            selected[index] = entry;
        }

        return new ReplicaRepairBatch(nextIndex - 1UL, PreviousTerm(leaderEntries, start, nextIndex, baseline), selected);
    }

    /// <summary>Selects retained entries, or a snapshot when the requested index is below the retained entry range.</summary>
    /// <param name="leaderEntries">Currently retained leader entries.</param>
    /// <param name="nextIndex">Follower's verified next index.</param>
    /// <param name="latestSnapshot">Latest fully published snapshot, when one exists.</param>
    /// <returns>The next repair payload.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="leaderEntries" /> is null.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the requested index is unavailable from both retention sources.</exception>
    internal ReplicaRepairSelection SelectRepair(IReadOnlyList<FollowerLogEntry> leaderEntries, ulong nextIndex, ReplicaSnapshotTransfer? latestSnapshot)
    {
        ArgumentNullException.ThrowIfNull(leaderEntries);
        if (leaderEntries.Count == 0 || leaderEntries[0].LogIndex > nextIndex)
        {
            if (latestSnapshot is { } snapshot && snapshot.LastIncludedIndex >= nextIndex)
                return new ReplicaRepairSelection(ReplicaRepairSelectionKind.Snapshot, default, snapshot);

            // A new replica group retains nothing yet: an empty leader log at the genesis index is a
            // valid empty repair, not a compaction gap.
            if (leaderEntries.Count == 0 && nextIndex == 1UL)
                return new ReplicaRepairSelection(ReplicaRepairSelectionKind.Entries, SelectBatch(leaderEntries, nextIndex), null);

            throw new InvalidOperationException($"Leader has compacted repair index '{nextIndex}' without an installable snapshot.");
        }

        SnapshotBaseline? baseline = null;
        if (latestSnapshot != null && latestSnapshot.LastIncludedIndex == nextIndex - 1UL)
            baseline = new SnapshotBaseline(latestSnapshot.LastIncludedIndex, latestSnapshot.LastIncludedTerm);
        return new ReplicaRepairSelection(ReplicaRepairSelectionKind.Entries, SelectBatch(leaderEntries, nextIndex, baseline), null);
    }

    private static ulong PreviousTerm(IReadOnlyList<FollowerLogEntry> entries, int start, ulong nextIndex, SnapshotBaseline? baseline)
    {
        if (nextIndex == 1UL)
            return 0UL;
        if (start > 0 && entries[start - 1].LogIndex == nextIndex - 1UL)
            return entries[start - 1].Term;
        if (baseline != null && baseline.LastIncludedIndex == nextIndex - 1UL)
            return baseline.LastIncludedTerm;
        throw new InvalidOperationException($"Leader log does not retain previous repair index '{nextIndex - 1UL}'.");
    }
}
