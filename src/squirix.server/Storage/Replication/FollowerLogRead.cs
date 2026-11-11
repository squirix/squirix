using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Replication;

/// <summary>Reads retained entries and metadata from a follower log.</summary>
/// <remarks>The caller holds the log gate, so the frame offsets stay valid for the whole read.</remarks>
internal static class FollowerLogRead
{
    internal static FollowerLogStatus CaptureStatus(FollowerLogJournal journal, IFollowerLogState owner)
    {
        var meta = owner.Meta;
        var lastLogIndex = owner.LastLogIndex;
        _ = TryGetRetainedTerm(journal, lastLogIndex, out var lastLogTerm);
        return new FollowerLogStatus(
            owner.GroupId,
            meta.TopologyFingerprint,
            meta.ConfigurationGeneration,
            meta.CurrentTerm,
            meta.VotedFor,
            lastLogIndex,
            lastLogTerm,
            meta.CommitIndex,
            meta.LastAppliedIndex,
            owner.Readiness,
            meta.MemberState)
        {
            CommitHint = owner.CommitHint,
        };
    }

    internal static FollowerLogRetention GetRetention(FollowerLogJournal journal, IFollowerLogContext owner)
    {
        var snapshotIndex = journal.SnapshotBaseline.LastIncludedIndex;
        var logLength = owner.LogLength;
        if (snapshotIndex == 0UL)
            return new FollowerLogRetention(logLength, journal.EntryOffsets.Count, journal.Entries.Count, snapshotIndex);

        var suffixOffset = journal.TryGetEntryOffset(snapshotIndex + 1UL, out var first) ? first.Offset : logLength;
        var lastLogIndex = owner.LastLogIndex;
        var suffixEntries = int.CreateChecked(lastLogIndex >= snapshotIndex ? lastLogIndex - snapshotIndex : 0UL);
        return new FollowerLogRetention(logLength, suffixEntries, journal.Entries.Count, snapshotIndex)
        {
            CompactionBytes = GroupLogSlots.HeaderSize + logLength - suffixOffset,
        };
    }

    /// <summary>Returns the term the log retains for an index.</summary>
    /// <param name="journal">The paired in-memory journal state.</param>
    /// <param name="owner">The owning log.</param>
    /// <param name="logIndex">The log index; zero is the log origin.</param>
    /// <returns>The term of the retained entry or snapshot baseline at <paramref name="logIndex" />; zero at the log origin.</returns>
    /// <exception cref="InvalidDataException">The log retains no term for a non-zero <paramref name="logIndex" />.</exception>
    internal static ulong GetTerm(FollowerLogJournal journal, IFollowerLogState owner, ulong logIndex)
    {
        var term = 0UL;
        return logIndex == 0UL || TryGetRetainedTerm(journal, logIndex, out term)
            ? term
            : throw new InvalidDataException($"Replica group '{owner.GroupId}' retains no term for log index '{logIndex}'.");
    }

    internal static async Task<FollowerLogEntriesRead> ReadRangeAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        ulong fromIndex,
        int maxCount,
        long maxPayloadBytes,
        CancellationToken cancellationToken)
    {
        var prev = fromIndex - 1UL;
        var lastLogIndex = owner.LastLogIndex;
        var commitIndex = owner.Meta.CommitIndex;

        // The position before the range must be verifiable by the follower: the origin, a retained frame, or the snapshot baseline.
        // Anything else is either compacted away or beyond the last index, and the caller backs up or fails closed.
        if (!TryGetRetainedTerm(journal, prev, out var prevTerm))
            return new FollowerLogEntriesRead(false, prev, 0UL, lastLogIndex, commitIndex, []);

        if (maxCount == 0 || fromIndex > lastLogIndex)
            return new FollowerLogEntriesRead(true, prev, prevTerm, lastLogIndex, commitIndex, []);

        var count = int.CreateTruncating(Math.Min(lastLogIndex - fromIndex + 1UL, ulong.CreateChecked(maxCount)));
        var frames = new (ulong LogIndex, long Offset)[count];
        var frameOverhead = GroupLogCodec.ComputeFrameEncodedLength(0);
        var selected = 0;
        long? nextOffset = null;
        for (var i = 0; i < count; i++)
        {
            var logIndex = fromIndex + ulong.CreateChecked(i);
            var offset = nextOffset ?? GetOffset(journal, owner, logIndex);
            if (maxPayloadBytes != long.MaxValue)
            {
                var end = logIndex == lastLogIndex ? owner.LogLength : GetOffset(journal, owner, logIndex + 1);
                var payloadLength = end - offset - frameOverhead;
                if (i > 0 && payloadLength > maxPayloadBytes)
                    break;

                maxPayloadBytes -= payloadLength;
                nextOffset = end;
            }

            frames[selected++] = (logIndex, offset);
        }

        var entries = await GroupLogFrameReader.ReadAllAsync(journal.LogPath, new ArraySegment<(ulong LogIndex, long Offset)>(frames, 0, selected), cancellationToken).ConfigureAwait(false);
        return new FollowerLogEntriesRead(true, prev, prevTerm, lastLogIndex, commitIndex, entries);
    }

    private static long GetOffset(FollowerLogJournal journal, IFollowerLogContext owner, ulong logIndex) =>
        journal.TryGetEntryOffset(logIndex, out var location)
            ? location.Offset
            : throw new InvalidDataException($"Replica group '{owner.GroupId}' retains no frame for log index '{logIndex}' inside its retained range.");

    /// <summary>Resolves the term the log retains for <paramref name="logIndex" /> without throwing for an unretained index.</summary>
    /// <param name="journal">The paired in-memory journal state.</param>
    /// <param name="logIndex">The log index; zero is the log origin.</param>
    /// <param name="term">The retained term, or zero when the index is the origin or not retained.</param>
    /// <returns><see langword="true" /> when the index is the origin of a log never compacted, a retained frame, or the snapshot baseline.</returns>
    private static bool TryGetRetainedTerm(FollowerLogJournal journal, ulong logIndex, out ulong term)
    {
        // Once a snapshot baseline exists the origin is compacted away: the first retained frame follows the baseline, not the origin.
        if (logIndex == 0UL)
        {
            term = 0UL;
            return journal.SnapshotBaseline.LastIncludedIndex == 0UL;
        }

        if (journal.TryGetEntryOffset(logIndex, out var location))
        {
            term = location.Term;
            return true;
        }

        if (journal.SnapshotBaseline.LastIncludedIndex == logIndex)
        {
            term = journal.SnapshotBaseline.LastIncludedTerm;
            return true;
        }

        term = 0UL;
        return false;
    }
}
