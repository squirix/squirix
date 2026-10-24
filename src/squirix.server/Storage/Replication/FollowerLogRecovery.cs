using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;

namespace Squirix.Server.Storage.Replication;

/// <summary>Startup recovery: rebuilds the in-memory log from the durable frame file.</summary>
internal static class FollowerLogRecovery
{
    /// <summary>Empties the generation file the active one superseded, which a rewrite or a crash may have left holding a full copy of the log.</summary>
    /// <param name="journal">The storage surface.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes once the superseded file is empty.</returns>
    internal static async Task EmptySupersededLogAsync(FollowerLogJournal journal, CancellationToken cancellationToken)
    {
        var path = journal.Paths.GetLogSlotPath(1 - journal.LogSlot);
        if (!File.Exists(path) || new FileInfo(path).Length == 0)
            return;

        await WorkPool.RunAsync(new RecoveryTruncateWork(path, 0), TaskCreationOptions.None, cancellationToken).ConfigureAwait(false);
    }

    internal static async Task RecoverLogFileAsync(FollowerLogJournal journal, IFollowerLogContext owner, CancellationToken cancellationToken)
    {
        // A published snapshot restores the committed baseline; the durable log then continues from its included
        // index plus one when the covered prefix was compacted away.
        var snapshotBase = await FollowerLogSnapshotRecovery.RestoreSnapshotBaseAsync(journal, owner, cancellationToken).ConfigureAwait(false);

        // Generation 0 means the group has never persisted a log, so nothing exists beyond the snapshot baseline.
        if (journal.LogGeneration == 0UL)
        {
            await SnapshotBoundaryWatermarks.ResetAndEnsureAsync(journal, owner, snapshotBase, cancellationToken).ConfigureAwait(false);
            return;
        }

        var handle = File.OpenHandle(
            journal.LogPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        using (handle)
            await RecoverOpenedFileAsync(handle, journal, owner, snapshotBase, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Derives the index the frame walk resumes from, given the first durable frame and the snapshot base.</summary>
    /// <remarks>
    ///     <para>
    ///     A populated log starts at index one (full history), right after the compacted snapshot base, or —
    ///     when a newer snapshot was published after the last compaction — inside the snapshot-covered prefix.
    ///     In the third shape the discarded head is backed by the published snapshot, so the walk resumes from
    ///     the last journaled index below the first frame.
    ///     </para>
    ///     <para>
    ///     Trusting the snapshot base against a full log would misread its first frame as a committed gap, so
    ///     the starting index is derived from the first frame instead.
    ///     </para>
    /// </remarks>
    /// <param name="firstIndex">The log index of the first durable frame, or zero when no valid frame opens the file.</param>
    /// <param name="snapshotBase">The restored snapshot baseline index.</param>
    /// <returns>The last journaled index below the first frame the walk continues from.</returns>
    private static ulong DeriveWalkBaseIndex(ulong firstIndex, ulong snapshotBase)
    {
        return true switch
        {
            _ when firstIndex == 1 || snapshotBase == 0 => 0UL,
            _ when firstIndex <= snapshotBase => firstIndex - 1UL,
            _ when firstIndex == snapshotBase + 1 => snapshotBase,
            _ => 0UL,
        };
    }

    private static async Task FinishRecoveredWalkAsync(
        SafeFileHandle handle,
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        ulong snapshotBase,
        ulong walkBase,
        FollowerLogWalkResult result,
        CancellationToken cancellationToken)
    {
        // A crash between snapshot publication and journal truncation leaves the old journal in place; a
        // surviving frame at the restored snapshot's included index carrying a different term means the
        // suffix above it belongs to a divergent history and is discarded, restarting from the snapshot.
        if (result.BoundaryDivergent)
        {
            await TruncateAndResetAsync(journal, owner, GroupLogSlots.HeaderSize, snapshotBase, cancellationToken).ConfigureAwait(false);
            return;
        }

        // A torn trailing frame is truncated back to the last valid boundary on disk,
        // since a CRC mismatch alone cannot tell an appended tail from corruption.
        if (result.Truncated && result.LastValidEnd < RandomAccess.GetLength(handle))
            await ScheduleTruncateAsync(journal, result.LastValidEnd, cancellationToken).ConfigureAwait(false);

        await SnapshotBoundaryWatermarks.CompleteWalkAsync(journal, owner, snapshotBase, (walkBase, result.LastValidEnd), cancellationToken).ConfigureAwait(false);
    }

    private static async Task RecoverOpenedFileAsync(
        SafeFileHandle handle,
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        ulong snapshotBase,
        CancellationToken cancellationToken)
    {
        if (await TryResetShortFileAsync(handle, journal, owner, snapshotBase, cancellationToken).ConfigureAwait(false))
            return;

        journal.ClearEntries();

        // A populated log starts at index one (full history), right after the compacted snapshot
        // base, or — when a newer snapshot was published after the last compaction — inside the
        // snapshot-covered prefix. In the third shape the discarded head is backed by the published
        // snapshot, so the walk resumes from the last journaled index below the first frame. Trusting
        // the snapshot base against a full log would misread its first frame as a committed gap, so
        // the starting index is derived from the first frame.
        var firstIndex = await FollowerLogFrameWalk.PeekFirstFrameIndexAsync(handle, cancellationToken).ConfigureAwait(false);
        if (await TryResetTornFirstFrameAsync(journal, owner, firstIndex, snapshotBase, cancellationToken).ConfigureAwait(false))
            return;

        var walkBase = DeriveWalkBaseIndex(firstIndex, snapshotBase);
        owner.LastLogIndex = walkBase;

        var result = await FollowerLogFrameWalk.WalkFramesAsync(journal, owner, handle, cancellationToken).ConfigureAwait(false);
        await FinishRecoveredWalkAsync(handle, journal, owner, snapshotBase, walkBase, result, cancellationToken).ConfigureAwait(false);
    }

    private static Task ScheduleTruncateAsync(FollowerLogJournal journal, long truncateLength, CancellationToken cancellationToken)
    {
        var work = new RecoveryTruncateWork(journal.LogPath, truncateLength);
        return WorkPool.RunAsync(work, TaskCreationOptions.None, cancellationToken);
    }

    private static async Task TruncateAndResetAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        long truncateLength,
        ulong snapshotBase,
        CancellationToken cancellationToken)
    {
        var work = new RecoveryTruncateWork(journal.LogPath, truncateLength);
        await WorkPool.RunAsync(work, TaskCreationOptions.None, cancellationToken).ConfigureAwait(false);
        journal.ClearEntries();

        // The truncation dropped every frame at or below the snapshot base, so nothing can apply them any more: the applied watermark
        // moves to the base, as a snapshot installation moves it. Keeping it below would point the applier at entries no frame holds,
        // and its catch-up would refuse the gap forever. The commit index already reaches the base, which the snapshot reconciliation
        // raised it to, so the watermark never passes the commit index.
        await SnapshotBoundaryWatermarks.ResetAndEnsureAsync(journal, owner, snapshotBase, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<bool> TryResetShortFileAsync(
        SafeFileHandle handle,
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        ulong snapshotBase,
        CancellationToken cancellationToken)
    {
        // A header-only file carries no frames, so only the snapshot baseline (if any) is durable.
        if (RandomAccess.GetLength(handle) != GroupLogSlots.HeaderSize)
            return false;

        await SnapshotBoundaryWatermarks.ResetAndEnsureAsync(journal, owner, snapshotBase, cancellationToken).ConfigureAwait(false);
        return true;
    }

    private static async Task<bool> TryResetTornFirstFrameAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        ulong firstIndex,
        ulong snapshotBase,
        CancellationToken cancellationToken)
    {
        if (firstIndex != 0 || snapshotBase == 0)
            return false;

        // The first suffix frame after the validated snapshot base is torn; the entire suffix is
        // unrecoverable. Truncate the file to the header and restart from the snapshot baseline,
        // but only while the snapshot covers the whole committed prefix: destroying the file below
        // a higher durable commit watermark would turn a repairable shortfall into a permanent one.
        if (snapshotBase < owner.Meta.CommitIndex)
        {
            owner.Readiness = FollowerLogReadiness.Failed;
            throw new InvalidDataException($"Replica group '{owner.GroupId}' first journal frame above snapshot base '{snapshotBase}' is unreadable while the commit watermark '{owner.Meta.CommitIndex}' exceeds it.");
        }

        await TruncateAndResetAsync(journal, owner, GroupLogSlots.HeaderSize, snapshotBase, cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>Background work that durably truncates the log during startup recovery.</summary>
    [Immutable]
    private sealed class RecoveryTruncateWork : IWorkPoolItem
    {
        private readonly long _length;
        private readonly string _path;

        internal RecoveryTruncateWork(string path, long length)
        {
            _path = path;
            _length = length;
        }

        void IWorkPoolItem.Execute()
        {
            using var handle = File.OpenHandle(_path, FileMode.Open, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete);
            RandomAccess.SetLength(handle, _length);
            RandomAccess.FlushToDisk(handle);
        }
    }
}
