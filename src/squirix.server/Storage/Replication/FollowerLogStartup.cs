using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Replication;

/// <summary>Startup validation of the on-disk group state.</summary>
internal static class FollowerLogStartup
{
    /// <summary>Validates the on-disk state and recovers the committed prefix, leaving the log ready; the caller holds the gate.</summary>
    /// <param name="journal">The storage surface.</param>
    /// <param name="owner">The owning follower log.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the group log is open and ready.</returns>
    /// <exception cref="InvalidDataException">Metadata is corrupt or missing beside a log file; readiness is set to <see cref="FollowerLogReadiness.Failed" />.</exception>
    internal static async Task OpenGroupAsync(FollowerLogJournal journal, IFollowerLogContext owner, CancellationToken cancellationToken)
    {
        var metaExists = File.Exists(journal.Paths.MetadataPath);
        SelectLog(journal, owner);
        var logExists = File.Exists(journal.Paths.LogSlot0Path) || File.Exists(journal.Paths.LogSlot1Path);

        switch (metaExists)
        {
            case false when !logExists && !journal.Snapshot.SnapshotExists:
                // The fresh unseeded record is written to both slots, like a vote grant, so that damage to one slot does not make the file unreadable.
                var fresh = new GroupLogMetadata(owner.GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 0UL, string.Empty, 0UL, 0UL, GroupMemberState.Unseeded, false);
                await FollowerLogAppend.PersistMirroredMetaOrFailReadinessAsync(journal, owner, fresh, cancellationToken).ConfigureAwait(false);
                owner.Meta = fresh;
                owner.LastLogIndex = 0;
                CreateFirstLog(journal, owner);
                owner.Readiness = FollowerLogReadiness.Ready;
                return;

            case false when !logExists:
                // A published snapshot is a durable state even when metadata and the log are absent, but the vote memory of the group is
                // gone with the metadata: a restart could grant a second vote in a term it already voted in. Fail readiness instead.
                owner.Readiness = FollowerLogReadiness.Failed;
                throw new InvalidDataException($"Replica group '{owner.GroupId}' metadata is missing while the snapshot file exists; the group requires recovery or repair.");
            case true:
                // The store opens its handle only on a valid record, so a refused file is left untouched.
                var loaded = journal.Metadata.Open();
                if (loaded.Status != GroupMetadataReadStatus.Valid)
                {
                    owner.Readiness = FollowerLogReadiness.Failed;
                    throw new InvalidDataException(GroupMetadataStore.DescribeFailure(loaded.Status, owner.GroupId));
                }

                owner.Meta = loaded.Metadata;
                break;

            default:
                // The log file exists without its metadata file, so the committed boundary is unknown.
                // Assuming CommitIndex = 0 would treat every durable frame as an uncommitted tail and truncate it,
                // destroying possibly-committed data. Fail readiness instead; the group requires explicit repair.
                owner.Readiness = FollowerLogReadiness.Failed;
                throw new InvalidDataException($"Replica group '{owner.GroupId}' metadata is missing while the log file exists; the group requires recovery or repair.");
        }

        await FollowerLogRecovery.RecoverLogFileAsync(journal, owner, cancellationToken).ConfigureAwait(false);
        if (journal.LogGeneration == 0UL)
        {
            CreateFirstLog(journal, owner);
        }
        else
        {
            // A retried open can still hold the superseded file from a rewrite that failed after its commit point; release it first.
            owner.Durability.Dispose();
            await FollowerLogRecovery.EmptySupersededLogAsync(journal, cancellationToken).ConfigureAwait(false);
            owner.Durability.Open(journal.LogPath, owner.LogLength);
        }

        owner.Readiness = FollowerLogReadiness.Ready;
    }

    private static void CreateFirstLog(FollowerLogJournal journal, IFollowerLogContext owner)
    {
        owner.Durability.Create(journal.Paths.GetLogSlotPath(0), 1UL);
        journal.UseLog(0, 1UL);
        owner.LogLength = GroupLogSlots.HeaderSize;
    }

    /// <summary>Picks the active group log file from the two generation files, refusing a log this build cannot trust.</summary>
    /// <param name="journal">The storage surface; its active log is set.</param>
    /// <param name="owner">The owning follower log, whose readiness fails on a refusal.</param>
    /// <exception cref="InvalidDataException">An earlier build's log exists, or the generation files are refused.</exception>
    private static void SelectLog(FollowerLogJournal journal, IFollowerLogContext owner)
    {
        if (File.Exists(journal.Paths.LegacyLogPath))
        {
            owner.Readiness = FollowerLogReadiness.Failed;
            throw new InvalidDataException(
                $"Replica group '{owner.GroupId}' log at '{journal.Paths.LegacyLogPath}' was written by an earlier build and is not supported; the file was left unchanged.");
        }

        Span<byte> first = stackalloc byte[GroupLogSlots.HeaderSize];
        Span<byte> second = stackalloc byte[GroupLogSlots.HeaderSize];
        var firstLength = ReadLogHead(journal.Paths.LogSlot0Path, first, out var firstRead);
        var secondLength = ReadLogHead(journal.Paths.LogSlot1Path, second, out var secondRead);
        try
        {
            var (slot, generation) = GroupLogSlots.Select(first[..firstRead], firstLength, second[..secondRead], secondLength, journal.Paths.GroupDirectory);
            journal.UseLog(slot, generation);
        }
        catch (InvalidDataException)
        {
            owner.Readiness = FollowerLogReadiness.Failed;
            throw;
        }
    }

    private static long ReadLogHead(string path, Span<byte> head, out int read)
    {
        read = 0;
        if (!File.Exists(path))
            return -1;

        using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        var length = RandomAccess.GetLength(handle);
        while (read < head.Length)
        {
            var count = RandomAccess.Read(handle, head[read..], read);
            if (count == 0)
                break;

            read += count;
        }

        return length;
    }
}
