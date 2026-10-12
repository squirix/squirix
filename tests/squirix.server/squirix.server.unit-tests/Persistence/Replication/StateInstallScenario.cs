using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Shared setup of the state transfer tests: a leader snapshot, a follower holding part of the leader log, and the slot checks.</summary>
internal static class StateInstallScenario
{
    /// <summary>The group the state transfer tests use.</summary>
    internal const string GroupId = "grp-transfer";

    /// <summary>The index the leader snapshot is taken at.</summary>
    internal const ulong SnapshotIndex = 3UL;

    /// <summary>Builds a leader log of three entries of term one with one resolved outcome, and returns the snapshot its compaction publishes.</summary>
    /// <param name="sourceDir">The data directory of the leader.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The published snapshot through <see cref="SnapshotIndex" />.</returns>
    internal static async Task<GroupSnapshot> LeaderSnapshotAsync(TempDirectory sourceDir, CancellationToken cancellationToken)
    {
        await using var source = NewLog(sourceDir);
        await source.OpenSeededAsync(cancellationToken);
        _ = await source.AppendAsync(FollowerFoundationScenario.Append("leader", 1UL, 1UL, "a"), cancellationToken);
        _ = await source.AppendAsync(FollowerFoundationScenario.Append("leader", 2UL, 1UL, "b"), cancellationToken);
        _ = await source.AppendAsync(FollowerFoundationScenario.Append("leader", 3UL, 1UL, "c"), cancellationToken);
        _ = source.Idempotency.Reserve("client", "operation-1", [1, 2, 3], GroupRecordKind.UserMutation, 1UL, 1UL);
        _ = source.Idempotency.TryResolve("client", "operation-1", [9], 1UL, 1UL);
        _ = await source.AdvanceCommitAsync(SnapshotIndex, cancellationToken);
        return await FollowerSnapshotScenario.CompactThroughAsync(source, sourceDir, SnapshotIndex, cancellationToken);
    }

    /// <summary>Creates a log of the group on a data directory, not opened.</summary>
    /// <param name="dir">The data directory.</param>
    /// <returns>The log.</returns>
    internal static FollowerLog NewLog(TempDirectory dir) => new(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);

    /// <summary>Creates a log of the group on a data directory with fault hooks, not opened.</summary>
    /// <param name="dir">The data directory.</param>
    /// <param name="faults">The fault hooks.</param>
    /// <returns>The log.</returns>
    internal static FollowerLog NewLog(TempDirectory dir, IFollowerLogFaultHooks faults) =>
        new(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, faults, TimeProvider.System);

    /// <summary>Opens a complete copy holding the first two entries of the leader log, committed and applied.</summary>
    /// <param name="log">The log to open.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the copy holds the entries.</returns>
    internal static async Task OpenBehindAsync(FollowerLog log, CancellationToken cancellationToken)
    {
        await log.OpenSeededAsync(cancellationToken);
        _ = await log.AppendAsync(FollowerFoundationScenario.Append("leader", 1UL, 1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(FollowerFoundationScenario.Append("leader", 2UL, 1UL, "b"), cancellationToken);
        _ = await log.AdvanceCommitAsync(2UL, cancellationToken);
        _ = await log.AdvanceAppliedAsync(2UL, cancellationToken);
    }

    /// <summary>Reads the bytes of the log file while the log holds it open.</summary>
    /// <param name="dir">The data directory.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The bytes of the file.</returns>
    internal static async Task<byte[]> ReadLogBytesAsync(TempDirectory dir, CancellationToken cancellationToken)
    {
        using var handle = File.OpenHandle(GroupStoragePaths.GetLogPath(dir, GroupId), FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, FileOptions.Asynchronous);
        var bytes = new byte[RandomAccess.GetLength(handle)];
        var read = 0;
        while (read < bytes.Length)
        {
            var count = await RandomAccess.ReadAsync(handle, bytes.AsMemory(read), read, cancellationToken);
            if (count == 0)
                break;

            read += count;
        }

        return bytes;
    }

    /// <summary>Reads the member state each of the two metadata slots holds when read alone.</summary>
    /// <param name="bytes">The bytes of the metadata file.</param>
    /// <returns>The member state each slot alone reads, in slot order.</returns>
    internal static (GroupMemberState First, GroupMemberState Second) SlotStates(byte[] bytes)
    {
        var first = SlotAlone(bytes, 0);
        var second = SlotAlone(bytes, 1);
        return (first, second);
    }

    /// <summary>Reads the record a metadata file holds when only one of its slots is kept.</summary>
    /// <param name="bytes">The bytes of the metadata file.</param>
    /// <param name="slot">The slot that is kept.</param>
    /// <returns>The record of the slot.</returns>
    /// <exception cref="InvalidOperationException">The slot alone holds no valid record.</exception>
    internal static GroupLogMetadata SlotRecord(byte[] bytes, int slot)
    {
        var alone = new byte[bytes.Length];
        bytes.CopyTo(alone, 0);
        Array.Clear(alone, (1 - slot) * GroupMetadataSlots.SlotSize, GroupMetadataSlots.SlotSize);
        var read = GroupMetadataSlots.Select(alone);
        return read.Status == GroupMetadataReadStatus.Valid ? read.Metadata : throw new InvalidOperationException($"Slot {slot} alone holds no valid record ({read.Status}).");
    }

    private static GroupMemberState SlotAlone(byte[] bytes, int slot) => SlotRecord(bytes, slot).MemberState;
}
