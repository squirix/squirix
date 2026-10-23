using System;
using System.Buffers.Binary;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.TestKit.Replication;

/// <summary>The copy, the snapshot and the checks the tests of a state transfer share.</summary>
internal static class TransferFixtures
{
    /// <summary>The group the transfer tests use.</summary>
    internal const string GroupId = "grp-transfer";

    /// <summary>The index of the snapshot a leader sends.</summary>
    internal const ulong SnapshotIndex = 3UL;

    /// <summary>The term of the entry at <see cref="SnapshotIndex" />.</summary>
    internal const ulong SnapshotTerm = 1UL;

    /// <summary>The topology fingerprint the receiving registry is activated with.</summary>
    internal static readonly byte[] Fingerprint = [9, 8, 7];

    /// <summary>Encodes the published file image a leader streams: the state of the group at <see cref="SnapshotIndex" /> in term <see cref="SnapshotTerm" />, with no idempotency outcome.</summary>
    /// <returns>The image.</returns>
    internal static byte[] Image()
    {
        var snapshot = new GroupSnapshot(GroupId, Fingerprint, 1UL, SnapshotTerm, SnapshotIndex, SnapshotIndex, [], new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        var image = new byte[GroupSnapshotStore.ComputePublishedLength(in snapshot, GroupSnapshotStore.DefaultMaxSnapshotBytes)];
        GroupSnapshotStore.EncodePublished(image, in snapshot);
        return image;
    }

    /// <summary>Reads the CRC32C an image ends with.</summary>
    /// <param name="image">The published image.</param>
    /// <returns>The checksum.</returns>
    internal static uint ChecksumOf(byte[] image) => BinaryPrimitives.ReadUInt32LittleEndian(image.AsSpan(image.Length - sizeof(uint)));

    /// <summary>Creates the append request of one entry of term one from a leader.</summary>
    /// <param name="index">The log index of the entry.</param>
    /// <param name="payload">The payload text.</param>
    /// <returns>The request.</returns>
    internal static FollowerLogAppendRequest AppendAt(ulong index, string payload) => new(
        "leader",
        1UL,
        index - 1UL,
        index == 1UL ? 0UL : 1UL,
        0UL,
        new[] { new FollowerLogEntry(index, 1UL, Encoding.UTF8.GetBytes(payload)) });

    /// <summary>Reads the member state each of the two metadata slots holds when read alone.</summary>
    /// <param name="bytes">The bytes of the metadata file.</param>
    /// <returns>The member state each slot alone reads, in slot order.</returns>
    /// <exception cref="InvalidOperationException">A slot alone holds no valid record.</exception>
    internal static (GroupMemberState First, GroupMemberState Second) SlotStates(byte[] bytes) => (SlotAlone(bytes, 0), SlotAlone(bytes, 1));

    /// <summary>Creates a complete copy of the group holding two committed and applied entries of term one, and closes it.</summary>
    /// <param name="root">The node data directory.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the copy is on disk.</returns>
    internal static async Task SeedBehindAsync(string root, CancellationToken cancellationToken)
    {
        await using var log = new FollowerLog(root, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await log.OpenSeededAsync(cancellationToken).ConfigureAwait(false);
        _ = await log.AppendAsync(AppendAt(1UL, "a"), cancellationToken).ConfigureAwait(false);
        _ = await log.AppendAsync(AppendAt(2UL, "b"), cancellationToken).ConfigureAwait(false);
        _ = await log.AdvanceCommitAsync(2UL, cancellationToken).ConfigureAwait(false);
        _ = await log.AdvanceAppliedAsync(2UL, cancellationToken).ConfigureAwait(false);
    }

    private static GroupMemberState SlotAlone(byte[] bytes, int slot)
    {
        var alone = new byte[bytes.Length];
        bytes.CopyTo(alone, 0);
        Array.Clear(alone, (1 - slot) * GroupMetadataSlots.SlotSize, GroupMetadataSlots.SlotSize);
        var read = GroupMetadataSlots.Select(alone);
        return read.Status == GroupMetadataReadStatus.Valid ? read.Metadata.MemberState : throw new InvalidOperationException($"Slot {slot} alone holds no valid record ({read.Status}).");
    }
}
