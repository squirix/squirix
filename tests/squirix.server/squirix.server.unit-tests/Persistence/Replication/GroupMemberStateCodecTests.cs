using System;
using System.Buffers.Binary;
using System.Text;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>How the member state and the bootstrap offer are encoded in a metadata record, and how a record of an earlier version is told from damage.</summary>
[Immutable]
public sealed class GroupMemberStateCodecTests : ServerUnitTestBase
{
    private const string GroupId = "member-state";

    /// <summary>Offset of the member state byte in an encoded record: magic(4) + version(1) + four ulongs.</summary>
    private const int StateOffset = 5 + 32;

    private static readonly GroupMemberState[] States = [GroupMemberState.Unseeded, GroupMemberState.Complete, GroupMemberState.Incomplete];

    /// <summary>Every state with either offer value is read back as written.</summary>
    /// <returns>A task that completes when the assertions are done.</returns>
    [Test]
    public async Task StateAndOfferRoundTrip()
    {
        // Each state is tried with both offer values: even steps are not offered, odd steps are.
        for (var step = 0; step < States.Length * 2; step++)
        {
            var state = States[step / 2];
            var offered = step % 2 == 1;
            var meta = Meta(state, offered);
            var buffer = Encode(in meta);

            var decoded = GroupLogCodec.TryDecodeMeta(buffer, out var read);

            _ = await Assert.That(decoded).IsTrue();
            _ = await Assert.That((read.MemberState, read.BootstrapOffered, read.CurrentTerm, read.VotedFor)).IsEqualTo((state, offered, 4UL, "node-b"));
        }
    }

    /// <summary>The record grew by the two bytes of the state and the offer against the earlier layout.</summary>
    /// <returns>A task that completes when the assertions are done.</returns>
    [Test]
    public async Task EncodedSizeGrewByTwoBytes()
    {
        var meta = new GroupLogMetadata(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 4UL, string.Empty, 0UL, 0UL, GroupMemberState.Complete, false);
        var earlier = EncodeEarlier(GroupId, 4UL);

        _ = await Assert.That(GroupLogCodec.ComputeMetaEncodedLength(in meta)).IsEqualTo(earlier.Length + 2);
    }

    /// <summary>A state byte outside the named values makes the record invalid.</summary>
    /// <returns>A task that completes when the assertions are done.</returns>
    [Test]
    public async Task UnknownStateByteIsInvalid()
    {
        var buffer = Encode(Meta(GroupMemberState.Complete, false));
        buffer[StateOffset] = 3;
        Reseal(buffer);

        _ = await Assert.That(GroupLogCodec.TryDecodeMeta(buffer, out _)).IsFalse();
    }

    /// <summary>An offer byte other than zero or one makes the record invalid.</summary>
    /// <returns>A task that completes when the assertions are done.</returns>
    [Test]
    public async Task UnknownOfferByteIsInvalid()
    {
        var buffer = Encode(Meta(GroupMemberState.Unseeded, true));
        buffer[StateOffset + 1] = 2;
        Reseal(buffer);

        _ = await Assert.That(GroupLogCodec.TryDecodeMeta(buffer, out _)).IsFalse();
    }

    /// <summary>A well-framed slot holding an earlier metadata version is unsupported.</summary>
    /// <returns>A task that completes when the assertions are done.</returns>
    [Test]
    public async Task EarlierVersionSlotIsUnsupported()
    {
        var image = EarlierImage();

        var read = GroupMetadataSlots.Select(image);

        _ = await Assert.That(read.Status).IsEqualTo(GroupMetadataReadStatus.Unsupported);
    }

    /// <summary>A damaged slot stays corrupt, whether it held the current version or an earlier one.</summary>
    /// <returns>A task that completes when the assertions are done.</returns>
    [Test]
    public async Task DamagedSlotStaysCorrupt()
    {
        var earlier = EarlierImage();
        earlier[40]++;
        var current = new byte[GroupMetadataSlots.FileSize];
        var meta = Meta(GroupMemberState.Complete, false);
        GroupMetadataSlots.Encode(current.AsSpan(0, GroupMetadataSlots.SlotSize), 1UL, in meta);
        current[40]++;

        _ = await Assert.That(GroupMetadataSlots.Select(earlier).Status).IsEqualTo(GroupMetadataReadStatus.Corrupt);
        _ = await Assert.That(GroupMetadataSlots.Select(current).Status).IsEqualTo(GroupMetadataReadStatus.Corrupt);
    }

    /// <summary>Builds a metadata file image whose first slot holds a record of the earlier version.</summary>
    /// <returns>The image of the whole file.</returns>
    internal static byte[] EarlierImage()
    {
        var payload = EncodeEarlier(GroupId, 4UL);
        var image = new byte[GroupMetadataSlots.FileSize];
        BinaryPrimitives.WriteUInt32LittleEndian(image, 0x534D5153u);
        image[4] = 1;
        BinaryPrimitives.WriteUInt64LittleEndian(image.AsSpan(5), 1UL);
        BinaryPrimitives.WriteInt32LittleEndian(image.AsSpan(13), payload.Length);
        payload.CopyTo(image, 17);
        var crcOffset = 17 + payload.Length;
        BinaryPrimitives.WriteUInt32LittleEndian(image.AsSpan(crcOffset), Crc32C.Compute(image.AsSpan(0, crcOffset)));
        return image;
    }

    private static byte[] Encode(in GroupLogMetadata meta)
    {
        var buffer = new byte[GroupLogCodec.ComputeMetaEncodedLength(in meta)];
        GroupLogCodec.EncodeMeta(in meta, buffer);
        return buffer;
    }

    /// <summary>Encodes a record in the layout of version one: the four ulongs, then the three length-prefixed fields.</summary>
    /// <param name="groupId">The replica group identifier.</param>
    /// <param name="term">The current term.</param>
    /// <returns>The sealed record.</returns>
    private static byte[] EncodeEarlier(string groupId, ulong term)
    {
        var id = Encoding.UTF8.GetBytes(groupId);
        var bytes = new byte[5 + 32 + 4 + id.Length + 4 + 4 + 4];
        BinaryPrimitives.WriteUInt32LittleEndian(bytes, 0x4D525153u);
        bytes[4] = 1;
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(13), term);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(37), id.Length);
        id.CopyTo(bytes, 41);
        Reseal(bytes);
        return bytes;
    }

    private static GroupLogMetadata Meta(GroupMemberState state, bool offered) =>
        new(GroupId, new byte[] { 1, 2, 3 }, 2UL, 4UL, "node-b", 7UL, 5UL, state, offered);

    private static void Reseal(byte[] bytes) =>
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(bytes.Length - 4), Crc32C.Compute(bytes.AsSpan(4, bytes.Length - 8)));
}
