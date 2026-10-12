using System;
using System.Buffers.Binary;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Replication;

/// <summary>Layout, encoding and selection rules of the two-slot group metadata file.</summary>
/// <remarks>
///     <para>
///     The file is exactly two slots of <see cref="SlotSize" /> bytes at offsets 0 and <see cref="SlotSize" />, little-endian throughout. A slot is
///     <c language="csharp">magic(4) | version(1) | sequence(8) | payloadLength(4) | payload | crc32c(4)</c>, zero-padded to the slot size; the checksum covers
///     every byte before it and the payload is the <see cref="GroupLogCodec" /> metadata encoding.
///     </para>
///     <para>
///     A slot is valid when its magic, version, length, checksum and payload all check and its sequence is not zero. The newest valid slot wins; two
///     valid slots with one sequence are accepted only when they are byte-identical, since distinct records never share a sequence.
///     </para>
///     <para>
///     A vote grant is stored in both slots (see <see cref="GroupMetadataStore" />), because the fallback to the older slot after a damaged newest one
///     must never roll back a vote; any other record may be rolled back by one publish.
///     </para>
/// </remarks>
internal static class GroupMetadataSlots
{
    /// <summary>The size of one slot in bytes.</summary>
    internal const int SlotSize = 4096;

    /// <summary>The size of the metadata file in bytes, for its whole life.</summary>
    internal const int FileSize = SlotSize * 2;

    /// <summary>The largest encoded metadata payload a slot holds.</summary>
    internal const int PayloadCapacity = SlotSize - HeaderSize - CrcSize;

    /// <summary>Header size: magic(4) + version(1) + sequence(8) + payloadLength(4).</summary>
    private const int HeaderSize = sizeof(uint) + sizeof(byte) + sizeof(ulong) + sizeof(int);

    private const int CrcSize = sizeof(uint);

    /// <summary>Slot magic bytes, <c language="csharp">"SQMS"</c>.</summary>
    private const uint SlotMagic = 0x534D5153u;

    /// <summary>The slot format version.</summary>
    private const byte SlotVersion = 1;

    /// <summary>Magic bytes of the single-record metadata file written by earlier builds, <c language="csharp">"SQRM"</c>.</summary>
    private const uint EarlierMagic = 0x4D525153u;

    private const int SequenceOffset = sizeof(uint) + sizeof(byte);

    private const int LengthOffset = SequenceOffset + sizeof(ulong);

    /// <summary>Encodes a record into a slot, zeroing the rest of it.</summary>
    /// <param name="slot">The destination of exactly <see cref="SlotSize" /> bytes.</param>
    /// <param name="sequence">The sequence number of the record.</param>
    /// <param name="meta">The record.</param>
    /// <exception cref="InvalidOperationException">The encoded record does not fit a slot; nothing is written.</exception>
    internal static void Encode(Span<byte> slot, ulong sequence, in GroupLogMetadata meta)
    {
        var length = GroupLogCodec.ComputeMetaEncodedLength(in meta);
        if (length > PayloadCapacity)
        {
            throw new InvalidOperationException(
                $"Replica group '{meta.GroupId}' metadata needs {length} bytes, which exceeds the {PayloadCapacity} bytes a metadata slot holds.");
        }

        slot.Clear();
        BinaryPrimitives.WriteUInt32LittleEndian(slot, SlotMagic);
        slot[sizeof(uint)] = SlotVersion;
        BinaryPrimitives.WriteUInt64LittleEndian(slot[SequenceOffset..], sequence);
        BinaryPrimitives.WriteInt32LittleEndian(slot[LengthOffset..], length);
        GroupLogCodec.EncodeMeta(in meta, slot.Slice(HeaderSize, length));
        var crcOffset = HeaderSize + length;
        BinaryPrimitives.WriteUInt32LittleEndian(slot[crcOffset..], Crc32C.Compute(slot[..crcOffset]));
    }

    /// <summary>Applies the slot rule to a whole metadata file image.</summary>
    /// <param name="image">The bytes of the file.</param>
    /// <returns>The newest valid record, or why there is none.</returns>
    internal static GroupMetadataRead Select(ReadOnlySpan<byte> image)
    {
        // Only the earlier magic proves a file belongs to an earlier build; any other wrong size is damage.
        if (image.Length >= sizeof(uint) && BinaryPrimitives.ReadUInt32LittleEndian(image) == EarlierMagic)
            return Failure(GroupMetadataReadStatus.Unsupported);

        if (image.Length != FileSize)
            return Failure(GroupMetadataReadStatus.Corrupt);

        var first = image[..SlotSize];
        var second = image[SlotSize..];
        var firstValid = TryDecode(first, out var firstSequence, out var firstMeta);
        var secondValid = TryDecode(second, out var secondSequence, out var secondMeta);
        return (firstValid, secondValid) switch
        {
            (false, false) => Failure(IsEarlierBuild(first) || IsEarlierBuild(second) ? GroupMetadataReadStatus.Unsupported : GroupMetadataReadStatus.Corrupt),
            (true, false) => new GroupMetadataRead(GroupMetadataReadStatus.Valid, firstMeta, 0, firstSequence),
            (false, true) => new GroupMetadataRead(GroupMetadataReadStatus.Valid, secondMeta, 1, secondSequence),
            _ when firstSequence > secondSequence => new GroupMetadataRead(GroupMetadataReadStatus.Valid, firstMeta, 0, firstSequence),
            _ when secondSequence > firstSequence => new GroupMetadataRead(GroupMetadataReadStatus.Valid, secondMeta, 1, secondSequence),
            _ when first.SequenceEqual(second) => new GroupMetadataRead(GroupMetadataReadStatus.Valid, firstMeta, 0, firstSequence),
            _ => Failure(GroupMetadataReadStatus.Corrupt),
        };
    }

    private static GroupMetadataRead Failure(GroupMetadataReadStatus status) => new(status, default, 0, 0UL);

    /// <summary>Checks whether a slot is well framed and holds a record of an earlier metadata version.</summary>
    /// <param name="slot">The bytes of the slot.</param>
    /// <returns><see langword="true" /> when the slot frame is valid and its payload was written by an earlier build.</returns>
    private static bool IsEarlierBuild(ReadOnlySpan<byte> slot) => TryReadPayload(slot, out _, out var payload) && GroupLogCodec.IsEarlierMetaVersion(payload);

    private static bool TryDecode(ReadOnlySpan<byte> slot, out ulong sequence, out GroupLogMetadata meta)
    {
        meta = default;
        return TryReadPayload(slot, out sequence, out var payload) && GroupLogCodec.TryDecodeMeta(payload, out meta);
    }

    private static bool TryReadPayload(ReadOnlySpan<byte> slot, out ulong sequence, out ReadOnlySpan<byte> payload)
    {
        payload = default;
        sequence = BinaryPrimitives.ReadUInt64LittleEndian(slot[SequenceOffset..]);
        var length = BinaryPrimitives.ReadInt32LittleEndian(slot[LengthOffset..]);
        if (BinaryPrimitives.ReadUInt32LittleEndian(slot) != SlotMagic || slot[sizeof(uint)] != SlotVersion || sequence == 0UL || length < 0 || length > PayloadCapacity)
            return false;

        var crcOffset = HeaderSize + length;
        if (Crc32C.Compute(slot[..crcOffset]) != BinaryPrimitives.ReadUInt32LittleEndian(slot[crcOffset..]))
            return false;

        payload = slot.Slice(HeaderSize, length);
        return true;
    }
}
