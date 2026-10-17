using System;
using System.Buffers.Binary;
using System.IO;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Layout, encoding and selection rules of the two-slot <c language="csharp">man-current</c> pointer file.</summary>
/// <remarks>
///     <para>
///     The file is exactly two slots of <see cref="SlotSize" /> bytes at offsets 0 and <see cref="SlotSize" />, little-endian throughout. A slot is
///     <c language="csharp">magic(4) | version(1) | sequence(8) | manifestIndex(4) | crc32c(4)</c>, the rest of the slot stays zero; the checksum covers
///     every byte before it.
///     </para>
///     <para>
///     A slot is valid when its magic, version, checksum and index check and its sequence is not zero. The newest valid slot wins; two valid slots
///     with one sequence are accepted only when they are byte-identical. A file whose two slot records hold no non-zero byte was never published. Any other shape
///     without a valid slot is damage and is never repaired here.
///     </para>
/// </remarks>
internal static class PointerSlots
{
    /// <summary>The size of one slot in bytes.</summary>
    internal const int SlotSize = 4096;

    /// <summary>The size of the pointer file in bytes, for its whole life.</summary>
    internal const int FileSize = SlotSize * 2;

    /// <summary>The size of the record at the start of a slot in bytes.</summary>
    internal const int RecordSize = CrcOffset + sizeof(uint);

    /// <summary>Slot magic bytes, <c language="csharp">"SQMP"</c>.</summary>
    private const uint SlotMagic = 0x504D5153u;

    /// <summary>Magic bytes of the single-record pointer written by earlier builds, <c language="csharp">"SQMC"</c>.</summary>
    private const uint EarlierMagic = 0x434D5153u;

    private const byte SlotVersion = 1;

    private const int SequenceOffset = sizeof(uint) + sizeof(byte);

    private const int IndexOffset = SequenceOffset + sizeof(ulong);

    private const int CrcOffset = IndexOffset + sizeof(int);

    private enum SlotState
    {
        Absent = 0,
        Valid = 1,
        Torn = 2,
    }

    /// <summary>Encodes a record.</summary>
    /// <param name="record">The destination of at least <see cref="RecordSize" /> bytes.</param>
    /// <param name="sequence">The sequence number of the record, at least one.</param>
    /// <param name="manifestIndex">The manifest index, at least one.</param>
    /// <exception cref="ArgumentException">The destination is too small.</exception>
    internal static void Encode(Span<byte> record, ulong sequence, int manifestIndex)
    {
        if (record.Length < RecordSize)
            throw new ArgumentException("Destination span is too small for a Manifest pointer record.", nameof(record));

        BinaryPrimitives.WriteUInt32LittleEndian(record, SlotMagic);
        record[sizeof(uint)] = SlotVersion;
        BinaryPrimitives.WriteUInt64LittleEndian(record[SequenceOffset..], sequence);
        BinaryPrimitives.WriteInt32LittleEndian(record[IndexOffset..], manifestIndex);
        BinaryPrimitives.WriteUInt32LittleEndian(record[CrcOffset..], Crc32C.Compute(record[..CrcOffset]));
    }

    /// <summary>Applies the slot rule to the two records of a pointer file.</summary>
    /// <param name="first">The bytes present of the record in slot 0, at most <see cref="RecordSize" />.</param>
    /// <param name="second">The bytes present of the record in slot 1, at most <see cref="RecordSize" />.</param>
    /// <param name="fileLength">The length of the file in bytes.</param>
    /// <param name="path">The file path, for messages.</param>
    /// <returns>The newest valid record, or a never published pointer.</returns>
    /// <exception cref="InvalidDataException">The file was written by an earlier build or holds no valid slot.</exception>
    internal static PointerSelection Select(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second, long fileLength, string path)
    {
        if (first.Length >= sizeof(uint) && BinaryPrimitives.ReadUInt32LittleEndian(first) == EarlierMagic)
        {
            throw new InvalidDataException(
                $"Manifest pointer at {path} was written by an earlier build and is not supported; the file was left unchanged.");
        }

        if (fileLength > FileSize)
            throw new InvalidDataException($"Manifest pointer at {path} has invalid length {fileLength}.");

        var firstState = Classify(first, out var firstSequence, out var firstIndex);
        var secondState = Classify(second, out var secondSequence, out var secondIndex);
        return (firstState, secondState) switch
        {
            (SlotState.Valid, SlotState.Valid) when firstSequence > secondSequence => new PointerSelection(firstIndex, 0, firstSequence),
            (SlotState.Valid, SlotState.Valid) when secondSequence > firstSequence => new PointerSelection(secondIndex, 1, secondSequence),
            (SlotState.Valid, SlotState.Valid) when first.SequenceEqual(second) => new PointerSelection(firstIndex, 0, firstSequence),
            (SlotState.Valid, SlotState.Valid) => throw new InvalidDataException($"Manifest pointer at {path} has two different records with one sequence."),
            (SlotState.Valid, _) => new PointerSelection(firstIndex, 0, firstSequence),
            (_, SlotState.Valid) => new PointerSelection(secondIndex, 1, secondSequence),
            (SlotState.Absent, SlotState.Absent) => new PointerSelection(0, 1, 0UL),
            _ => throw new InvalidDataException($"Manifest pointer at {path} has no valid slot."),
        };
    }

    private static SlotState Classify(ReadOnlySpan<byte> record, out ulong sequence, out int manifestIndex)
    {
        sequence = 0UL;
        manifestIndex = 0;
        if (!record.ContainsAnyExcept<byte>(0))
            return SlotState.Absent;

        if (record.Length < RecordSize || BinaryPrimitives.ReadUInt32LittleEndian(record) != SlotMagic || record[sizeof(uint)] != SlotVersion)
            return SlotState.Torn;

        sequence = BinaryPrimitives.ReadUInt64LittleEndian(record[SequenceOffset..]);
        manifestIndex = BinaryPrimitives.ReadInt32LittleEndian(record[IndexOffset..]);
        var isValid = sequence != 0UL && manifestIndex > 0 && Crc32C.Compute(record[..CrcOffset]) == BinaryPrimitives.ReadUInt32LittleEndian(record[CrcOffset..]);
        return isValid ? SlotState.Valid : SlotState.Torn;
    }
}
