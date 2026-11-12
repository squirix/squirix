using System;
using System.Buffers.Binary;
using System.IO;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Replication;

/// <summary>Two checksummed pointers to immutable snapshot generations in one small file.</summary>
internal static class GroupSnapshotPointers
{
    internal const int SlotSize = 4096;
    internal const int FileSize = SlotSize * 2;
    private const uint Magic = 0x53515047;
    private const int CrcOffset = SlotSize - sizeof(uint);

    internal static void Encode(Span<byte> slot, ulong sequence, Guid generation, (ulong Generation, long Offset, long RewrittenOffset, long RewrittenBoundary, ulong RewrittenGeneration) position = default)
    {
        slot.Clear();
        BinaryPrimitives.WriteUInt32LittleEndian(slot, Magic);
        BinaryPrimitives.WriteUInt64LittleEndian(slot.Slice(4), sequence);
        _ = generation.TryWriteBytes(slot.Slice(12, 16));
        BinaryPrimitives.WriteUInt64LittleEndian(slot[28..], position.Generation);
        BinaryPrimitives.WriteInt64LittleEndian(slot[36..], position.Offset);
        BinaryPrimitives.WriteInt64LittleEndian(slot[44..], position.RewrittenOffset);
        BinaryPrimitives.WriteInt64LittleEndian(slot[52..], position.RewrittenBoundary);
        BinaryPrimitives.WriteUInt64LittleEndian(slot[60..], position.RewrittenGeneration);
        BinaryPrimitives.WriteUInt32LittleEndian(slot.Slice(CrcOffset), Crc32C.Compute(slot.Slice(0, CrcOffset)));
    }

    internal static SnapshotPointer Decode(ReadOnlySpan<byte> image, int slot)
    {
        var offset = slot * SlotSize;
        if (image.Length < offset + SlotSize)
            return default;

        var bytes = image.Slice(offset, SlotSize);
        if (BinaryPrimitives.ReadUInt32LittleEndian(bytes) != Magic ||
            Crc32C.Compute(bytes[..CrcOffset]) != BinaryPrimitives.ReadUInt32LittleEndian(bytes[CrcOffset..]))
            return default;

        var sequence = BinaryPrimitives.ReadUInt64LittleEndian(bytes[4..]);
        var generation = new Guid(bytes.Slice(12, 16));
        var pointer = new SnapshotPointer(sequence, generation, slot)
        {
            WalPosition = (BinaryPrimitives.ReadUInt64LittleEndian(bytes[28..]), BinaryPrimitives.ReadInt64LittleEndian(bytes[36..]),
                BinaryPrimitives.ReadInt64LittleEndian(bytes[44..]), BinaryPrimitives.ReadInt64LittleEndian(bytes[52..]), BinaryPrimitives.ReadUInt64LittleEndian(bytes[60..])),
        };
        return sequence == 0UL ? default : pointer;
    }

    internal static (bool ReferencesCurrent, ulong Latest) ReadWalGenerations(string path, ulong current)
    {
        var latest = current;
        var referencesCurrent = false;
        foreach (var pointer in Read(path))
        {
            var (generation, _, _, _, rewritten) = pointer.WalPosition;
            referencesCurrent |= current != 0UL && (generation == current || rewritten == current);
            latest = Math.Max(latest, Math.Max(generation, rewritten));
        }

        return (referencesCurrent, latest);
    }

    internal static SnapshotPointer[] Read(string path)
    {
        if (!File.Exists(path))
            return [];

        using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        if (RandomAccess.GetLength(handle) > FileSize)
            throw new InvalidDataException("The snapshot pointer file exceeds its fixed size.");

        Span<byte> image = stackalloc byte[FileSize];
        var read = 0;
        while (read < image.Length)
        {
            var count = RandomAccess.Read(handle, image.Slice(read), read);
            if (count == 0)
                break;

            read += count;
        }

        var first = Decode(image.Slice(0, read), 0);
        var second = Decode(image.Slice(0, read), 1);
        SnapshotPointer[] ordered = first.Sequence >= second.Sequence ? [first, second] : [second, first];
        return first.Sequence != 0UL && first.Sequence == second.Sequence && first.Generation != second.Generation
            ? throw new InvalidDataException("Snapshot pointers of the same generation disagree.")
            : ordered;
    }
}
