using System;
using System.Buffers.Binary;
using System.IO;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Fixed-size SQMC CURRENT pointer; on-disk layout is documented in docs/manifest-format.md.</summary>
internal static class ManifestPointer
{
    internal const int Size = 12;

    private static ReadOnlySpan<byte> Magic => "SQMC"u8;

    public static void Write(Span<byte> destination, int manifestIndex)
    {
        if (destination.Length < Size)
            throw new ArgumentException("Destination span is too small for a Manifest pointer.", nameof(destination));

        Magic.CopyTo(destination);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(Magic.Length), manifestIndex);
        var crc = Crc32C.Compute(destination.Slice(0, Magic.Length + 4));
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(Magic.Length + 4), crc);
    }

    public static int Read(ReadOnlySpan<byte> source)
    {
        if (source.Length < Size)
            throw new InvalidDataException("Manifest current pointer is truncated.");

        if (!source.StartsWith(Magic))
            throw new InvalidDataException("Manifest current pointer has an invalid magic header.");

        var index = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(Magic.Length));
        var expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(Magic.Length + 4));
        if (Crc32C.Compute(source.Slice(0, Magic.Length + 4)) != expectedCrc)
            throw new InvalidDataException("Manifest current pointer failed CRC validation.");

        if (index <= 0)
            throw new InvalidDataException("Manifest current pointer index is invalid.");

        return index;
    }

    public static bool IsValidPointer(ReadOnlySpan<byte> source) =>
        source.Length >= Magic.Length && source.StartsWith(Magic);
}
