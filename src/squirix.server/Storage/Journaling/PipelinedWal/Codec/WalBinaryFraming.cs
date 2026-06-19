using System;
using System.Buffers.Binary;
using System.IO;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.PipelinedWal.Codec;

/// <summary>Binary WAL framing constants (SJRN file header version 2).</summary>
internal static class WalBinaryFraming
{
    internal const int FileHeaderSize = 4 + 1;

    internal const int FrameFooterSize = 4;

    internal const int FrameHeaderSize = 4;

    internal const byte FileVersion = 2;

    internal static ReadOnlySpan<byte> Magic => "SJRN"u8;

    public static void WriteFileHeader(Span<byte> destination)
    {
        Magic.CopyTo(destination);
        destination[4] = FileVersion;
    }

    public static void ValidateFileHeader(ReadOnlySpan<byte> header)
    {
        if (header.Length < FileHeaderSize)
            throw new InvalidDataException("journal segment has a truncated file header.");

        if (!header[..4].SequenceEqual(Magic) || header[4] != FileVersion)
            throw new InvalidDataException("journal segment has an invalid binary WAL header.");
    }

    public static int ComputeFrameBodyLength(JournalRecord record) => BinaryWalJournalCodec.ComputeFrameBodyLength(record);

    public static void WriteFrame(Span<byte> frame, ReadOnlySpan<byte> body)
    {
        BinaryPrimitives.WriteInt32LittleEndian(frame, body.Length);
        body.CopyTo(frame[FrameHeaderSize..]);
        var crc = Crc32C.Compute(body);
        BinaryPrimitives.WriteUInt32LittleEndian(frame[(FrameHeaderSize + body.Length)..], crc);
    }

    public static int FrameTotalLength(int bodyLength) => FrameHeaderSize + bodyLength + FrameFooterSize;
}
