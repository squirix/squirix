using System;
using System.Buffers.Binary;
using System.Globalization;
using System.IO;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.JsonFramed;

internal static class JournalFraming
{
    internal const int FileHeaderSize = 4 + 1; // Magic(4) + Version(1)

    internal const int FrameHeaderSize = JournalFrameEnvelope.HeaderSize;

    internal const byte Version = 1;

    private const int FrameFooterSize = JournalFrameEnvelope.FooterSize;

    /// <summary>Gets the on-disk segment file magic (four ASCII bytes).</summary>
    internal static ReadOnlySpan<byte> Magic => "SJRN"u8;

    public static int FrameTotalLength(int bodyLength) => FrameHeaderSize + bodyLength + FrameFooterSize;

    public static void WriteFileHeader(Stream stream)
    {
        stream.Write(Magic);
        stream.WriteByte(Version);
    }

    public static void WriteFrame(Stream stream, ReadOnlySpan<byte> payload)
    {
        Span<byte> lenBuf = stackalloc byte[FrameHeaderSize];
        BinaryPrimitives.WriteInt32LittleEndian(lenBuf, payload.Length);
        stream.Write(lenBuf);
        stream.Write(payload);

        var crc = Crc32C.Compute(payload);
        Span<byte> crcBuf = stackalloc byte[FrameFooterSize];
        BinaryPrimitives.WriteUInt32LittleEndian(crcBuf, crc);
        stream.Write(crcBuf);
    }

    internal static InvalidDataException CreateTruncatedHeaderException(long fileLength) =>
        new($"journal segment has a truncated file header ({fileLength.ToString(CultureInfo.InvariantCulture)} bytes).");

    /// <summary>
    /// Throws when a non-empty segment file does not contain a valid journal header.
    /// Zero-length files are allowed (brand-new segment).
    /// </summary>
    /// <param name="fileLength">Total segment file length in bytes.</param>
    /// <param name="header">First <see cref="FileHeaderSize" /> bytes when the file is at least that long.</param>
    internal static void ThrowIfSegmentHeaderInvalid(long fileLength, ReadOnlySpan<byte> header)
    {
        switch (fileLength)
        {
            case 0:
                return;
            case < FileHeaderSize:
                throw CreateTruncatedHeaderException(fileLength);
            default:
                ThrowIfSegmentHeaderBytesInvalid(header);
                return;
        }
    }

    private static InvalidDataException CreateInvalidHeaderException() => new("invalid or missing journal file header");

    private static bool IsSegmentHeaderValid(ReadOnlySpan<byte> header) => header[..4].SequenceEqual(Magic) && header[4] == Version;

    private static void ThrowIfSegmentHeaderBytesInvalid(ReadOnlySpan<byte> header)
    {
        if (!IsSegmentHeaderValid(header))
            throw CreateInvalidHeaderException();
    }
}
