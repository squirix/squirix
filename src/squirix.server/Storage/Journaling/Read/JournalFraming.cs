using System;
using System.Buffers.Binary;
using System.IO;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.Read;

internal static class JournalFraming
{
    /// <summary>Magic(4) + Version(1).</summary>
    internal const int FileHeaderSize = 4 + 1;

    internal const int FrameHeaderSize = JournalFrameEnvelope.HeaderSize;

    internal const byte Version = 1;

    private const int FrameFooterSize = JournalFrameEnvelope.FooterSize;

    /// <summary>Gets the on-disk segment file magic (four ASCII bytes).</summary>
    private static ReadOnlySpan<byte> Magic => "SJRN"u8;

    internal static InvalidDataException CreateTruncatedHeaderException() => new("journal segment has a truncated file header.");

    internal static int FrameTotalLength(int bodyLength) => FrameHeaderSize + bodyLength + FrameFooterSize;

    /// <summary>Reads the segment file header from <paramref name="handle" /> at <paramref name="offset" /> and validates it, advancing the offset past the header.</summary>
    /// <param name="handle">The segment file handle.</param>
    /// <param name="offset">The read offset; advanced past the header on success.</param>
    /// <exception cref="InvalidDataException">Thrown when the header cannot be read completely or is invalid.</exception>
    internal static void ReadAndValidateSegmentHeader(SafeFileHandle handle, long offset)
    {
        Span<byte> header = stackalloc byte[FileHeaderSize];
        if (!HandleEx.TryReadExact(handle, header, offset))
            throw CreateTruncatedHeaderException();

        EnsureSegmentHeaderSupported(header);
    }

    internal static void WriteFileHeader(Span<byte> destination)
    {
        Magic.CopyTo(destination);
        destination[4] = Version;
    }

    internal static void WriteFrame(Span<byte> frame, ReadOnlySpan<byte> body)
    {
        BinaryPrimitives.WriteInt32LittleEndian(frame, body.Length);
        body.CopyTo(frame[FrameHeaderSize..(FrameHeaderSize + body.Length)]);
        var crc = Crc32C.Compute(body);
        BinaryPrimitives.WriteUInt32LittleEndian(frame.Slice(FrameHeaderSize + body.Length, FrameFooterSize), crc);
    }

    private static InvalidDataException CreateInvalidHeaderException() => new("invalid or missing journal file header");

    private static void EnsureSegmentHeaderSupported(ReadOnlySpan<byte> header) => ThrowIfSegmentHeaderInvalid(header.Length, header);

    private static bool IsSegmentHeaderValid(ReadOnlySpan<byte> header) => header[..4].SequenceEqual(Magic) && header[4] == Version;

    private static void ThrowIfSegmentHeaderBytesInvalid(ReadOnlySpan<byte> header)
    {
        if (!IsSegmentHeaderValid(header))
            throw CreateInvalidHeaderException();
    }

    /// <summary>
    /// Throws when a non-empty segment file does not contain a valid journal header.
    /// Zero-length files are allowed (brand-new segment).
    /// </summary>
    /// <param name="fileLength">Total segment file length in bytes.</param>
    /// <param name="header">First <see cref="FileHeaderSize" /> bytes when the file is at least that long.</param>
    /// <exception cref="InvalidDataException">Thrown when the segment file is non-empty but does not contain a valid journal header.</exception>
    private static void ThrowIfSegmentHeaderInvalid(long fileLength, ReadOnlySpan<byte> header)
    {
        switch (fileLength)
        {
            case 0:
                return;
            case < FileHeaderSize:
                throw CreateTruncatedHeaderException();
            default:
                ThrowIfSegmentHeaderBytesInvalid(header);
                return;
        }
    }
}
