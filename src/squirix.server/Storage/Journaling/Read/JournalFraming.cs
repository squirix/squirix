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

    internal const byte Version = 3;

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

    /// <summary>Determines whether the segment file at <paramref name="path" /> carries the journal magic under an unsupported format version.</summary>
    /// <param name="path">The segment file path.</param>
    /// <returns><see langword="true" /> when the file must be rejected as another format rather than repaired as a torn header.</returns>
    internal static bool HasUnsupportedVersion(string path)
    {
        using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, FileOptions.SequentialScan);
        Span<byte> header = stackalloc byte[FileHeaderSize];
        return HandleEx.TryReadExact(handle, header, 0) && IsUnsupportedVersionHeader(header);
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
        if (IsSegmentHeaderValid(header))
            return;

        throw IsUnsupportedVersionHeader(header)
            ? new InvalidDataException($"journal segment has file format version {header[4]}, but only version {Version} is supported.")
            : CreateInvalidHeaderException();
    }

    /// <summary>
    /// Determines whether a header carries the journal magic under a format version this build does not read. A zero version byte is what
    /// a header torn during creation of a pre-sized file looks like, so it is not a version mismatch.
    /// </summary>
    /// <param name="header">The header bytes.</param>
    /// <returns><see langword="true" /> when the segment is a whole file of another format, which must never be repaired or truncated.</returns>
    private static bool IsUnsupportedVersionHeader(ReadOnlySpan<byte> header) => header[..4].SequenceEqual(Magic) && header[4] != 0 && header[4] != Version;

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
