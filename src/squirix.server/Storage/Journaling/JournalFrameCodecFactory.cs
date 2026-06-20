using System;
using System.Buffers.Binary;
using System.Globalization;
using System.IO;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.JsonFramed;
using Squirix.Server.Storage.Journaling.Pipelined.Codec;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Resolves <see cref="IJournalFrameCodec" /> instances for journal read and write paths.</summary>
internal static class JournalFrameCodecFactory
{
    public static IJournalFrameCodec Binary => BinaryJournalCodec.Instance;

    public static IJournalFrameCodec JsonFramed => JsonFramedJournalCodec.Instance;

    public static IJournalFrameCodec DetectFromSegmentStart(ReadOnlySpan<byte> header, Stream stream, long fileLength)
    {
        EnsureSegmentHeaderSupported(header);
        if (fileLength <= header.Length)
            return DetectFromHeader(header);

        var frameStart = stream.Position;
        Span<byte> lengthBytes = stackalloc byte[JournalFrameEnvelope.HeaderSize];
        if (!StreamEx.TryReadExact(stream, lengthBytes))
        {
            stream.Position = frameStart;
            return JsonFramed;
        }

        var bodyLength = BinaryPrimitives.ReadInt32LittleEndian(lengthBytes);
        if (bodyLength <= 0 || frameStart + JournalFrameEnvelope.HeaderSize + bodyLength > fileLength)
        {
            stream.Position = frameStart;
            return JsonFramed;
        }

        Span<byte> firstBodyByte = stackalloc byte[1];
        if (!StreamEx.TryReadExact(stream, firstBodyByte))
        {
            stream.Position = frameStart;
            return JsonFramed;
        }

        stream.Position = frameStart;
        return firstBodyByte[0] switch
        {
            0x7B => JsonFramed,
            _ => Binary,
        };
    }

    public static void EnsureSegmentHeaderSupported(ReadOnlySpan<byte> header)
    {
        JournalFraming.ThrowIfSegmentHeaderInvalid(header.Length, header);
        EnsureSupportedFileVersion(header[4]);
    }

    private static IJournalFrameCodec DetectFromHeader(ReadOnlySpan<byte> header)
    {
        EnsureSegmentHeaderSupported(header);
        return JsonFramed;
    }

    private static void EnsureSupportedFileVersion(byte version)
    {
        if (version != JsonFramed.FileVersion && version != Binary.FileVersion)
        {
            throw new InvalidDataException($"unsupported journal segment file version {version.ToString(CultureInfo.InvariantCulture)}.");
        }
    }
}
