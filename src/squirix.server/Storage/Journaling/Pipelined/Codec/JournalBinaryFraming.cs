using System;
using System.Buffers.Binary;
using Squirix.Server.Storage.Journaling.JsonFramed;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.Pipelined.Codec;

/// <summary>Binary frame body framing for Pipelined journal segments (SJRN file header version 1).</summary>
internal static class JournalBinaryFraming
{
    internal const int FileHeaderSize = JournalFraming.FileHeaderSize;

    internal const byte FileVersion = JournalFraming.Version;

    internal const int FrameHeaderSize = JournalFrameEnvelope.HeaderSize;

    public static int FrameTotalLength(int bodyLength) => FrameHeaderSize + bodyLength + JournalFrameEnvelope.FooterSize;

    public static void ValidateFileHeader(ReadOnlySpan<byte> header) => JournalFrameCodecFactory.EnsureSegmentHeaderSupported(header);

    public static void WriteFileHeader(Span<byte> destination)
    {
        JournalFraming.Magic.CopyTo(destination);
        destination[4] = BinaryJournalCodec.Instance.FileVersion;
    }

    public static void WriteFrame(Span<byte> frame, ReadOnlySpan<byte> body)
    {
        BinaryPrimitives.WriteInt32LittleEndian(frame, body.Length);
        body.CopyTo(frame[FrameHeaderSize..(FrameHeaderSize + body.Length)]);
        var crc = Crc32C.Compute(body);
        BinaryPrimitives.WriteUInt32LittleEndian(frame.Slice(FrameHeaderSize + body.Length, JournalFrameEnvelope.FooterSize), crc);
    }
}
