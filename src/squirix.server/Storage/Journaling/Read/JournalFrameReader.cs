using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.Read;

internal static class JournalFrameReader
{
    internal static JournalFrameReadResult ReadNext(Stream stream, long frameOffset, out byte[]? rentedBuffer, out int payloadLength)
    {
        rentedBuffer = null;
        payloadLength = 0;

        Span<byte> lengthBytes = stackalloc byte[JournalFrameEnvelope.HeaderSize];
        var headerRead = ReadHeader(stream, lengthBytes);
        return headerRead switch
        {
            0 => new JournalFrameReadResult(JournalFrameReadStatus.EndOfFile, frameOffset),
            < JournalFrameEnvelope.HeaderSize => new JournalFrameReadResult(JournalFrameReadStatus.TruncatedHeader, frameOffset),
            _ => ReadNextFromValidStreamHeader(stream, frameOffset, lengthBytes, out rentedBuffer, out payloadLength),
        };
    }

    private static int ReadHeader(Stream stream, Span<byte> buffer)
    {
        var read = stream.Read(buffer);
        if (read == 0)
            return 0;

        while (read < buffer.Length)
        {
            var next = stream.Read(buffer[read..]);
            if (next == 0)
                return read;

            read += next;
        }

        return read;
    }

    private static JournalFrameReadResult ReadNextFromValidStreamHeader(
        Stream stream,
        long frameOffset,
        ReadOnlySpan<byte> lengthBytes,
        out byte[]? rentedBuffer,
        out int payloadLength)
    {
        rentedBuffer = null;
        payloadLength = 0;

        var declaredPayloadLength = BinaryPrimitives.ReadInt32LittleEndian(lengthBytes);
        if (declaredPayloadLength < 0)
            return new JournalFrameReadResult(JournalFrameReadStatus.OversizedFrame, frameOffset);

        payloadLength = declaredPayloadLength;
        var rented = ArrayPool<byte>.Shared.Rent(Math.Max(payloadLength, 1));
        try
        {
            var payload = rented.AsSpan(0, payloadLength);
            if (!TryReadExact(stream, payload))
                return new JournalFrameReadResult(JournalFrameReadStatus.TruncatedPayload, frameOffset);

            Span<byte> checksumBytes = stackalloc byte[JournalFrameEnvelope.FooterSize];
            if (!TryReadExact(stream, checksumBytes))
                return new JournalFrameReadResult(JournalFrameReadStatus.TruncatedChecksum, frameOffset);

            var expectedChecksum = BinaryPrimitives.ReadUInt32LittleEndian(checksumBytes);
            var actualChecksum = Crc32C.Compute(payload);
            if (actualChecksum != expectedChecksum)
                return new JournalFrameReadResult(JournalFrameReadStatus.ChecksumMismatch, frameOffset);

            rentedBuffer = rented;
            ArgumentNullException.ThrowIfNull(rentedBuffer);
            var nextFrameOffset = frameOffset + JournalFrameEnvelope.TotalLength(payloadLength);
            return new JournalFrameReadResult(JournalFrameReadStatus.Success, nextFrameOffset);
        }
        finally
        {
            if (rentedBuffer == null)
                ArrayPool<byte>.Shared.ReturnCleared(rented);
        }
    }

    private static bool TryReadExact(Stream stream, Span<byte> buffer)
    {
        while (!buffer.IsEmpty)
        {
            var read = stream.Read(buffer);
            if (read == 0)
                return false;

            buffer = buffer[read..];
        }

        return true;
    }
}
