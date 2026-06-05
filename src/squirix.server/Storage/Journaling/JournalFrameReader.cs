using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

internal static class JournalFrameReader
{
    internal static JournalFrameReadResult ReadNext(Stream stream, long frameOffset, out byte[]? rentedBuffer, out int payloadLength)
    {
        rentedBuffer = null;
        payloadLength = 0;

        Span<byte> lengthBytes = stackalloc byte[JournalFraming.FrameHeaderSize];
        var headerRead = ReadHeader(stream, lengthBytes);
        return headerRead switch
        {
            0 => new JournalFrameReadResult(JournalFrameReadStatus.EndOfFile, frameOffset, frameOffset),
            < JournalFraming.FrameHeaderSize => new JournalFrameReadResult(JournalFrameReadStatus.TruncatedHeader, frameOffset, frameOffset),
            _ => ReadNextFromValidStreamHeader(stream, frameOffset, lengthBytes, out rentedBuffer, out payloadLength),
        };
    }

    internal static JournalFrameReadResult ReadNext(ReadOnlySpan<byte> data, long frameOffset)
    {
        return data.Length switch
        {
            0 => new JournalFrameReadResult(JournalFrameReadStatus.EndOfFile, frameOffset, frameOffset),
            < JournalFraming.FrameHeaderSize => new JournalFrameReadResult(JournalFrameReadStatus.TruncatedHeader, frameOffset, frameOffset),
            _ => ReadNextFromValidSpanHeader(data, frameOffset),
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

    private static JournalFrameReadResult ReadNextFromValidSpanHeader(ReadOnlySpan<byte> data, long frameOffset)
    {
        var declaredPayloadLength = BinaryPrimitives.ReadUInt32LittleEndian(data[..JournalFraming.FrameHeaderSize]);
        if (declaredPayloadLength > int.MaxValue)
            return new JournalFrameReadResult(JournalFrameReadStatus.OversizedFrame, frameOffset, frameOffset);

        var payloadLength = (int)declaredPayloadLength;
        if (data.Length - JournalFraming.FrameHeaderSize < payloadLength)
            return new JournalFrameReadResult(JournalFrameReadStatus.TruncatedPayload, frameOffset, frameOffset);

        var checksumOffset = JournalFraming.FrameHeaderSize + payloadLength;
        if (data.Length - checksumOffset < JournalFraming.FrameFooterSize)
        {
            return new JournalFrameReadResult(JournalFrameReadStatus.TruncatedChecksum, frameOffset, frameOffset);
        }

        var payload = data.Slice(JournalFraming.FrameHeaderSize, payloadLength);
        var expectedChecksum = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(checksumOffset, JournalFraming.FrameFooterSize));
        var actualChecksum = Crc32C.Compute(payload);
        if (actualChecksum != expectedChecksum)
        {
            return new JournalFrameReadResult(JournalFrameReadStatus.ChecksumMismatch, frameOffset, frameOffset);
        }

        var nextFrameOffset = frameOffset + JournalFraming.FrameHeaderSize + declaredPayloadLength + JournalFraming.FrameFooterSize;
        return new JournalFrameReadResult(JournalFrameReadStatus.Success, frameOffset, nextFrameOffset);
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

        var declaredPayloadLength = BinaryPrimitives.ReadUInt32LittleEndian(lengthBytes);
        if (declaredPayloadLength > int.MaxValue)
            return new JournalFrameReadResult(JournalFrameReadStatus.OversizedFrame, frameOffset, frameOffset);

        payloadLength = (int)declaredPayloadLength;
        var rented = ArrayPool<byte>.Shared.Rent(Math.Max(payloadLength, 1));
        try
        {
            var payload = rented.AsSpan(0, payloadLength);
            if (!TryReadExact(stream, payload))
                return new JournalFrameReadResult(JournalFrameReadStatus.TruncatedPayload, frameOffset, frameOffset);

            Span<byte> checksumBytes = stackalloc byte[JournalFraming.FrameFooterSize];
            if (!TryReadExact(stream, checksumBytes))
                return new JournalFrameReadResult(JournalFrameReadStatus.TruncatedChecksum, frameOffset, frameOffset);

            var expectedChecksum = BinaryPrimitives.ReadUInt32LittleEndian(checksumBytes);
            var actualChecksum = Crc32C.Compute(payload);
            if (actualChecksum != expectedChecksum)
            {
                return new JournalFrameReadResult(JournalFrameReadStatus.ChecksumMismatch, frameOffset, frameOffset);
            }

            rentedBuffer = rented;
            ArgumentNullException.ThrowIfNull(rentedBuffer);
            var nextFrameOffset = frameOffset + JournalFraming.FrameHeaderSize + declaredPayloadLength + JournalFraming.FrameFooterSize;
            return new JournalFrameReadResult(JournalFrameReadStatus.Success, frameOffset, nextFrameOffset);
        }
        finally
        {
            if (rentedBuffer is null)
                ArrayPool<byte>.Shared.Return(rented);
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
