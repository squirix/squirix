using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.Read;

internal static class JournalFrameReader
{
    internal static JournalFrameReadResult ReadNext(SafeFileHandle handle, long frameOffset, out byte[]? rentedBuffer, out int payloadLength)
    {
        rentedBuffer = null;
        payloadLength = 0;

        Span<byte> lengthBytes = stackalloc byte[JournalFrameEnvelope.HeaderSize];
        var headerRead = ReadAtLeast(handle, lengthBytes, frameOffset);
        return headerRead switch
        {
            0 => new JournalFrameReadResult(JournalFrameReadStatus.EndOfFile, frameOffset),
            < JournalFrameEnvelope.HeaderSize => new JournalFrameReadResult(JournalFrameReadStatus.TruncatedHeader, frameOffset),
            _ => ReadNextFromValidStreamHeader(handle, frameOffset, lengthBytes, out rentedBuffer, out payloadLength),
        };
    }

    /// <summary>Reads up to <paramref name="buffer.Length" /> bytes at <paramref name="offset" /> and returns the number of bytes read.</summary>
    /// <param name="handle">The file handle to read from.</param>
    /// <param name="buffer">The span to fill partially.</param>
    /// <param name="offset">The file offset to start reading at.</param>
    /// <returns>The number of bytes read; zero at end of file.</returns>
    private static int ReadAtMost(SafeFileHandle handle, Span<byte> buffer, long offset)
    {
        var total = 0;
        while (total < buffer.Length)
        {
            var read = RandomAccess.Read(handle, buffer[total..], offset + total);
            if (read == 0)
                break;

            total += read;
        }

        return total;
    }

    private static int ReadAtLeast(SafeFileHandle handle, Span<byte> buffer, long offset)
    {
        var total = ReadAtMost(handle, buffer, offset);
        while (total > 0 && total < buffer.Length)
        {
            var next = RandomAccess.Read(handle, buffer[total..], offset + total);
            if (next == 0)
                break;

            total += next;
        }

        return total;
    }

    private static bool TryReadExact(SafeFileHandle handle, Span<byte> buffer, long offset) => ReadAtMost(handle, buffer, offset) == buffer.Length;

    private static JournalFrameReadResult ReadNextFromValidStreamHeader(
        SafeFileHandle handle,
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
            if (!TryReadExact(handle, payload, frameOffset + JournalFrameEnvelope.HeaderSize))
                return new JournalFrameReadResult(JournalFrameReadStatus.TruncatedPayload, frameOffset);

            Span<byte> checksumBytes = stackalloc byte[JournalFrameEnvelope.FooterSize];
            var checksumOffset = frameOffset + JournalFrameEnvelope.HeaderSize + payloadLength;
            if (!TryReadExact(handle, checksumBytes, checksumOffset))
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
}
