using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.Read;

/// <summary>
/// Reads journal frames from a file of known length through a pooled read window, so walking a segment costs one read per window
/// instead of several per frame. Results are identical to <see cref="JournalFrameReader.ReadNext" /> for the same file.
/// The reader never reads at or past the known file length and is not thread-safe.
/// </summary>
/// <remarks>
/// A successful frame's payload is copied into a reader-owned buffer that starts at index zero; that buffer is valid only until the next
/// <see cref="ReadNext" /> call or <see cref="Dispose" />. Nothing returned aliases the window, so refilling it never invalidates a payload.
/// </remarks>
[Mutable]
internal sealed class JournalFrameWindowReader : IDisposable
{
    internal const int DefaultWindowSize = 256 * 1024;

    private readonly long _fileLength;
    private readonly SafeFileHandle _handle;
    private readonly byte[] _window;
    private readonly int _windowSize;
    private int _disposed;
    private byte[]? _payload;
    private int _windowLength;
    private long _windowStart;

    internal JournalFrameWindowReader(SafeFileHandle handle, long fileLength, int windowSize = DefaultWindowSize)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(windowSize, JournalFrameEnvelope.HeaderSize);
        _handle = handle;
        _fileLength = fileLength;
        _windowSize = windowSize;
        _window = ArrayPool<byte>.Shared.Rent(windowSize);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        ArrayPool<byte>.Shared.ReturnCleared(_window);
        if (_payload != null)
        {
            ArrayPool<byte>.Shared.ReturnCleared(_payload);
            _payload = null;
        }
    }

    /// <summary>Reads the frame that starts at <paramref name="frameOffset" />.</summary>
    /// <param name="frameOffset">The file offset of the frame length field.</param>
    /// <param name="payloadBuffer">The payload buffer on success, otherwise null; valid until the next call or disposal.</param>
    /// <param name="payloadLength">The declared payload length when the length field was accepted, otherwise zero.</param>
    /// <returns>The frame status and the offset of the next frame on success, or the frame offset otherwise.</returns>
    internal JournalFrameReadResult ReadNext(long frameOffset, out byte[]? payloadBuffer, out int payloadLength)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        payloadBuffer = null;
        payloadLength = 0;

        var headerRead = Fill(frameOffset, JournalFrameEnvelope.HeaderSize);
        return headerRead switch
        {
            0 => new JournalFrameReadResult(JournalFrameReadStatus.EndOfFile, frameOffset),
            < JournalFrameEnvelope.HeaderSize => new JournalFrameReadResult(JournalFrameReadStatus.TruncatedHeader, frameOffset),
            _ => ReadFrame(frameOffset, out payloadBuffer, out payloadLength),
        };
    }

    private static bool ChecksumMatches(ReadOnlySpan<byte> payload, ReadOnlySpan<byte> checksumBytes) =>
        Crc32C.Compute(payload) == BinaryPrimitives.ReadUInt32LittleEndian(checksumBytes);

    private JournalFrameReadResult ReadFrame(long frameOffset, out byte[]? payloadBuffer, out int payloadLength)
    {
        payloadBuffer = null;
        payloadLength = 0;
        var declared = BinaryPrimitives.ReadInt32LittleEndian(_window);
        if (declared is < 0 or > JournalSegmentLimits.MaxFramePayloadBytes)
            return new JournalFrameReadResult(JournalFrameReadStatus.OversizedFrame, frameOffset);

        payloadLength = declared;
        return (frameOffset + JournalFrameEnvelope.HeaderSize + declared > _fileLength, JournalFrameEnvelope.TotalLength(declared) <= _windowSize) switch
        {
            (true, _) => new JournalFrameReadResult(JournalFrameReadStatus.TruncatedPayload, frameOffset),
            (false, true) => ReadFrameFromWindow(frameOffset, declared, out payloadBuffer),
            (false, false) => ReadLargeFrame(frameOffset, declared, out payloadBuffer),
        };
    }

    private byte[] EnsurePayloadBuffer(int length)
    {
        if (_payload != null && _payload.Length >= length)
            return _payload;

        if (_payload != null)
            ArrayPool<byte>.Shared.ReturnCleared(_payload);

        _payload = ArrayPool<byte>.Shared.Rent(Math.Max(length, 1));
        return _payload;
    }

    /// <summary>Starts the window at <paramref name="offset" /> and returns how many of the first <paramref name="count" /> bytes exist there.</summary>
    /// <param name="offset">The file offset the window must start at.</param>
    /// <param name="count">The number of bytes wanted; not greater than the window size.</param>
    /// <returns>The number of bytes available at <paramref name="offset" />, at most <paramref name="count" />, bounded by the file length.</returns>
    private int Fill(long offset, int count)
    {
        var wanted = int.CreateTruncating(Math.Min(count, Math.Max(_fileLength - offset, 0)));
        if (wanted == 0)
            return 0;

        SlideTo(offset);
        while (_windowLength < wanted)
        {
            var space = int.CreateTruncating(Math.Min(_windowSize - _windowLength, _fileLength - (_windowStart + _windowLength)));
            var read = RandomAccess.Read(_handle, _window.AsSpan(_windowLength, space), _windowStart + _windowLength);
            if (read == 0)
                break;

            _windowLength += read;
        }

        return Math.Min(wanted, _windowLength);
    }

    private JournalFrameReadResult ReadFrameFromWindow(long frameOffset, int declared, out byte[]? payloadBuffer)
    {
        payloadBuffer = null;
        var total = JournalFrameEnvelope.TotalLength(declared);
        var available = Fill(frameOffset, total);
        if (available < JournalFrameEnvelope.HeaderSize + declared)
            return new JournalFrameReadResult(JournalFrameReadStatus.TruncatedPayload, frameOffset);

        if (available < total)
            return new JournalFrameReadResult(JournalFrameReadStatus.TruncatedChecksum, frameOffset);

        var payload = _window.AsSpan(JournalFrameEnvelope.HeaderSize, declared);
        if (!ChecksumMatches(payload, _window.AsSpan(JournalFrameEnvelope.HeaderSize + declared, JournalFrameEnvelope.FooterSize)))
            return new JournalFrameReadResult(JournalFrameReadStatus.ChecksumMismatch, frameOffset);

        var buffer = EnsurePayloadBuffer(declared);
        payload.CopyTo(buffer);
        payloadBuffer = buffer;
        return new JournalFrameReadResult(JournalFrameReadStatus.Success, frameOffset + total);
    }

    /// <summary>Reads a frame that does not fit the window straight into the payload buffer.</summary>
    /// <param name="frameOffset">The file offset of the frame length field.</param>
    /// <param name="declared">The accepted declared payload length.</param>
    /// <param name="payloadBuffer">The payload buffer on success, otherwise null.</param>
    /// <returns>The frame status and the offset of the next frame on success, or the frame offset otherwise.</returns>
    private JournalFrameReadResult ReadLargeFrame(long frameOffset, int declared, out byte[]? payloadBuffer)
    {
        payloadBuffer = null;
        var buffer = EnsurePayloadBuffer(declared);
        var payload = buffer.AsSpan(0, declared);
        var payloadOffset = frameOffset + JournalFrameEnvelope.HeaderSize;
        if (!TryReadExact(payload, payloadOffset))
            return new JournalFrameReadResult(JournalFrameReadStatus.TruncatedPayload, frameOffset);

        Span<byte> checksumBytes = stackalloc byte[JournalFrameEnvelope.FooterSize];
        if (!TryReadExact(checksumBytes, payloadOffset + declared))
            return new JournalFrameReadResult(JournalFrameReadStatus.TruncatedChecksum, frameOffset);

        if (!ChecksumMatches(payload, checksumBytes))
            return new JournalFrameReadResult(JournalFrameReadStatus.ChecksumMismatch, frameOffset);

        payloadBuffer = buffer;
        return new JournalFrameReadResult(JournalFrameReadStatus.Success, payloadOffset + declared + JournalFrameEnvelope.FooterSize);
    }

    /// <summary>Moves the window start to <paramref name="offset" />, keeping the bytes already read when the offset lies inside the window.</summary>
    /// <param name="offset">The file offset the window must start at.</param>
    private void SlideTo(long offset)
    {
        var windowEnd = _windowStart + _windowLength;
        if (offset < _windowStart || offset > windowEnd)
        {
            _windowStart = offset;
            _windowLength = 0;
            return;
        }

        var shift = int.CreateTruncating(offset - _windowStart);
        if (shift == 0)
            return;

        _window.AsSpan(shift, _windowLength - shift).CopyTo(_window);
        _windowLength -= shift;
        _windowStart = offset;
    }

    private bool TryReadExact(Span<byte> buffer, long offset)
    {
        if (offset + buffer.Length > _fileLength)
            return false;

        var total = 0;
        while (total < buffer.Length)
        {
            var read = RandomAccess.Read(_handle, buffer[total..], offset + total);
            if (read == 0)
                break;

            total += read;
        }

        return total == buffer.Length;
    }
}
