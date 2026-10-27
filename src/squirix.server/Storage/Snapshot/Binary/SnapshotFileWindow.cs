using System;
using System.Buffers;
using System.IO;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Snapshot.Binary;

/// <summary>Sequential reader over a snapshot file that serves exact byte ranges from one pooled window; a range larger than the window gets its own pooled buffer.</summary>
internal sealed class SnapshotFileWindow : IDisposable
{
    /// <summary>Smallest accepted window; it must hold a record header and the file footer.</summary>
    internal const int MinWindowSize = 16;

    private readonly SafeFileHandle _handle;
    private readonly byte[] _window;
    private int _end;
    private byte[]? _large;
    private bool _largeActive;
    private long _position;
    private long _readOffset;
    private int _start;

    internal SnapshotFileWindow(SafeFileHandle handle, int windowSize, long position)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(windowSize, MinWindowSize);
        _handle = handle;
        _window = ArrayPool<byte>.Shared.Rent(windowSize);
        _position = position;
        _readOffset = position;
    }

    /// <summary>Gets the file offset of the next unread byte.</summary>
    internal long Position => _position;

    public void Dispose()
    {
        // The buffers hold cached user data, so they are cleared before returning to the shared pool.
        ArrayPool<byte>.Shared.ReturnCleared(_window);
        if (_large != null)
        {
            ArrayPool<byte>.Shared.ReturnCleared(_large);
            _large = null;
        }
    }

    /// <summary>Consumes the bytes returned by the last successful <see cref="TryRead" />.</summary>
    /// <param name="count">The number of bytes to consume.</param>
    internal void Advance(int count)
    {
        _position += count;
        if (_largeActive)
        {
            _largeActive = false;
            _start = 0;
            _end = 0;
            _readOffset = _position;
            return;
        }

        _start += count;
    }

    /// <summary>Exposes the next <paramref name="count" /> bytes without consuming them; the span stays valid until the next call.</summary>
    /// <param name="count">The exact number of bytes needed.</param>
    /// <param name="bytes">The bytes on success.</param>
    /// <returns><see langword="false" /> when the file ends first.</returns>
    internal bool TryRead(int count, out ReadOnlySpan<byte> bytes)
    {
        if (count > _window.Length)
            return TryReadLarge(count, out bytes);

        if (_end - _start < count)
        {
            if (_start > 0)
            {
                Buffer.BlockCopy(_window, _start, _window, 0, _end - _start);
                _end -= _start;
                _start = 0;
            }

            while (_end < count)
            {
                var read = RandomAccess.Read(_handle, _window.AsSpan(_end), _readOffset);
                if (read == 0)
                {
                    bytes = default;
                    return false;
                }

                _end += read;
                _readOffset += read;
            }
        }

        bytes = _window.AsSpan(_start, count);
        return true;
    }

    private bool TryReadLarge(int count, out ReadOnlySpan<byte> bytes)
    {
        if (_large == null || _large.Length < count)
        {
            if (_large != null)
                ArrayPool<byte>.Shared.ReturnCleared(_large);

            _large = ArrayPool<byte>.Shared.Rent(count);
        }

        var buffered = _end - _start;
        Buffer.BlockCopy(_window, _start, _large, 0, buffered);
        var remaining = _large.AsSpan(buffered, count - buffered);
        var offset = _readOffset;
        if (!HandleEx.TryReadExact(_handle, remaining, offset))
        {
            bytes = default;
            return false;
        }

        _largeActive = true;
        bytes = _large.AsSpan(0, count);
        return true;
    }
}
