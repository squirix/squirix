using System;
using System.Buffers;

namespace Squirix.Server.Storage.Journaling;

internal sealed class PooledByteBufferWriter : IBufferWriter<byte>, IDisposable
{
    private const int DefaultInitialCapacity = 256;
    private byte[] _buffer;
    private bool _disposed;
    private int _index;

    public PooledByteBufferWriter(int initialCapacity = DefaultInitialCapacity)
    {
        _buffer = ArrayPool<byte>.Shared.Rent(Math.Max(1, initialCapacity));
    }

    public ReadOnlySpan<byte> WrittenSpan => _buffer.AsSpan(0, _index);

    public void Advance(int count)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (count < 0 || _index > _buffer.Length - count)
            throw new ArgumentOutOfRangeException(nameof(count));

        _index += count;
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = [];
        _index = 0;
        _disposed = true;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        EnsureCapacity(sizeHint);
        return _buffer.AsMemory(_index);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        EnsureCapacity(sizeHint);
        return _buffer.AsSpan(_index);
    }

    private void EnsureCapacity(int sizeHint)
    {
        var required = Math.Max(1, sizeHint);
        if (_buffer.Length - _index >= required)
            return;

        var newSize = Math.Max(_buffer.Length * 2, _index + required);
        var next = ArrayPool<byte>.Shared.Rent(newSize);
        _buffer.AsSpan(0, _index).CopyTo(next);
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = next;
    }
}
