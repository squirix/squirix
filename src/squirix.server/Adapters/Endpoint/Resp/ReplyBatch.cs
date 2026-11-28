using System;
using System.Buffers;

namespace Squirix.Server.Adapters.Endpoint.Resp;

internal sealed class ReplyBatch : IBufferWriter<byte>, IDisposable
{
    private byte[] _buffer = ArrayPool<byte>.Shared.Rent(4096);

    internal int WrittenCount { get; private set; }

    internal ReadOnlyMemory<byte> WrittenMemory => _buffer.AsMemory(0, WrittenCount);

    public void Advance(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(count, _buffer.Length - WrittenCount);
        WrittenCount += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsMemory(WrittenCount);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsSpan(WrittenCount);
    }

    public void Dispose() => ArrayPool<byte>.Shared.Return(_buffer);

    internal void Clear() => WrittenCount = 0;

    private void EnsureCapacity(int sizeHint)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(sizeHint);
        var needed = Math.Max(sizeHint, 1);
        if (needed <= _buffer.Length - WrittenCount)
            return;

        var next = ArrayPool<byte>.Shared.Rent(checked(WrittenCount + needed));
        _buffer.AsSpan(0, WrittenCount).CopyTo(next);
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = next;
    }
}
