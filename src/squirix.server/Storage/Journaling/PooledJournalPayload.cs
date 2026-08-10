using System;
using System.Buffers;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Pool-backed journal payload buffer returned by <see cref="JournalEntryPayload.Encode" />.</summary>
/// <remarks>Dispose returns the underlying array to the shared <see cref="ArrayPool{T}" />; callers must not retain the buffer after disposal.</remarks>
internal struct PooledJournalPayload : IDisposable
{
    private readonly int _length;
    private byte[]? _buffer;

    internal PooledJournalPayload(byte[] buffer, int length)
    {
        _buffer = buffer;
        _length = length;
    }

    internal readonly ReadOnlyMemory<byte> Memory => _buffer!.AsMemory(0, _length);

    internal readonly ReadOnlySpan<byte> Span => _buffer!.AsSpan(0, _length);

    public void Dispose()
    {
        if (_buffer is null)
            return;
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = null;
    }
}
