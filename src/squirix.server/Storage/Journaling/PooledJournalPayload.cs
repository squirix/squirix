using System;
using System.Buffers;
using System.Threading;
using Squirix.Attributes;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Pool-backed journal payload buffer returned by <see cref="JournalEntryPayload.Encode" />.</summary>
/// <remarks>Dispose returns the underlying array to the shared <see cref="ArrayPool{T}" />; callers must not retain the buffer after disposal.</remarks>
[Immutable]
internal sealed class PooledJournalPayload : IDisposable
{
    private readonly int _length;
    private readonly byte[] _buffer;
    private int _disposed;

    internal PooledJournalPayload(byte[] buffer, int length)
    {
        _buffer = buffer;
        _length = length;
    }

    internal ReadOnlyMemory<byte> Memory => _buffer.AsMemory(0, _length);

    internal ReadOnlySpan<byte> Span => _buffer.AsSpan(0, _length);

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) is not 0)
            return;
        ArrayPool<byte>.Shared.Return(_buffer);
    }
}
