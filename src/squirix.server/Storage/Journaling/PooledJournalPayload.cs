using System;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Pool-backed journal payload buffer returned by <see cref="JournalEntryPayload.Encode" />.</summary>
/// <remarks>Callers must not retain the buffer after disposal.</remarks>
[Immutable]
internal sealed class PooledJournalPayload : IDisposable
{
    private readonly byte[] _buffer;
    private readonly int _length;
    private int _disposed;

    internal PooledJournalPayload(byte[] buffer, int length)
    {
        _buffer = buffer;
        _length = length;
    }

    internal ReadOnlyMemory<byte> Memory => _buffer.AsMemory(0, _length);

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        PayloadBuffer.Release(_buffer);
    }
}
