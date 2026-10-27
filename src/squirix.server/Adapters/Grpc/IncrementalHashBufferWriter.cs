using System;
using System.Buffers;
using System.Security.Cryptography;

namespace Squirix.Server.Adapters.Grpc;

/// <summary>
/// An <see cref="IBufferWriter{T}"/> that feeds every advanced byte into an <see cref="IncrementalHash"/> and keeps only one fixed-size pooled chunk,
/// so a message of any size is hashed without a message-sized buffer.
/// </summary>
/// <remarks>The writer owns the pooled chunk and returns it on <see cref="Dispose"/>; it does not own the hash. It is not thread-safe.</remarks>
internal sealed class IncrementalHashBufferWriter : IBufferWriter<byte>, IDisposable
{
    /// <summary>The size of the pooled chunk handed to the serializer; below the large object heap threshold.</summary>
    internal const int ChunkBytes = 16 * 1024;

    private readonly IncrementalHash _hash;
    private byte[]? _chunk;

    /// <summary>Initializes a new instance of the <see cref="IncrementalHashBufferWriter"/> class.</summary>
    /// <param name="hash">The hash that receives the written bytes.</param>
    internal IncrementalHashBufferWriter(IncrementalHash hash)
    {
        ArgumentNullException.ThrowIfNull(hash);
        _hash = hash;
        _chunk = ArrayPool<byte>.Shared.Rent(ChunkBytes);
    }

    /// <inheritdoc />
    public void Advance(int count)
    {
        var chunk = GetChunk();
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(count, chunk.Length);
        _hash.AppendData(chunk, 0, count);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        var chunk = _chunk;
        if (chunk == null)
            return;

        _chunk = null;
        ArrayPool<byte>.Shared.Return(chunk);
    }

    /// <inheritdoc />
    public Memory<byte> GetMemory(int sizeHint = 0) => GetChunk();

    /// <inheritdoc />
    public Span<byte> GetSpan(int sizeHint = 0) => GetChunk();

    private byte[] GetChunk()
    {
        ObjectDisposedException.ThrowIf(_chunk == null, this);
        return _chunk;
    }
}
