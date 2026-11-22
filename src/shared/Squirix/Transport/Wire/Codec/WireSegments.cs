using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Transport.Wire.Codec;

/// <summary>Buffers encoded metadata in small blocks and borrows existing value bytes until the send completes.</summary>
internal sealed class WireSegments : IBufferWriter<byte>, IDisposable
{
    private const int ChunkBytes = 1024 * 1024;
    private const int MetadataBytes = 16 * 1024;

    private readonly List<Segment> _segments = [];
    private byte[]? _current;
    private int _used;

    public void Advance(int count) => _used += count;

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        var capacity = Math.Max(1, sizeHint);
        if (_current == null || _current.Length - _used < capacity)
        {
            Seal();
            _current = ArrayPool<byte>.Shared.Rent(Math.Max(MetadataBytes, capacity));
        }

        return _current.AsMemory(_used);
    }

    public Span<byte> GetSpan(int sizeHint = 0) => GetMemory(sizeHint).Span;

    public void Dispose()
    {
        if (_current != null)
            ArrayPool<byte>.Shared.Return(_current, true);

        for (var i = 0; i < _segments.Count; i++)
        {
            if (_segments[i].Buffer is { } buffer)
                ArrayPool<byte>.Shared.Return(buffer, true);
        }

        _current = null;
        _used = 0;
        _segments.Clear();
    }

    internal void Write(ReadOnlyMemory<byte> value)
    {
        Seal();
        _segments.Add(new Segment(value, null));
    }

    internal IEnumerable<ReadOnlyMemory<byte>> Chunks()
    {
        Seal();
        for (var i = 0; i < _segments.Count; i++)
        {
            var remaining = _segments[i].Bytes;
            while (!remaining.IsEmpty)
            {
                var count = Math.Min(ChunkBytes, remaining.Length);
                yield return remaining[..count];
                remaining = remaining[count..];
            }
        }
    }

    internal async ValueTask SendAsync(Stream stream, CancellationToken cancellationToken)
    {
        foreach (var chunk in Chunks())
            await stream.WriteAsync(chunk, cancellationToken).ConfigureAwait(false);
    }

    private void Seal()
    {
        if (_current == null)
            return;

        _segments.Add(new Segment(_current.AsMemory(0, _used), _current));
        _current = null;
        _used = 0;
    }

    private readonly record struct Segment(ReadOnlyMemory<byte> Bytes, byte[]? Buffer);
}
