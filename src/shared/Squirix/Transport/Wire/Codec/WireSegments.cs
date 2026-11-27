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
    private Memory<byte> _scratch;
    private byte[]? _current;
    private Memory<byte> _frame;
    private int _used;

    internal WireSegments(byte[]? scratch = null)
    {
        _scratch = scratch;
    }

    public void Advance(int count) => _used += count;

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        var capacity = Math.Max(1, sizeHint);
        var block = _current ?? _scratch;
        if (block.Length - _used >= capacity)
            return block[_used..];

        Seal();
        _current = ArrayPool<byte>.Shared.Rent(Math.Max(MetadataBytes, capacity));
        return _current;
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
        _scratch = default;
        _frame = default;
        _segments.Clear();
    }

    /// <summary>Reserves a complete ordinary frame for a single TLS write.</summary>
    /// <param name="length">Length of the encoded frame, including its header.</param>
    /// <returns>The writable frame, valid until disposal.</returns>
    internal Memory<byte> ReserveFrame(int length) => _frame = GetMemory(length)[..length];

    internal void Write(ReadOnlyMemory<byte> value)
    {
        Seal();
        _segments.Add(new Segment(value, null));
    }

    internal IEnumerable<ReadOnlyMemory<byte>> Chunks()
    {
        if (!_frame.IsEmpty)
        {
            yield return _frame;
            yield break;
        }

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
        if (_current != null)
        {
            _segments.Add(new Segment(_current.AsMemory(0, _used), _current));
            _current = null;
        }
        else if (_used != 0)
        {
            _segments.Add(new Segment(_scratch[.._used], null));
            _scratch = _scratch[_used..];
        }

        _used = 0;
    }

    private readonly record struct Segment(ReadOnlyMemory<byte> Bytes, byte[]? Buffer);
}
