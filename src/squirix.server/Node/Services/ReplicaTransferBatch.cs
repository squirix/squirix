using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using Squirix.Server.Cluster.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>The entries of one transfer message, built in rented buffers that are reused from message to message.</summary>
/// <remarks>
/// The entry array and the buffers that hold the key bytes come from the shared pools and go back when the batch is disposed, so a transfer
/// allocates nothing per entry beyond its payload. The memory handed to a message stays unchanged until <see cref="Clear" /> runs, which the
/// caller does only after the gateway call returned. Not thread-safe: one transfer fills it one message at a time.
/// </remarks>
internal sealed class ReplicaTransferBatch : IDisposable
{
    /// <summary>The most entries one message carries.</summary>
    internal const int MaxEntries = 512;

    private const int KeyBufferBytes = 64 * 1024;

    private readonly List<byte[]> _buffers = [];
    private readonly ReplicaStateEntry[] _entries = ArrayPool<ReplicaStateEntry>.Shared.Rent(MaxEntries);
    private int _bufferIndex = -1;
    private int _used;

    /// <summary>Gets the bytes the batch counts against the message budget.</summary>
    internal long Bytes { get; private set; }

    /// <summary>Gets the number of entries in the batch.</summary>
    internal int Count { get; private set; }

    /// <summary>Gets the entries of the batch.</summary>
    internal ReadOnlyMemory<ReplicaStateEntry> Entries => _entries.AsMemory(0, Count);

    /// <inheritdoc />
    public void Dispose()
    {
        ArrayPool<ReplicaStateEntry>.Shared.Return(_entries, true);
        for (var i = 0; i < _buffers.Count; i++)
            ArrayPool<byte>.Shared.Return(_buffers[i]);

        _buffers.Clear();
    }

    /// <summary>Counts the bytes of an entry against the message budget: the UTF-8 bytes of its cache name, key and payload plus the framing of the wire.</summary>
    /// <param name="cacheName">The cache the key belongs to.</param>
    /// <param name="key">The key.</param>
    /// <param name="payloadLength">The length of the encoded payload.</param>
    /// <returns>The bytes the entry takes in a message.</returns>
    internal static long SizeOf(string cacheName, string key, int payloadLength) =>
        Encoding.UTF8.GetByteCount(cacheName) + Encoding.UTF8.GetByteCount(key) + payloadLength + ReplicaTransferOptions.EntryFramingBytes;

    /// <summary>Adds an entry; the caller checked <see cref="Fits" /> first.</summary>
    /// <param name="cacheName">The cache the key belongs to.</param>
    /// <param name="key">The key.</param>
    /// <param name="payload">The encoded payload.</param>
    /// <param name="expiresUtcTicks">The absolute deadline in UTC ticks; zero for none.</param>
    /// <param name="size">The bytes of the entry as <see cref="SizeOf" /> counted them.</param>
    /// <returns>The entry as the message carries it.</returns>
    internal ReplicaStateEntry Add(string cacheName, string key, byte[] payload, long expiresUtcTicks, long size)
    {
        var slot = Reserve(Encoding.UTF8.GetByteCount(key));
        _ = Encoding.UTF8.GetBytes(key, slot.Span);
        var entry = new ReplicaStateEntry(cacheName, slot, payload, expiresUtcTicks);
        _entries[Count++] = entry;
        Bytes += size;
        return entry;
    }

    /// <summary>Forgets the entries and keeps the buffers for the next message.</summary>
    internal void Clear()
    {
        Array.Clear(_entries, 0, Count);
        Count = 0;
        Bytes = 0;
        _bufferIndex = _buffers.Count == 0 ? -1 : 0;
        _used = 0;
    }

    /// <summary>Tells whether an entry fits the message: an empty batch takes any entry, so one larger than the budget travels alone.</summary>
    /// <param name="size">The bytes of the entry as <see cref="SizeOf" /> counted them.</param>
    /// <returns><see langword="true" /> when the entry may join the batch.</returns>
    internal bool Fits(long size) => Count == 0 || (Count < MaxEntries && Bytes + size <= ReplicaTransferOptions.MessageBudgetBytes);

    private static byte[] Grow(byte[] small, int length)
    {
        ArrayPool<byte>.Shared.Return(small);
        return ArrayPool<byte>.Shared.Rent(length);
    }

    private Memory<byte> Reserve(int length)
    {
        if (_bufferIndex < 0 || _buffers[_bufferIndex].Length - _used < length)
        {
            _bufferIndex++;
            _used = 0;
            if (_bufferIndex == _buffers.Count)
                _buffers.Add(ArrayPool<byte>.Shared.Rent(Math.Max(KeyBufferBytes, length)));
            else if (_buffers[_bufferIndex].Length < length)
                _buffers[_bufferIndex] = Grow(_buffers[_bufferIndex], length);
        }

        var slot = _buffers[_bufferIndex].AsMemory(_used, length);
        _used += length;
        return slot;
    }
}
