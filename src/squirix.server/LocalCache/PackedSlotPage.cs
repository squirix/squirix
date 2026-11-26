using System;
using System.Buffers.Binary;
using System.Text;
using System.Threading;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

/// <summary>Packs a slot’s UTF-8 key and typed value into one immutable byte range. Only the writer allocates or relocates them.</summary>
/// <typeparam name="T">The stored value type.</typeparam>
internal sealed class PackedSlotPage<T>
{
    private const int SmallestPage = 256;
    private const int LargestRecord = 16384;
    private readonly string _namespace;
    private Layout _layout;
    private string?[]? _namespaces;
    private SlotPayload<T>[]? _fallback;
    private int _fallbackCount;
    private int _end;
    private int _live;

    private PackedSlotPage(Layout layout, string primaryNamespace)
    {
        _namespace = primaryNamespace;
        _layout = layout;
    }

    internal static PackedSlotPage<T> Seal(SlotPayload<T>[] plain, int offset, SlotPayload<T> incoming)
    {
        Span<int> sizes = stackalloc int[plain.Length];
        Span<int> keyLengths = stackalloc int[plain.Length];
        MeasureRecords(plain, offset, incoming, sizes, keyLengths);

        var layout = new Layout(sizes);
        var packed = new PackedSlotPage<T>(layout, offset == 0 ? incoming.Key.Namespace : plain[0].Key.Namespace);
        for (var i = 0; i < plain.Length; i++)
        {
            var payload = i == offset ? incoming : plain[i];
            if (sizes[i] == 0)
                packed.StoreFallback(i, payload);
            else
                packed.Encode(i, payload, keyLengths[i], layout.Bytes.AsSpan(packed._end, sizes[i]));
            packed._end += sizes[i];
        }

        packed._live = packed._end;
        return packed;
    }

    internal SlotPayload<T> Read(int slot)
    {
        // Buffer and range directory belong to the same generation across relocation.
        var layout = Volatile.Read(ref _layout);
        var range = layout.Read(slot);
        if (range == 0)
            return Volatile.Read(ref _fallback)?[slot] ?? default;
        var ns = Volatile.Read(ref _namespaces)?[slot] ?? _namespace;
        var record = StoredValue.FromPage(StoredValueKind.Binary, layout.Bytes, Offset(range), Size(range), true);
        return new SlotPayload<T>(new CacheKey(ns, string.Empty), SlotPayload<T>.AsGeneric(record));
    }

    internal void Write(int slot, SlotPayload<T> payload)
    {
        var original = SlotPayload<T>.AsStored(payload.Value);
        var size = RecordSize(payload.Key.Key, in original, out var keyLength);
        var oldSize = Size(_layout.Read(slot));
        if (size == 0)
        {
            PrepareRemove(slot);
            StoreFallback(slot, payload);
            _layout.Set(slot, 0);
            _namespaces?[slot] = null;
            _live -= oldSize;
            return;
        }

        if (size > _layout.Bytes.Length - _end)
            Rebuild(_live + size);

        _layout.PrepareWrite(size);
        Encode(slot, payload, keyLength, _layout.Bytes.AsSpan(_end, size));
        _layout.Set(slot, Range(_end, size));
        _end += size;
        _live += size - oldSize;
        ClearFallback(slot);
    }

    internal void Remove(int slot)
    {
        _live -= Size(_layout.Read(slot));
        _layout.Set(slot, 0);
        ClearFallback(slot);
        _namespaces?[slot] = null;
    }

    /// <summary>Reclaims sparse storage before unlinking so that removal itself needs no allocation.</summary>
    /// <param name="slot">The occupant scheduled for removal.</param>
    internal void PrepareRemove(int slot)
    {
        var remaining = _live - Size(_layout.Read(slot));
        if (_layout.Bytes.Length > SmallestPage && remaining < _layout.Bytes.Length / 4 && _live < _layout.Bytes.Length / 2)
            Rebuild(_live);
        _layout.PrepareWrite(0);
    }

    private static long Range(int offset, int size) => (long.CreateChecked(offset) << 32) | uint.CreateChecked(size);

    private static int Size(long range) => int.CreateTruncating(range);

    private static int Offset(long range) => int.CreateChecked(range >> 32);

    private static void MeasureRecords(SlotPayload<T>[] plain, int offset, SlotPayload<T> incoming, Span<int> sizes, Span<int> keyLengths)
    {
        for (var i = 0; i < plain.Length; i++)
        {
            var payload = i == offset ? incoming : plain[i];
            sizes[i] = RecordSize(payload.Key.Key, SlotPayload<T>.AsStored(payload.Value), out keyLengths[i]);
        }
    }

    private static int RecordSize(string key, in StoredValue original, out int keyLength)
    {
        keyLength = KeyLength(key);
        if (keyLength < 0 || original.EstimatedPayloadBytes > LargestRecord)
            return 0;
        var size = sizeof(ushort) + keyLength + CompactPageValue.EncodedLength(in original);
        return size <= LargestRecord ? size : 0;
    }

    private static int KeyLength(string key)
    {
        if (key.Length > LargestRecord)
            return -1;
        try
        {
            return CompactPageValue.KeyEncoding.GetByteCount(key);
        }
        catch (EncoderFallbackException)
        {
            // Preserve ordinal keys containing an unpaired UTF-16 surrogate in the ordinary slot representation.
            return -1;
        }
    }

    private void Encode(int slot, SlotPayload<T> payload, int keyLength, Span<byte> record)
    {
        var primary = string.Equals(_namespace, payload.Key.Namespace, StringComparison.Ordinal);
        if (!primary && _namespaces == null)
            Volatile.Write(ref _namespaces, new string?[_layout.Slots]);

        BinaryPrimitives.WriteUInt16LittleEndian(record, ushort.CreateChecked(keyLength));
        _ = CompactPageValue.KeyEncoding.GetBytes(payload.Key.Key, record[sizeof(ushort)..]);
        var value = SlotPayload<T>.AsStored(payload.Value);
        CompactPageValue.Write(record[(sizeof(ushort) + keyLength)..], in value);
        _namespaces?[slot] = primary ? null : payload.Key.Namespace;
    }

    private void StoreFallback(int slot, SlotPayload<T> payload)
    {
        var fallback = _fallback;
        if (fallback == null)
        {
            fallback = new SlotPayload<T>[_layout.Slots];
            Volatile.Write(ref _fallback, fallback);
        }

        if (fallback[slot].Key.Key == null)
            _fallbackCount++;
        fallback[slot] = payload;
    }

    private void ClearFallback(int slot)
    {
        if (_fallback?[slot].Key.Key == null)
            return;
        _fallback[slot] = default;
        if (--_fallbackCount == 0)
            Volatile.Write(ref _fallback, null);
    }

    private void Rebuild(int capacity)
    {
        var old = _layout;
        var next = new Layout(old.Slots, capacity, old.FixedSize);
        var end = 0;
        for (var i = 0; i < old.Slots; i++)
        {
            var range = old.Read(i);
            if (range == 0)
                continue;
            var size = Size(range);
            old.Bytes.AsSpan(Offset(range), size).CopyTo(next.Bytes.AsSpan(end));
            next.Set(i, Range(end, size));
            end += size;
        }

        _end = end;
        Volatile.Write(ref _layout, next);
    }

    /// <summary>Sealed pages derive immutable ranges from record lengths. On mutation, equal-length records need only a position;
    /// arbitrary replacements use atomic offset/length pairs. Readers may retain slices from any published buffer.</summary>
    private sealed class Layout
    {
        private readonly int _stride;
        private readonly int[]? _ends;
        private ushort[]? _positions;
        private long[]? _ranges;

        internal Layout(ReadOnlySpan<int> sizes)
        {
            Slots = sizes.Length;
            _stride = sizes[0];
            var capacity = Slots * _stride;
            if (sizes.ContainsAnyExcept(_stride))
            {
                _ends = new int[Slots];
                capacity = 0;
                for (var i = 0; i < Slots; i++)
                {
                    capacity += sizes[i];
                    _ends[i] = capacity;
                }
            }

            Bytes = GC.AllocateUninitializedArray<byte>(capacity);
        }

        internal Layout(int slots, int capacity, int stride)
        {
            Slots = slots;
            _stride = stride;
            if (stride > 0)
                _positions = new ushort[slots];
            else
                _ranges = new long[slots];
            Bytes = GC.AllocateUninitializedArray<byte>(Math.Max(SmallestPage, capacity + (capacity / 4)));
        }

        internal int Slots { get; }

        internal byte[] Bytes { get; }

        internal int FixedSize => _ends == null && _ranges == null ? _stride : 0;

        internal long Read(int slot)
        {
            if (Volatile.Read(ref _ranges) is { } ranges)
                return Volatile.Read(ref ranges[slot]);
            if (Volatile.Read(ref _positions) is not { } positions)
                return InitialRange(slot);

            var position = Volatile.Read(ref positions[slot]);
            return position == 0 ? 0 : Range((position - 1) * _stride, _stride);
        }

        internal void Set(int slot, long range)
        {
            if (_ranges is { } ranges)
                Volatile.Write(ref ranges[slot], range);
            else
                Volatile.Write(ref _positions![slot], ushort.CreateChecked(range == 0 ? 0 : (Offset(range) / _stride) + 1));
        }

        internal void PrepareWrite(int size)
        {
            if (_ranges != null)
                return;

            if (FixedSize > 0 && (size == 0 || size == _stride))
            {
                if (_positions != null)
                    return;

                var positions = new ushort[Slots];
                for (var i = 0; i < positions.Length; i++)
                    positions[i] = ushort.CreateChecked(i + 1);
                Volatile.Write(ref _positions, positions);
                return;
            }

            var ranges = new long[Slots];
            for (var i = 0; i < ranges.Length; i++)
                ranges[i] = Read(i);
            Volatile.Write(ref _ranges, ranges);
        }

        private long InitialRange(int slot)
        {
            if (_ends == null)
                return Range(slot * _stride, _stride);

            var start = slot == 0 ? 0 : _ends[slot - 1];
            var length = _ends[slot] - start;
            return length == 0 ? 0 : Range(start, length);
        }
    }
}
