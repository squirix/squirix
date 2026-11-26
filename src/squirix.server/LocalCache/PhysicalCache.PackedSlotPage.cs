using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

internal sealed partial class PhysicalCache<T>
{
    /// <summary>Packs a slot page's keys and typed values into immutable byte ranges. Only the writer allocates or relocates them.</summary>
    private sealed class PackedSlotPage
    {
        private const int SmallestPage = 256;
        private const int LargestRecord = 16384;
        private readonly string _namespace;
        private Layout _layout;
        private string?[]? _namespaces;
        private Payload[]? _fallback;
        private int _fallbackCount;
        private int _end;
        private int _live;

        private PackedSlotPage(int slots, int capacity, string primaryNamespace)
        {
            _namespace = primaryNamespace;
            _layout = new Layout(slots, capacity);
        }

        internal static bool Typed => typeof(T) == typeof(StoredValue);

        private static Encoding Utf8 => CompactPageValue.KeyEncoding;

        internal static PackedSlotPage Seal(SlotPage page, Payload[] plain)
        {
            var needed = 0;
            string? primaryNamespace = null;
            for (var i = 0; i < plain.Length; i++)
            {
                if (page[i].Lifetime <= 0)
                    continue;
                primaryNamespace ??= plain[i].Key.Namespace;
                needed += RecordSize(plain[i].Key.Key, AsStored(plain[i].Value), out _);
            }

            var packed = new PackedSlotPage(plain.Length, needed, primaryNamespace ?? string.Empty);
            for (var i = 0; i < plain.Length; i++)
            {
                if (page[i].Lifetime > 0)
                    packed.Write(i, plain[i]);
            }

            return packed;
        }

        internal static StoredValue AsStored(T? value) => Unsafe.As<T?, StoredValue>(ref value);

        internal static T? Unpack(T? raw)
        {
            if (!Typed)
                return raw;
            var value = AsStored(raw);
            if (!value.IsPageRecord)
                return raw;
            var keyLength = BinaryPrimitives.ReadUInt16LittleEndian(value.Memory.Span);
            return AsGeneric(CompactPageValue.ReadRecord(in value, sizeof(ushort) + keyLength));
        }

        internal static string KeyText(T? raw) => Utf8.GetString(KeyBytes(raw));

        internal static bool Matches(T? raw, string key)
        {
            var saved = KeyBytes(raw);
            if (saved.Length == key.Length && Ascii.Equals(saved, key))
                return true;
            if (key.Length > saved.Length)
                return false;
            Span<char> chars = key.Length <= 256 ? stackalloc char[key.Length] : new char[key.Length];
            return System.Text.Unicode.Utf8.ToUtf16(saved, chars, out _, out var written) == System.Buffers.OperationStatus.Done &&
                chars[..written].SequenceEqual(key);
        }

        internal Payload Read(int slot)
        {
            // Buffer and range directory belong to the same generation across relocation.
            var layout = Volatile.Read(ref _layout);
            var range = Volatile.Read(ref layout.Ranges[slot]);
            if (range == 0)
                return Volatile.Read(ref _fallback)?[slot] ?? default;
            var ns = Volatile.Read(ref _namespaces)?[slot] ?? _namespace;
            var record = StoredValue.FromPage(StoredValueKind.Binary, layout.Bytes, Offset(range), Size(range), true);
            return new Payload(new CacheKey(ns, string.Empty), AsGeneric(record));
        }

        internal void Write(int slot, Payload payload)
        {
            var original = AsStored(payload.Value);
            var size = RecordSize(payload.Key.Key, in original, out var keyLength);
            var oldSize = Size(_layout.Ranges[slot]);
            if (size == 0)
            {
                PrepareRemove(slot);
                var fallback = _fallback;
                if (fallback == null)
                {
                    fallback = new Payload[_layout.Ranges.Length];
                    Volatile.Write(ref _fallback, fallback);
                }

                if (fallback[slot].Key.Key == null)
                    _fallbackCount++;
                fallback[slot] = payload;
                Volatile.Write(ref _layout.Ranges[slot], 0);
                _namespaces?[slot] = null;
                _live -= oldSize;
                return;
            }

            if (!string.Equals(_namespace, payload.Key.Namespace, StringComparison.Ordinal) && _namespaces == null)
                Volatile.Write(ref _namespaces, new string?[_layout.Ranges.Length]);
            if (size > _layout.Bytes.Length - _end)
                Rebuild(_live + size);

            var record = _layout.Bytes.AsSpan(_end, size);
            BinaryPrimitives.WriteUInt16LittleEndian(record, ushort.CreateChecked(keyLength));
            _ = Utf8.GetBytes(payload.Key.Key, record[sizeof(ushort)..]);
            CompactPageValue.Write(record[(sizeof(ushort) + keyLength)..], in original);
            _namespaces?[slot] = string.Equals(payload.Key.Namespace, _namespace, StringComparison.Ordinal) ? null : payload.Key.Namespace;
            Volatile.Write(ref _layout.Ranges[slot], Range(_end, size));
            _end += size;
            _live += size - oldSize;
            ClearFallback(slot);
        }

        internal void Remove(int slot)
        {
            _live -= Size(_layout.Ranges[slot]);
            Volatile.Write(ref _layout.Ranges[slot], 0);
            ClearFallback(slot);
            _namespaces?[slot] = null;
        }

        /// <summary>Reclaims sparse storage before unlinking so that removal itself needs no allocation.</summary>
        /// <param name="slot">The occupant scheduled for removal.</param>
        internal void PrepareRemove(int slot)
        {
            var remaining = _live - Size(_layout.Ranges[slot]);
            if (_layout.Bytes.Length > SmallestPage && remaining < _layout.Bytes.Length / 4 && _live < _layout.Bytes.Length / 2)
                Rebuild(_live);
        }

        private static long Range(int offset, int size) => (long.CreateChecked(offset) << 32) | uint.CreateChecked(size);

        private static int Size(long range) => int.CreateTruncating(range);

        private static int Offset(long range) => int.CreateChecked(range >> 32);

        private static T? AsGeneric(StoredValue value) => Unsafe.As<StoredValue, T?>(ref value);

        private static ReadOnlySpan<byte> KeyBytes(T? raw)
        {
            var data = AsStored(raw).Memory.Span;
            return data.Slice(sizeof(ushort), BinaryPrimitives.ReadUInt16LittleEndian(data));
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
                return Utf8.GetByteCount(key);
            }
            catch (EncoderFallbackException)
            {
                // Preserve ordinal keys containing an unpaired UTF-16 surrogate in the ordinary slot representation.
                return -1;
            }
        }

        private void ClearFallback(int slot)
        {
            if (_fallback == null || _fallback[slot].Key.Key == null)
                return;
            _fallback[slot] = default;
            if (--_fallbackCount == 0)
                Volatile.Write(ref _fallback, null);
        }

        private void Rebuild(int capacity)
        {
            var old = _layout;
            var next = new Layout(old.Ranges.Length, capacity);
            var end = 0;
            for (var i = 0; i < old.Ranges.Length; i++)
            {
                var range = old.Ranges[i];
                if (range == 0)
                    continue;
                var size = Size(range);
                old.Bytes.AsSpan(Offset(range), size).CopyTo(next.Bytes.AsSpan(end));
                next.Ranges[i] = Range(end, size);
                end += size;
            }

            _end = end;
            Volatile.Write(ref _layout, next);
        }

        private sealed class Layout
        {
            internal Layout(int slots, int capacity)
            {
                Ranges = new long[slots];
                Bytes = GC.AllocateUninitializedArray<byte>(Math.Max(SmallestPage, capacity + (capacity / 4)));
            }

            internal long[] Ranges { get; }

            internal byte[] Bytes { get; }
        }
    }
}
