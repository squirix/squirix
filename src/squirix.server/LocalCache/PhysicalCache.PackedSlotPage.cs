using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

internal sealed partial class PhysicalCache<T>
{
    /// <summary>Packs a slot page's keys and typed values into immutable byte ranges. Only the writer allocates or relocates them.</summary>
    private sealed class PackedSlotPage
    {
        private const int SmallestPage = 256;
        private const int LargestRecord = 16384;
        private byte[] _bytes = [];
        private int _end;
        private int _live;
        private string? _namespace;

        internal static bool Typed => typeof(T) == typeof(StoredValue);

        private static Encoding Utf8 => CompactPageValue.KeyEncoding;

        internal static PackedSlotPage Seal(SlotRecord[] slots)
        {
            var writer = new PackedSlotPage();
            var needed = 0;
            foreach (ref var slot in slots.AsSpan())
            {
                if (slot.Lifetime > 0)
                    needed += RecordSize(slot.Entry.Key.Key, AsStored(slot.Entry.RawValue), out _);
            }

            writer._bytes = GC.AllocateUninitializedArray<byte>(Math.Max(SmallestPage, needed + (needed / 4)));
            for (var i = 0; i < slots.Length; i++)
            {
                ref var slot = ref slots[i];
                if (slot.Lifetime <= 0)
                    continue;
                var key = slot.Entry.Key;
                var raw = slot.Entry.RawValue;
                if (RecordSize(key.Key, AsStored(raw), out _) == 0)
                    continue;
                raw = writer.Pack(slots, i, ref key, raw);
                slot.Entry.WriteValue(key, raw);
            }

            return writer;
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

        internal T? Pack(SlotRecord[] slots, int offset, ref CacheKey key, T? logical)
        {
            var original = AsStored(logical);
            var size = RecordSize(key.Key, in original, out var keyLength);
            if (size == 0)
            {
                Release(slots, offset);
                return logical;
            }

            var retained = _live - Occupied(slots, offset);
            if (size > _bytes.Length - _end)
                Rebuild(slots, offset, retained + size);
            var record = _bytes.AsSpan(_end, size);
            BinaryPrimitives.WriteUInt16LittleEndian(record, ushort.CreateChecked(keyLength));
            _ = Utf8.GetBytes(key.Key, record[sizeof(ushort)..]);
            CompactPageValue.Write(record[(sizeof(ushort) + keyLength)..], in original);
            var value = StoredValue.FromPage(StoredValueKind.Binary, _bytes, _end, size, true);
            _end += size;
            _live = retained + size;
            if (!string.Equals(_namespace, key.Namespace, StringComparison.Ordinal))
                _namespace = key.Namespace;
            key = new CacheKey(_namespace ?? key.Namespace, string.Empty);
            return AsGeneric(value);
        }

        internal void Release(SlotRecord[] slots, int offset)
        {
            var retained = _live - Occupied(slots, offset);
            if (retained == 0)
            {
                _bytes = [];
                _end = 0;
            }
            else if (_bytes.Length > SmallestPage && retained < _bytes.Length / 4)
            {
                Rebuild(slots, offset, retained);
            }

            _live = retained;
        }

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

        private static int Occupied(SlotRecord[] slots, int offset)
        {
            if (slots[offset].Lifetime <= 0)
                return 0;
            var record = AsStored(slots[offset].Entry.RawValue);
            return record.IsPageRecord ? record.Memory.Length : 0;
        }

        private void Rebuild(SlotRecord[] slots, int except, int needed)
        {
            var next = GC.AllocateUninitializedArray<byte>(Math.Max(SmallestPage, needed + (needed / 4)));
            var end = 0;
            for (var i = 0; i < slots.Length; i++)
            {
                if (i == except || slots[i].Lifetime <= 0)
                    continue;
                var record = AsStored(slots[i].Entry.RawValue);
                if (!record.IsPageRecord)
                    continue;
                var data = record.Memory;
                data.Span.CopyTo(next.AsSpan(end));
                slots[i].Entry.Relocate(AsGeneric(StoredValue.FromPage(StoredValueKind.Binary, next, end, data.Length, true)));
                end += data.Length;
            }

            _bytes = next;
            _end = end;
        }
    }
}
