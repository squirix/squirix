using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;
using Squirix.Server.Attributes;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

/// <summary>Holds the logical key and value. A packed record stores the key’s UTF-8 bytes alongside the encoded value.</summary>
/// <typeparam name="T">The stored value type.</typeparam>
/// <param name="Key">The key, or just its namespace for a packed record.</param>
/// <param name="Value">The cached value or a reference to its packed record.</param>
[Immutable]
internal readonly record struct SlotPayload<T>(CacheKey Key, T? Value)
{
    internal CacheKey FullKey => IsPacked ? Key with { Key = KeyText(Value) } : Key;

    private bool IsPacked => typeof(T) == typeof(StoredValue) && AsStored(Value).IsPageRecord;

    internal static StoredValue AsStored(T? value) => Unsafe.As<T?, StoredValue>(ref value);

    internal static T? Unpack(T? raw)
    {
        if (typeof(T) != typeof(StoredValue))
            return raw;
        var value = AsStored(raw);
        if (!value.IsPageRecord)
            return raw;
        var keyLength = BinaryPrimitives.ReadUInt16LittleEndian(value.Memory.Span);
        return AsGeneric(CompactPageValue.ReadRecord(in value, sizeof(ushort) + keyLength));
    }

    internal static T? AsGeneric(StoredValue value) => Unsafe.As<StoredValue, T?>(ref value);

    internal bool Matches(CacheKey key) => IsPacked
        ? string.Equals(Key.Namespace, key.Namespace, StringComparison.Ordinal) && Matches(Value, key.Key)
        : Key.Equals(key);

    private static string KeyText(T? raw) => CompactPageValue.KeyEncoding.GetString(KeyBytes(raw));

    private static bool Matches(T? raw, string key)
    {
        var saved = KeyBytes(raw);
        if (saved.Length == key.Length)
            return Ascii.Equals(saved, key);
        if (saved.Length < key.Length)
            return false;
        if (key.Length > 256)
            return MatchesLongKey(saved, key);

        Span<char> chars = stackalloc char[key.Length];
        return System.Text.Unicode.Utf8.ToUtf16(saved, chars, out _, out var written) == System.Buffers.OperationStatus.Done &&
            chars[..written].SequenceEqual(key);
    }

    private static bool MatchesLongKey(ReadOnlySpan<byte> saved, ReadOnlySpan<char> remaining)
    {
        Span<char> chars = stackalloc char[256];
        while (true)
        {
            var status = System.Text.Unicode.Utf8.ToUtf16(saved, chars, out var consumed, out var written);
            if (!remaining.StartsWith(chars[..written], StringComparison.Ordinal))
                return false;
            remaining = remaining[written..];
            if (status == System.Buffers.OperationStatus.Done)
                return remaining.IsEmpty;
            if (status != System.Buffers.OperationStatus.DestinationTooSmall || consumed == 0)
                return false;
            saved = saved[consumed..];
        }
    }

    private static ReadOnlySpan<byte> KeyBytes(T? raw)
    {
        var data = AsStored(raw).Memory.Span;
        return data.Slice(sizeof(ushort), BinaryPrimitives.ReadUInt16LittleEndian(data));
    }
}
