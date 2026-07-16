using System;
using System.Buffers.Binary;
using System.Collections.Frozen;
using System.Text.Json;
using Squirix.Server.Serialization;

namespace Squirix.Server.Storage.Entries.Binary;

/// <summary>Binary cache-entry encoding for journal and snapshot payloads.</summary>
internal static class CacheEntryCodec
{
    internal const int MaxUtf16StringLength = ushort.MaxValue;

    /// <summary>
    /// Returns a value already in a directly-encodable form: primitives, strings, byte arrays and
    /// <see cref="JsonElement" /> pass through unchanged, while any other object is serialized to a
    /// <see cref="JsonElement" /> exactly once. Callers normalize before the
    /// <see cref="ComputeEncodedLength" /> / <see cref="Write" /> pair so an arbitrary object is not
    /// re-serialized on every length and write pass.
    /// </summary>
    /// <param name="value">The raw cache value.</param>
    /// <returns>The same value when directly encodable; otherwise its <see cref="JsonElement" /> form.</returns>
    internal static object? NormalizeValue(object? value) => value switch
    {
        null or bool or string or byte[] or sbyte or byte or short or ushort or int or uint or long or float or double or decimal or JsonElement => value,
        _ => ServerSerializationProvider.Instance.SerializeToElement(value),
    };

    internal static bool TryMapEntry<T>(NodeCacheEntry<object?> entry, out NodeCacheEntry<T>? mapped)
    {
        if (!CacheEntryValueEncoding.TryCoerceTo<T>(entry.Value, out var typedValue))
        {
            mapped = null;
            return false;
        }

        mapped = new NodeCacheEntry<T>(typedValue, entry.Version, entry.ExpiresUtc, entry.Expiration, entry.Tags);
        return true;
    }

    internal static bool TryRead<T>(ReadOnlySpan<byte> source, out NodeCacheEntry<T>? entry, out int bytesRead)
    {
        entry = null;
        bytesRead = 0;
        if (!TryReadEnvelope(source, out var envelope))
            return false;

        if (!CacheEntryValueEncoding.TryCoerceTo<T>(envelope.Value, out var typedValue))
            return false;

        // bytesRead reports the full envelope consumed so callers can advance snapshot/journal cursors.
        entry = CreateEntry(typedValue, in envelope);
        bytesRead = envelope.BytesRead;
        return true;
    }

    internal static void Write(NodeCacheEntry<object?> entry, Span<byte> destination)
    {
        if (destination.Length < ComputeEncodedLength(entry))
            throw new ArgumentException("Destination span is too small for the encoded cache entry.", nameof(destination));

        var offset = 0;
        if (entry.ExpiresUtc is { } expiresUtc)
        {
            destination[offset++] = 1;
            BinaryPrimitives.WriteInt64LittleEndian(destination[offset..], new DateTimeOffset(expiresUtc.ToUniversalTime()).ToUnixTimeMilliseconds());
            offset += 8;
        }
        else
        {
            destination[offset++] = 0;
        }

        if (entry.Expiration is { } expiration)
        {
            destination[offset++] = 1;
            BinaryPrimitives.WriteInt64LittleEndian(destination[offset..], expiration.Ticks);
            offset += 8;
        }
        else
        {
            destination[offset++] = 0;
        }

        BinaryPrimitives.WriteInt64LittleEndian(destination[offset..], entry.Version);
        offset += 8;
        offset += CacheEntryTagEncoding.Write(entry.Tags, destination[offset..]);
        _ = CacheEntryValueEncoding.Write(entry.Value, destination[offset..]);
    }

    internal static int ComputeEncodedLength(NodeCacheEntry<object?> entry)
    {
        var length = 1 + 1 + 8;
        length += CacheEntryTagEncoding.ComputeLength(entry.Tags);
        length += CacheEntryValueEncoding.ComputeLength(entry.Value);
        if (entry.ExpiresUtc is not null)
            length += 8;

        if (entry.Expiration is not null)
            length += 8;

        return length;
    }

    private static NodeCacheEntry<T> CreateEntry<T>(T? typedValue, in ReadEnvelope envelope) =>
        new(typedValue, envelope.Version, envelope.ExpiresUtc, envelope.Expiration, envelope.Tags);

    private static bool TryReadEnvelope(ReadOnlySpan<byte> source, out ReadEnvelope envelope)
    {
        envelope = default;
        if (source.Length < 1 + 1 + 8)
            return false;

        var offset = 0;
        if (!TryReadExpirationFields(source, ref offset, out var expiresUtc, out var expiration))
            return false;

        if (!TryReadVersion(source, ref offset, out var version))
            return false;

        if (!TryReadTagsAndValue(source, ref offset, out var tags, out var value))
            return false;

        envelope = new ReadEnvelope(expiresUtc, expiration, version, tags, value, offset);
        return true;
    }

    private static bool TryReadExpirationFields(ReadOnlySpan<byte> source, ref int offset, out DateTime? expiresUtc, out TimeSpan? expiration)
    {
        expiresUtc = null;
        expiration = null;

        // Entry envelope: optional expires/expiration flags, fixed version, tags, then typed value payload.
        if (source[offset++] is not 0)
        {
            if (source.Length < offset + 8)
                return false;

            expiresUtc = DateTimeOffset.FromUnixTimeMilliseconds(BinaryPrimitives.ReadInt64LittleEndian(source[offset..])).UtcDateTime;
            offset += 8;
        }

        if (source[offset++] is 0)
            return true;
        if (source.Length < offset + 8)
            return false;

        expiration = TimeSpan.FromTicks(BinaryPrimitives.ReadInt64LittleEndian(source[offset..]));
        offset += 8;

        return true;
    }

    private static bool TryReadTagsAndValue(ReadOnlySpan<byte> source, ref int offset, out FrozenDictionary<string, string>? tags, out object? value)
    {
        tags = null;
        value = null;

        // Tags and value are length-prefixed sections; both must parse before coercion to T.
        if (!CacheEntryTagEncoding.TryRead(source[offset..], out tags, out var tagsBytes))
            return false;

        offset += tagsBytes;
        if (!CacheEntryValueEncoding.TryRead(source[offset..], out value, out var valueBytes))
            return false;

        offset += valueBytes;
        return true;
    }

    private static bool TryReadVersion(ReadOnlySpan<byte> source, ref int offset, out long version)
    {
        version = 0;
        if (source.Length < offset + 8)
            return false;

        version = BinaryPrimitives.ReadInt64LittleEndian(source[offset..]);
        offset += 8;
        return true;
    }

    private readonly record struct ReadEnvelope
    {
        internal ReadEnvelope(DateTime? expiresUtc, TimeSpan? expiration, long version, FrozenDictionary<string, string>? tags, object? value, int bytesRead)
        {
            ExpiresUtc = expiresUtc;
            Expiration = expiration;
            Version = version;
            Tags = tags;
            Value = value;
            BytesRead = bytesRead;
        }

        internal DateTime? ExpiresUtc { get; }

        internal TimeSpan? Expiration { get; }

        internal long Version { get; }

        internal FrozenDictionary<string, string>? Tags { get; }

        internal object? Value { get; }

        internal int BytesRead { get; }
    }
}
