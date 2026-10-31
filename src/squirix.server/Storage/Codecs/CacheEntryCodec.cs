using System;
using System.Buffers.Binary;
using System.Collections.Frozen;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Codecs;

/// <summary>Binary cache-entry encoding for journal and snapshot payloads.</summary>
internal static partial class CacheEntryCodec
{
    private const byte False = 0;
    private const int MaxUtf16StringLength = ushort.MaxValue;
    private const byte True = 1;

    internal static int ComputeEncodedLength(NodeCacheEntry<object?> entry) =>
        ComputeEncodedLength(entry.ExpiresUtc, entry.Expiration, entry.Tags, CacheEntryValueEncoding.ComputeLength(entry.Value));

    /// <summary>Computes the encoded length of an entry whose value is already measured.</summary>
    /// <param name="expiresUtc">The absolute deadline, or <see langword="null" />.</param>
    /// <param name="expiration">The relative expiration, or <see langword="null" />.</param>
    /// <param name="tags">The entry tags, or <see langword="null" />.</param>
    /// <param name="valueLength">The encoded length of the value.</param>
    /// <returns>The encoded entry length.</returns>
    internal static int ComputeEncodedLength(DateTime? expiresUtc, TimeSpan? expiration, FrozenDictionary<string, string>? tags, int valueLength)
    {
        var length = 1 + 1 + 8;
        length += TagEncoding.ComputeLength(tags);
        length += valueLength;
        if (expiresUtc != null)
            length += 8;

        if (expiration != null)
            length += 8;

        return length;
    }

    /// <summary>Computes the encoded length of a normalized value.</summary>
    /// <param name="normalizedValue">The normalized value.</param>
    /// <returns>The encoded value length.</returns>
    internal static int ComputeValueLength(object? normalizedValue) => CacheEntryValueEncoding.ComputeLength(normalizedValue);

    /// <summary>Writes a normalized value in the entry value encoding.</summary>
    /// <param name="normalizedValue">The normalized value.</param>
    /// <param name="destination">The destination, at least <see cref="ComputeValueLength" /> bytes.</param>
    internal static void WriteValue(object? normalizedValue, Span<byte> destination) => _ = CacheEntryValueEncoding.WriteInternal(normalizedValue, destination);

    /// <summary>Writes an entry around a value that is already encoded, so the value is not measured or serialized again.</summary>
    /// <param name="expiresUtc">The absolute deadline, or <see langword="null" />.</param>
    /// <param name="expiration">The relative expiration, or <see langword="null" />.</param>
    /// <param name="version">The entry version.</param>
    /// <param name="tags">The entry tags, or <see langword="null" />.</param>
    /// <param name="encodedValue">The value encoded by <see cref="WriteValue" />.</param>
    /// <param name="destination">The destination, at least <see cref="ComputeEncodedLength(DateTime?, TimeSpan?, FrozenDictionary{string, string}, int)" /> bytes.</param>
    internal static void WriteWithEncodedValue(
        DateTime? expiresUtc,
        TimeSpan? expiration,
        long version,
        FrozenDictionary<string, string>? tags,
        ReadOnlySpan<byte> encodedValue,
        Span<byte> destination)
    {
        var offset = WriteHeader(expiresUtc, expiration, version, tags, destination);
        encodedValue.CopyTo(destination[offset..]);
    }

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

        _ = WriteMeasured(entry, destination);
    }

    /// <summary>Writes an entry into a destination the caller already sized with <c language="csharp">ComputeEncodedLength</c>, without measuring the value again.</summary>
    /// <param name="entry">The entry to write.</param>
    /// <param name="destination">The destination, at least the measured encoded length.</param>
    /// <returns>The number of bytes written.</returns>
    internal static int WriteMeasured(NodeCacheEntry<object?> entry, Span<byte> destination)
    {
        var offset = WriteHeader(entry.ExpiresUtc, entry.Expiration, entry.Version, entry.Tags, destination);
        return offset + CacheEntryValueEncoding.WriteInternal(entry.Value, destination[offset..]);
    }

    private static int WriteHeader(DateTime? expiresUtcValue, TimeSpan? expirationValue, long version, FrozenDictionary<string, string>? tags, Span<byte> destination)
    {
        var offset = 0;
        if (expiresUtcValue is { } expiresUtc)
        {
            destination[offset++] = 1;
            BinaryPrimitives.WriteInt64LittleEndian(destination[offset..], new DateTimeOffset(expiresUtc.ToUniversalTime()).ToUnixTimeMilliseconds());
            offset += 8;
        }
        else
        {
            destination[offset++] = 0;
        }

        if (expirationValue is { } expiration)
        {
            destination[offset++] = 1;
            BinaryPrimitives.WriteInt64LittleEndian(destination[offset..], expiration.Ticks);
            offset += 8;
        }
        else
        {
            destination[offset++] = 0;
        }

        BinaryPrimitives.WriteInt64LittleEndian(destination[offset..], version);
        offset += 8;
        offset += TagEncoding.WriteTag(tags, destination[offset..]);
        return offset;
    }

    private static NodeCacheEntry<T> CreateEntry<T>(T? typedValue, in ReadEnvelope e) => new(typedValue, e.Version, e.ExpiresUtc, e.Expiration, e.Tags);

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
        if (source[offset++] != 0)
        {
            if (source.Length < offset + 8)
                return false;

            if (!WireBounds.TryUtcFromUnixMilliseconds(BinaryPrimitives.ReadInt64LittleEndian(source[offset..]), out var absolute))
                return false;

            expiresUtc = absolute;
            offset += 8;
        }

        if (source[offset++] == 0)
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
        if (!TagEncoding.TryReadTag(source[offset..], out tags, out var tagsBytes))
            return false;

        offset += tagsBytes;
        if (!CacheEntryValueEncoding.TryReadInternal(source[offset..], out value, out var valueBytes))
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

    [Immutable]
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

        internal int BytesRead { get; }

        internal TimeSpan? Expiration { get; }

        internal DateTime? ExpiresUtc { get; }

        internal FrozenDictionary<string, string>? Tags { get; }

        internal object? Value { get; }

        internal long Version { get; }
    }
}
