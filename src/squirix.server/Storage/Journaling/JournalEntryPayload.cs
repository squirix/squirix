using System;
using System.Buffers;
using Squirix.Server.Core;
using Squirix.Server.Storage.Codecs;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Sync encode/decode of journal Put payloads via <see cref="CacheEntryCodec" />.</summary>
internal static class JournalEntryPayload
{
    internal static PooledJournalPayload Encode(in PreparedJournalEntry prepared)
    {
        var pooledBuffer = ArrayPool<byte>.Shared.Rent(prepared.EncodedLength);
        try
        {
            CacheEntryCodec.WriteMeasured(prepared.ObjectEntry, pooledBuffer);
            return new PooledJournalPayload(pooledBuffer, prepared.EncodedLength);
        }
        catch
        {
            ArrayPool<byte>.Shared.ReturnCleared(pooledBuffer);
            throw;
        }
    }

    /// <summary>Assembles the payload of an entry around a value that is already encoded, without normalizing or serializing the value again.</summary>
    /// <typeparam name="T">The cache value type.</typeparam>
    /// <param name="entry">The entry that supplies the deadline, version and tags; its value is not read.</param>
    /// <param name="value">The prepared value of the entry.</param>
    /// <returns>The pooled payload.</returns>
    /// <exception cref="Squirix.Server.Errors.SquirixException">The assembled entry exceeds the entry size limit.</exception>
    internal static PooledJournalPayload EncodeWithPreparedValue<T>(NodeCacheEntry<T> entry, PreparedJournalValue value)
    {
        var length = CacheEntryCodec.ComputeEncodedLength(entry.ExpiresUtc, entry.Expiration, entry.Tags, value.EncodedLength);
        EntryPayloadSizeGuard.EnsureLengthWithinLimit(length);
        var pooledBuffer = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            CacheEntryCodec.WriteWithEncodedValue(entry.ExpiresUtc, entry.Expiration, entry.Version, entry.Tags, value.Memory.Span, pooledBuffer);
            return new PooledJournalPayload(pooledBuffer, length);
        }
        catch
        {
            ArrayPool<byte>.Shared.ReturnCleared(pooledBuffer);
            throw;
        }
    }

    /// <summary>Normalizes and encodes a value once.</summary>
    /// <typeparam name="T">The cache value type.</typeparam>
    /// <param name="value">The value to prepare.</param>
    /// <returns>The prepared value; the caller disposes it.</returns>
    internal static PreparedJournalValue PrepareValue<T>(T? value) => PreparedJournalValue.Create(NodeCacheEntry<T>.NormalizeValue(value));

    internal static void EnsureEncodedLengthWithinLimit<T>(NodeCacheEntry<T> entry)
    {
        EntryTagsGuard.EnsureWithinLimits(entry.Tags);
        EntryPayloadSizeGuard.EnsureLengthWithinLimit(ComputeEncodedLength(entry));
    }

    internal static int MeasureSerializedBytes<T>(NodeCacheEntry<T> entry) => ComputeEncodedLength(entry);

    internal static PreparedJournalEntry PrepareEncode<T>(NodeCacheEntry<T> entry) => PreparedJournalEntry.From(entry);

    internal static bool TryDecode<T>(ReadOnlySpan<byte> source, out NodeCacheEntry<T>? entry)
    {
        if (CacheEntryCodec.TryRead(source, out entry, out _))
            return true;
        entry = null;
        return false;
    }

    private static int ComputeEncodedLength<T>(NodeCacheEntry<T> entry) => PrepareEncode(entry).EncodedLength;
}
