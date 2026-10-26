using System;
using System.Buffers;
using Squirix.Server.Core;
using Squirix.Server.Storage.Codecs;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Sync encode/decode of journal Put payloads via <see cref="CacheEntryCodec" />.</summary>
internal static class JournalEntryPayload
{
    /// <summary>Encodes a prepared entry.</summary>
    /// <param name="prepared">The prepared entry.</param>
    /// <returns>The pooled payload; its length is the encoded length of the entry.</returns>
    /// <remarks>
    /// The prepared length may be a hint taken from bytes the entry was decoded from. When the entry does not encode to exactly that length, it is
    /// measured and encoded again at its real length.
    /// </remarks>
    /// <exception cref="InvalidOperationException">The entry does not encode to its measured length.</exception>
    internal static PooledJournalPayload Encode(in PreparedJournalEntry prepared)
    {
        byte[]? pooledBuffer = null;
        try
        {
            var length = prepared.EncodedLength;
            pooledBuffer = ArrayPool<byte>.Shared.Rent(length);
            if (WriteWithin(prepared.ObjectEntry, pooledBuffer.AsSpan(0, length)) == length)
                return new PooledJournalPayload(pooledBuffer, length);
            ArrayPool<byte>.Shared.ReturnCleared(pooledBuffer);
            pooledBuffer = null;
            length = CacheEntryCodec.ComputeEncodedLength(prepared.ObjectEntry);
            EntryPayloadSizeGuard.EnsureLengthWithinLimit(length);
            pooledBuffer = ArrayPool<byte>.Shared.Rent(length);
            return CacheEntryCodec.WriteMeasured(prepared.ObjectEntry, pooledBuffer.AsSpan(0, length)) != length
                ? throw new InvalidOperationException("The journal entry encoded to a length other than the measured one.")
                : new PooledJournalPayload(pooledBuffer, length);
        }
        catch
        {
            if (pooledBuffer != null)
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
    /// <exception cref="InvalidOperationException">The entry has a relative expiration; callers pass a decided absolute deadline.</exception>
    internal static PooledJournalPayload EncodeWithPreparedValue<T>(NodeCacheEntry<T> entry, PreparedJournalValue value)
    {
        if (entry.Expiration != null)
            throw CreateRelativeExpirationException();

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

    internal static void EnsureEncodedLengthWithinLimit<T>(NodeCacheEntry<T> entry)
    {
        EntryTagsGuard.EnsureWithinLimits(entry.Tags);
        EntryPayloadSizeGuard.EnsureLengthWithinLimit(ComputeEncodedLength(entry));
    }

    internal static int MeasureSerializedBytes<T>(NodeCacheEntry<T> entry) => ComputeEncodedLength(entry);

    internal static PreparedJournalEntry PrepareEncode<T>(NodeCacheEntry<T> entry) => PreparedJournalEntry.From(entry);

    internal static PreparedJournalEntry PrepareEncode<T>(NodeCacheEntry<T> entry, int encodedLength) => PreparedJournalEntry.From(entry, encodedLength);

    /// <summary>Normalizes and encodes a value once.</summary>
    /// <typeparam name="T">The cache value type.</typeparam>
    /// <param name="value">The value to prepare.</param>
    /// <returns>The prepared value; the caller disposes it.</returns>
    internal static PreparedJournalValue PrepareValue<T>(T? value) => PreparedJournalValue.Create(NodeCacheEntry<T>.NormalizeValue(value));

    internal static bool TryDecode<T>(ReadOnlySpan<byte> source, out NodeCacheEntry<T>? entry) => TryDecode(source, out entry, out _);

    /// <summary>Decodes a journal entry payload and reports how many bytes it spans.</summary>
    /// <typeparam name="T">The cache value type.</typeparam>
    /// <param name="source">The payload.</param>
    /// <param name="entry">The decoded entry.</param>
    /// <param name="bytesRead">The number of bytes the entry spans.</param>
    /// <returns><see langword="true" /> when the payload decodes.</returns>
    internal static bool TryDecode<T>(ReadOnlySpan<byte> source, out NodeCacheEntry<T>? entry, out int bytesRead)
    {
        if (CacheEntryCodec.TryRead(source, out entry, out bytesRead))
            return true;
        entry = null;
        return false;
    }

    /// <summary>Decodes a payload whose absolute deadline travels apart from it and reports the journal-encoded length of the entry that results.</summary>
    /// <param name="source">The payload; any deadline it holds is replaced.</param>
    /// <param name="expiresUtcTicks">The absolute deadline in UTC ticks, or zero for none.</param>
    /// <param name="entry">The decoded entry carrying <paramref name="expiresUtcTicks" /> and no relative expiration.</param>
    /// <param name="encodedLength">
    /// The journal-encoded length of <paramref name="entry" /> derived from the payload bytes, or <see langword="null" /> when the payload holds
    /// bytes past the entry and the length is not derived. Encoding treats it as a hint and measures when the entry does not fit it.
    /// </param>
    /// <returns><see langword="true" /> when the payload decodes.</returns>
    internal static bool TryDecodeWithDeadline(ReadOnlySpan<byte> source, long expiresUtcTicks, out NodeCacheEntry<object?>? entry, out int? encodedLength)
    {
        entry = null;
        encodedLength = null;
        if (!TryDecode<object?>(source, out var decoded, out var bytesRead) || decoded == null)
            return false;

        DateTime? expiresUtc = expiresUtcTicks == 0 ? null : new DateTime(expiresUtcTicks, DateTimeKind.Utc);
        entry = new NodeCacheEntry<object?>(decoded.Value, decoded.Version, expiresUtc, null, decoded.Tags);
        if (bytesRead != source.Length)
            return true;

        var decodedDeadlines = CacheEntryCodec.ComputeEncodedLength(decoded.ExpiresUtc, decoded.Expiration, null, 0);
        encodedLength = bytesRead - decodedDeadlines + CacheEntryCodec.ComputeEncodedLength(expiresUtc, null, null, 0);
        return true;
    }

    private static int ComputeEncodedLength<T>(NodeCacheEntry<T> entry) => PreparedJournalEntry.MeasureEncodedLength(entry);

    private static InvalidOperationException CreateRelativeExpirationException() =>
        new("A journal entry assembled around a prepared value must carry an absolute deadline, not a relative expiration.");

    private static int WriteWithin(NodeCacheEntry<object?> entry, Span<byte> destination)
    {
        try
        {
            return CacheEntryCodec.WriteMeasured(entry, destination);
        }
        catch (Exception error) when (error is ArgumentException or IndexOutOfRangeException)
        {
            // The destination is the length the caller expected: a longer encoding runs out of it, and the caller measures the entry and tries again.
            return -1;
        }
    }
}
