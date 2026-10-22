using System.Collections.Frozen;
using System.Text;
using Squirix.Server.Attributes;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

/// <summary>Bounded deterministic entry-size approximation for memory accounting (v0.7.x). Not an exact CLR heap measurement.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
[Immutable]
internal sealed class CacheEntrySizeEstimator<T> : ICacheEntrySizeEstimator<T>
{
    /// <summary>The entry node of the store: object header (16) and its fields, five references, five longs, three ints and a flag, padded to 96; 112 in all.</summary>
    /// <remarks>The node keeps the version, the deadline and the deadline index fields of every entry, with a deadline or without one.</remarks>
    private const int StoreNodeBytes = 112;

    /// <summary>The key object the node keeps: object header (16) and its namespace and key references.</summary>
    private const int CacheKeyObjectBytes = 32;

    /// <summary>The share of the bucket array of the store per entry: two slots of 8 bytes.</summary>
    /// <remarks>The array doubles once the entries fill three quarters of it, so it holds between 4/3 and 8/3 slots per entry; two is the middle.</remarks>
    private const int BucketSlotsBytes = 2 * sizeof(long);

    /// <summary>Fixed overhead per stored entry, with or without a deadline.</summary>
    /// <remarks>
    /// Derived from the layout of the store on a 64-bit runtime: 160 bytes. Measured with one million entries of a shared value, keys built
    /// beforehand: the heap grew 128.8 bytes per entry without a deadline, which with the 32-byte key object makes 160.8; the bucket array was
    /// at 2.1 slots per entry. A cache with a capacity also links every entry into its eviction order, a 48-byte list node not counted here.
    /// </remarks>
    private const int FixedPerEntryOverheadBytes = StoreNodeBytes + CacheKeyObjectBytes + BucketSlotsBytes;

    /// <summary>One item of the deadline index, padded: key and node references, deadline ticks and generation.</summary>
    private const int DeadlineIndexItemBytes = 32;

    /// <summary>One slot of the deadline index heap: an item and its priority.</summary>
    private const int DeadlineIndexSlotBytes = DeadlineIndexItemBytes + sizeof(long);

    /// <summary>The most slots the deadline index holds per entry with a deadline: its live item and, until the heap is rebuilt, one stale item.</summary>
    private const int MaxDeadlineIndexSlotsPerEntry = 2;

    /// <summary>Bytes the deadline index adds for an entry with a deadline.</summary>
    /// <remarks>
    /// The share follows the entry, so every node accounts it alike. A stale item left by an entry that lost its deadline is not counted: it is
    /// dropped at its old deadline or when the heap is rebuilt, which bounds the heap at twice the entries plus 1024 items. Measured with one
    /// million entries and no stale items, a deadline grew the heap 41.9 bytes per entry, one slot; the second slot is the bound on stale items.
    /// The entry node stores the deadline in a field every entry has, so a deadline adds nothing to the node.
    /// </remarks>
    private const int DeadlineIndexBytes = DeadlineIndexSlotBytes * MaxDeadlineIndexSlotsPerEntry;

    /// <summary>The width the entry codec stores every integer (as an Int64) and every floating-point number (as a Double) at.</summary>
    private const int EncodedNumberBytes = sizeof(long);

    /// <summary>Conservative fallback when no cheap payload size is derived for typed values.</summary>
    private const int UnknownTypedPayloadFallbackBytes = 128;

    /// <inheritdoc />
    public long EstimateBytes(CacheKey key, NodeCacheEntry<T> entry, bool payloadIsCounter)
    {
        long n = FixedPerEntryOverheadBytes;
        n += Encoding.UTF8.GetByteCount(key.Namespace);
        n += Encoding.UTF8.GetByteCount(key.Key);
        n += HasDeadline(entry) ? DeadlineIndexBytes : 0;
        n += EstimateTagsBytes(entry.Tags);
        n += payloadIsCounter ? sizeof(long) : EstimateTypedPayloadBytes(entry.Value);
        return n;
    }

    /// <inheritdoc />
    /// <remarks>Typed estimates derive from the value, so the known length is not used.</remarks>
    public long EstimateBytesForEncodedLength(CacheKey key, NodeCacheEntry<T> entry, int knownEncodedLength) => EstimateBytes(key, entry, false);

    /// <inheritdoc />
    public bool HasUnknownPayloadMagnitude(NodeCacheEntry<T> entry, bool isCounter) => !isCounter && MemoryAdmissionPayloadClassifier.IsUnknownTypedPayloadEstimate(entry.Value);

    /// <summary>Gets the bytes the deadline index adds for <paramref name="entry" />.</summary>
    /// <param name="entry">The entry.</param>
    /// <returns>The deadline index share for an entry with a deadline; otherwise zero.</returns>
    internal static long DeadlineIndexBytesOf(NodeCacheEntry<T> entry) => HasDeadline(entry) ? DeadlineIndexBytes : 0;

    /// <summary>Tells whether the entry is stored with a deadline.</summary>
    /// <param name="entry">The entry.</param>
    /// <returns><see langword="true" /> for an absolute deadline or a relative expiration, which becomes a stored deadline when the entry is written.</returns>
    private static bool HasDeadline(NodeCacheEntry<T> entry) => entry.ExpiresUtc != null || entry.Expiration != null;

    private static long EstimateTagsBytes(FrozenDictionary<string, string>? tags)
    {
        if (tags == null || tags.Count == 0)
            return 0;

        long sum = 0;
        foreach (var pair in tags)
        {
            sum += Encoding.UTF8.GetByteCount(pair.Key);
            sum += Encoding.UTF8.GetByteCount(pair.Value);
        }

        return sum;
    }

    private static long EstimateTypedPayloadBytes(T? value)
    {
        // A dedicated null arm detects actual null presence for unconstrained T; comparing against
        // default(T) would also zero-size valid payloads such as 0 or default-valued structs.
        // Numeric arms live in EstimateNumericOrFallbackBytes so this dispatch stays small.
        return value switch
        {
            null => 0,
            string s => Encoding.UTF8.GetByteCount(s),
            byte[] bytes => bytes.LongLength,
            bool => sizeof(bool),
            char => sizeof(char),
            decimal => sizeof(decimal),
            _ => EstimateNumericOrFallbackBytes(value),
        };
    }

    private static long EstimateNumericOrFallbackBytes(T? value) => value switch
    {
        // The entry codec returns every number at its stored width after a restart or a replicated apply, so each counts at that width.
        sbyte or byte => EncodedNumberBytes,
        short or ushort => EncodedNumberBytes,
        int or uint or float => EncodedNumberBytes,
        long or ulong or double => EncodedNumberBytes,
        _ => UnknownTypedPayloadFallbackBytes,
    };
}
