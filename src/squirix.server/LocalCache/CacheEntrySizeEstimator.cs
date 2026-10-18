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
    /// <summary>Structural overhead per stored entry: dictionary node, metadata handles, alignment slack.</summary>
    private const int DictionaryEntryOverheadBytes = 96;

    /// <summary>The fields every entry node keeps for the deadline index: the deadline and generation of its canonical item.</summary>
    private const int DeadlineIndexNodeFieldsBytes = sizeof(long) + sizeof(int);

    /// <summary>Fixed overhead per stored entry, with or without a deadline.</summary>
    private const int FixedPerEntryOverheadBytes = DictionaryEntryOverheadBytes + DeadlineIndexNodeFieldsBytes;

    /// <summary>One item of the deadline index, padded: key and node references, deadline ticks and generation.</summary>
    private const int DeadlineIndexItemBytes = 32;

    /// <summary>One slot of the deadline index heap: an item and its priority.</summary>
    private const int DeadlineIndexSlotBytes = DeadlineIndexItemBytes + sizeof(long);

    /// <summary>The most slots the deadline index holds per entry with a deadline: its live item and, until the heap is rebuilt, one stale item.</summary>
    private const int MaxDeadlineIndexSlotsPerEntry = 2;

    /// <summary>Bytes the deadline index adds for an entry with a deadline.</summary>
    /// <remarks>
    /// The share follows the entry, so every node accounts it alike. A stale item left by an entry that lost its deadline is not counted: it is
    /// dropped at its old deadline or when the heap is rebuilt, which bounds the heap at twice the entries plus 1024 items.
    /// </remarks>
    private const int DeadlineIndexBytes = DeadlineIndexSlotBytes * MaxDeadlineIndexSlotsPerEntry;

    /// <summary>The stored deadline of an entry: a nullable <see cref="System.DateTime" /> field.</summary>
    private const int StoredDeadlineBytes = 16;

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
        n += sizeof(long);
        n += HasDeadline(entry) ? StoredDeadlineBytes + DeadlineIndexBytes : 0;
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
