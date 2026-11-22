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
    /// <summary>The existing admission budget: a 104-byte store node, two eight-byte bucket slots, and 16 bytes of headroom on x64.</summary>
    private const int FixedPerEntryOverheadBytes = 136;

    /// <summary>The width the entry codec stores every integer (as an Int64) and every floating-point number (as a Double) at.</summary>
    private const int EncodedNumberBytes = sizeof(long);

    /// <summary>Conservative fallback when no cheap payload size is derived for typed values.</summary>
    private const int UnknownTypedPayloadFallbackBytes = 128;

    /// <inheritdoc />
    public long EstimateBytes(CacheKey key, NodeCacheEntry<T> entry, bool payloadIsCounter) => EstimateWithPayload(
        key,
        entry,
        payloadIsCounter ? sizeof(long) : EstimateTypedPayloadBytes(entry.RawValue));

    /// <inheritdoc />
    public long MeasureValue(T? value) => EstimateTypedPayloadBytes(value);

    /// <inheritdoc />
    public long EstimateBytesForMeasuredValue(CacheKey key, NodeCacheEntry<T> entry, long measuredValue) => EstimateWithPayload(key, entry, measuredValue);

    /// <inheritdoc />
    /// <remarks>Typed estimates derive from the value, so the known length is not used.</remarks>
    public long EstimateBytesForEncodedLength(CacheKey key, NodeCacheEntry<T> entry, int knownEncodedLength) => EstimateBytes(key, entry, false);

    /// <inheritdoc />
    public bool HasUnknownPayloadMagnitude(NodeCacheEntry<T> entry, bool isCounter) => !isCounter && MemoryAdmissionPayloadClassifier.IsUnknownTypedPayloadEstimate(entry.RawValue);

    /// <summary>Includes the resident policy metadata in estimates used by admission and the physical store.</summary>
    /// <param name="estimator">Base entry-size estimator.</param>
    /// <param name="options">Eviction configuration.</param>
    /// <returns>The estimator appropriate for this cache.</returns>
    internal static ICacheEntrySizeEstimator<T> ForEviction(ICacheEntrySizeEstimator<T> estimator, EvictionOptions options)
    {
        if (!options.Enabled || estimator is BoundedEstimator)
            return estimator;

        return new BoundedEstimator(estimator, options.Policy == EvictionPolicyType.Lru ? 52 : 40);
    }

    /// <summary>Gets the additional index bytes when an entry gains an absolute deadline.</summary>
    /// <param name="entry">The stored entry.</param>
    /// <returns>The additional index bytes.</returns>
    internal static long DeadlineIndexGrowthOf(NodeCacheEntry<T> entry) => CacheEntryDeadlineSize.IndexBytes - DeadlineIndexBytesOf(entry);

    /// <summary>Gets the bytes the deadline index adds for <paramref name="entry" />.</summary>
    /// <param name="entry">The entry.</param>
    /// <returns>The deadline index share for an entry with a deadline; otherwise zero.</returns>
    private static long DeadlineIndexBytesOf(NodeCacheEntry<T> entry) => HasDeadline(entry) ? CacheEntryDeadlineSize.IndexBytes : 0;

    /// <summary>Tells whether the entry is stored with a deadline.</summary>
    /// <param name="entry">The entry.</param>
    /// <returns><see langword="true" /> for an absolute deadline or a relative expiration, which becomes a stored deadline when the entry is written.</returns>
    private static bool HasDeadline(NodeCacheEntry<T> entry) => entry.ExpiresUtc != null || entry.Expiration != null;

    private static long EstimateWithPayload(CacheKey key, NodeCacheEntry<T> entry, long payloadBytes)
    {
        long n = FixedPerEntryOverheadBytes;
        n += Encoding.UTF8.GetByteCount(key.Namespace);
        n += Encoding.UTF8.GetByteCount(key.Key);
        n += DeadlineIndexBytesOf(entry);
        n += EstimateTagsBytes(entry.Tags);
        n += payloadBytes;
        return n;
    }

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
            StoredText text => text.Bytes.Length,
            string s => Encoding.UTF8.GetByteCount(s),
            byte[] bytes => bytes.LongLength,
            bool => sizeof(bool),
            char => sizeof(char),
            decimal => sizeof(decimal),
            JsonUtf8Value json => json.Length + JsonUtf8Value.OverheadBytes,
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

    /// <summary>The bounded node adds 40 bytes. Sampled LRU also reserves one to two eight-byte array slots per resident.</summary>
    [Immutable]
    private sealed class BoundedEstimator : ICacheEntrySizeEstimator<T>
    {
        private readonly ICacheEntrySizeEstimator<T> _inner;
        private readonly int _overhead;

        internal BoundedEstimator(ICacheEntrySizeEstimator<T> inner, int overhead)
        {
            _inner = inner;
            _overhead = overhead;
        }

        public long EstimateBytes(CacheKey key, NodeCacheEntry<T> entry, bool payloadIsCounter) =>
            _overhead + _inner.EstimateBytes(key, entry, payloadIsCounter);

        public long EstimateBytesForEncodedLength(CacheKey key, NodeCacheEntry<T> entry, int knownEncodedLength) =>
            _overhead + _inner.EstimateBytesForEncodedLength(key, entry, knownEncodedLength);

        public long EstimateBytesForMeasuredValue(CacheKey key, NodeCacheEntry<T> entry, long measuredValue) =>
            _overhead + _inner.EstimateBytesForMeasuredValue(key, entry, measuredValue);

        public long MeasureValue(T? value) => _inner.MeasureValue(value);

        public bool HasUnknownPayloadMagnitude(NodeCacheEntry<T> entry, bool isCounter) => _inner.HasUnknownPayloadMagnitude(entry, isCounter);
    }
}
