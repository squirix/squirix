using System.Collections.Frozen;
using System.Text;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

/// <summary>
/// Bounded deterministic entry-size approximation for memory accounting (v0.7.x). Not an exact CLR heap measurement.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class CacheEntrySizeEstimator<T> : ICacheEntrySizeEstimator<T>
{
    /// <summary>
    /// Fixed structural overhead per stored entry (dictionary node, metadata handles, alignment slack).
    /// </summary>
    private const int FixedPerEntryOverheadBytes = 96;

    /// <summary>
    /// Conservative fallback when no cheap payload size is derived for typed values.
    /// </summary>
    private const int UnknownTypedPayloadFallbackBytes = 128;

    /// <inheritdoc />
    public long EstimateBytes(CacheKey key, CacheEntry<T> entry, bool payloadIsCounter)
    {
        long n = FixedPerEntryOverheadBytes;
        n += Encoding.UTF8.GetByteCount(key.Namespace);
        n += Encoding.UTF8.GetByteCount(key.Key);
        n += sizeof(long);
        n += entry.ExpiresUtc.HasValue ? 16 : 0;
        n += EstimateTagsBytes(entry.Tags);
        n += payloadIsCounter ? sizeof(long) : EstimateTypedPayloadBytes(entry.Value);
        return n;
    }

    private static long EstimateTagsBytes(FrozenDictionary<string, string>? tags)
    {
        if (tags is null || tags.Count == 0)
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
        return value is null
            ? 0
            : value switch
            {
                string s => Encoding.UTF8.GetByteCount(s),
                byte[] bytes => bytes.LongLength,
                bool => 1,
                char => 2,
                sbyte or byte => 1,
                short or ushort => 2,
                int or uint or float => 4,
                long or ulong or double => 8,
                decimal => 16,
                _ => UnknownTypedPayloadFallbackBytes,
            };
    }
}
