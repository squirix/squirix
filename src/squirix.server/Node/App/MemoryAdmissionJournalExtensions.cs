using System;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;

namespace Squirix.Server.Node.App;

/// <summary>Shared admission sizing helpers for journal-fronted caches.</summary>
internal static class MemoryAdmissionJournalExtensions
{
    /// <summary>Computes non-negative net growth for a replace/upsert style mutation using the same estimator inputs as accounting.</summary>
    /// <typeparam name="T">The cache value type.</typeparam>
    /// <param name="key">The cache key.</param>
    /// <param name="existing">The existing entry, if any.</param>
    /// <param name="proposed">The proposed entry.</param>
    /// <param name="estimator">The entry size estimator.</param>
    /// <param name="magnitudeUnknown">Set when growth cannot be bounded conservatively.</param>
    /// <param name="measuredProposedBytes">An exact estimate for the proposed entry from the same estimator, when available.</param>
    /// <returns>Estimated non-negative net growth in bytes.</returns>
    internal static long ComputeNetGrowthForReplace<T>(
        CacheKey key,
        NodeCacheEntry<T>? existing,
        NodeCacheEntry<T> proposed,
        ICacheEntrySizeEstimator<T> estimator,
        out bool magnitudeUnknown,
        long? measuredProposedBytes = null)
    {
        ArgumentNullException.ThrowIfNull(proposed);
        magnitudeUnknown = estimator.HasUnknownPayloadMagnitude(proposed, false) ||
                           (existing != null && estimator.HasUnknownPayloadMagnitude(existing, false));

        var nextBytes = measuredProposedBytes ?? estimator.EstimateBytes(key, proposed, false);
        if (existing == null)
            return nextBytes;

        var prevBytes = estimator.EstimateBytes(key, existing, false);
        var delta = nextBytes - prevBytes;
        return delta > 0 ? delta : 0;
    }
}
