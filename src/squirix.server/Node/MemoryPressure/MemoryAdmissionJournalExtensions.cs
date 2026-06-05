using System;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>
/// Shared admission sizing helpers for journal-fronted caches.
/// </summary>
internal static class MemoryAdmissionJournalExtensions
{
    /// <summary>
    /// Computes non-negative net growth for a replace/upsert style mutation using the same estimator inputs as accounting.
    /// </summary>
    /// <typeparam name="T">The cache value type.</typeparam>
    /// <param name="key">The cache key.</param>
    /// <param name="existing">The existing entry, if any.</param>
    /// <param name="existingPayloadIsCounter">Whether the existing payload is stored as a counter.</param>
    /// <param name="proposed">The proposed entry.</param>
    /// <param name="proposedPayloadIsCounter">Whether the proposed payload is a counter.</param>
    /// <param name="estimator">The entry size estimator.</param>
    /// <param name="magnitudeUnknown">Set when growth cannot be bounded conservatively.</param>
    /// <returns>Estimated non-negative net growth in bytes.</returns>
    public static long ComputeNetGrowthForReplace<T>(
        CacheKey key,
        CacheEntry<T>? existing,
        bool existingPayloadIsCounter,
        CacheEntry<T> proposed,
        bool proposedPayloadIsCounter,
        ICacheEntrySizeEstimator<T> estimator,
        out bool magnitudeUnknown)
    {
        ArgumentNullException.ThrowIfNull(proposed);
        magnitudeUnknown = MemoryAdmissionPayloadClassifier.IsUnknownTypedPayloadEstimate(proposed.Value) ||
                           (existing is not null && MemoryAdmissionPayloadClassifier.IsUnknownTypedPayloadEstimate(existing.Value));

        var nextBytes = estimator.EstimateBytes(key, proposed, proposedPayloadIsCounter);
        if (existing is null)
            return nextBytes;

        var prevBytes = estimator.EstimateBytes(key, existing, existingPayloadIsCounter);
        var delta = nextBytes - prevBytes;
        return delta > 0 ? delta : 0;
    }
}
