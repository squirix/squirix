using Squirix.Server.Core;
using Squirix.Server.Limits;
using Squirix.Server.Node.MemoryPressure;

namespace Squirix.Server.LocalCache;

/// <summary>
/// Entry-size estimator for <c>object?</c> cache values that uses journal-encoded entry size for complex payloads.
/// </summary>
internal sealed class ObjectCacheEntrySizeEstimator : ICacheEntrySizeEstimator<object?>
{
    private static readonly CacheEntry<object?> NullPayloadShell = new() { Value = null };
    private readonly CacheEntrySizeEstimator<object?> _typed = new();

    /// <inheritdoc />
    public long EstimateBytes(CacheKey key, CacheEntry<object?> entry, bool payloadIsCounter)
    {
        if (payloadIsCounter || entry.Value is null || !MemoryAdmissionPayloadClassifier.IsUnknownTypedPayloadEstimate(entry.Value))
            return _typed.EstimateBytes(key, entry, payloadIsCounter);

        var dictionaryOverhead = _typed.EstimateBytes(key, NullPayloadShell, false);
        return dictionaryOverhead + EntryPayloadSizeGuard.MeasureSerializedBytes(entry);
    }

    /// <inheritdoc />
    public bool HasUnknownPayloadMagnitude(CacheEntry<object?> entry, bool payloadIsCounter) => false;
}
