using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>Entry-size estimator for <c>object?</c> cache values that uses journal-encoded entry size for complex payloads.</summary>
[Immutable]
internal sealed class ObjectCacheEntrySizeEstimator : ICacheEntrySizeEstimator<object?>
{
    private static readonly NodeCacheEntry<object?> NullPayloadShell = new() { Value = null };
    private readonly CacheEntrySizeEstimator<object?> _typed = new();

    /// <inheritdoc />
    public long EstimateBytes(CacheKey key, NodeCacheEntry<object?> entry, bool payloadIsCounter)
    {
        if (payloadIsCounter || entry.Value == null || !MemoryAdmissionPayloadClassifier.IsUnknownTypedPayloadEstimate(entry.Value))
            return _typed.EstimateBytes(key, entry, payloadIsCounter);

        var dictionaryOverhead = _typed.EstimateBytes(key, NullPayloadShell, false);
        return dictionaryOverhead + JournalEntryPayload.MeasureSerializedBytes(entry);
    }

    /// <inheritdoc />
    public bool HasUnknownPayloadMagnitude(NodeCacheEntry<object?> entry, bool payloadIsCounter) => false;
}
