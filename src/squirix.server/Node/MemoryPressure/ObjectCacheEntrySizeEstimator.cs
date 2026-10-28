using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>Entry-size estimator for <c language="csharp">object?</c> cache values that sizes a complex payload as the JSON value it is stored and recovered as.</summary>
/// <remarks>
/// A structured value arrives as <see cref="JsonUtf8Value" />, whose size follows from the length of its bytes. A <see cref="System.Text.Json.JsonElement" />
/// or an arbitrary object is first normalized to the form the journal stores, so an entry weighs the same before and after a restart and on a follower.
/// </remarks>
[Immutable]
internal sealed class ObjectCacheEntrySizeEstimator : ICacheEntrySizeEstimator<object?>
{
    private readonly CacheEntrySizeEstimator<object?> _typed = new();

    /// <inheritdoc />
    public long EstimateBytes(CacheKey key, NodeCacheEntry<object?> entry, bool payloadIsCounter) =>
        payloadIsCounter || entry.Value == null || !MemoryAdmissionPayloadClassifier.IsUnknownTypedPayloadEstimate(entry.Value)
            ? _typed.EstimateBytes(key, entry, payloadIsCounter)
            : _typed.EstimateBytesForMeasuredValue(key, entry, MeasureValue(entry.Value));

    /// <inheritdoc />
    /// <remarks>The estimate derives from the value, so the known length is not used.</remarks>
    public long EstimateBytesForEncodedLength(CacheKey key, NodeCacheEntry<object?> entry, int knownEncodedLength) => EstimateBytes(key, entry, false);

    /// <inheritdoc />
    public long MeasureValue(object? value) =>
        MemoryAdmissionPayloadClassifier.IsUnknownTypedPayloadEstimate(value) ? _typed.MeasureValue(NodeCacheEntry<object?>.NormalizeValue(value)) : _typed.MeasureValue(value);

    /// <inheritdoc />
    public long EstimateBytesForMeasuredValue(CacheKey key, NodeCacheEntry<object?> entry, long measuredValue) => _typed.EstimateBytesForMeasuredValue(key, entry, measuredValue);

    /// <inheritdoc />
    public bool HasUnknownPayloadMagnitude(NodeCacheEntry<object?> entry, bool isCounter) => false;
}
