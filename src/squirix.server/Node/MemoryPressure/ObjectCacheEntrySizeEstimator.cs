using System;
using System.Globalization;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>Entry-size estimator for <c language="csharp">object?</c> cache values that uses journal-encoded entry size for complex payloads.</summary>
/// <remarks>
/// For a complex payload the overhead is the typed estimate of an entry with no value, deadline or tags, because the encoded length already
/// holds the value, the deadline field and the tags; the deadline index share is added on top for an entry with a deadline.
/// </remarks>
[Immutable]
internal sealed class ObjectCacheEntrySizeEstimator : ICacheEntrySizeEstimator<object?>
{
    private static readonly NodeCacheEntry<object?> NullPayloadShell = new() { Value = null };
    private readonly CacheEntrySizeEstimator<object?> _typed = new();

    /// <inheritdoc />
    public long EstimateBytes(CacheKey key, NodeCacheEntry<object?> entry, bool payloadIsCounter) =>
        payloadIsCounter || entry.Value == null || !MemoryAdmissionPayloadClassifier.IsUnknownTypedPayloadEstimate(entry.Value)
            ? _typed.EstimateBytes(key, entry, payloadIsCounter)
            : EstimateEncodedBytes(key, entry, JournalEntryPayload.MeasureSerializedBytes(entry));

    /// <inheritdoc />
    public long EstimateBytesForEncodedLength(CacheKey key, NodeCacheEntry<object?> entry, int knownEncodedLength) =>
        entry.Value == null || !MemoryAdmissionPayloadClassifier.IsUnknownTypedPayloadEstimate(entry.Value)
            ? _typed.EstimateBytes(key, entry, false)
            : EstimateEncodedBytes(key, entry, knownEncodedLength);

    /// <inheritdoc />
    public long MeasureValue(object? value) =>
        MemoryAdmissionPayloadClassifier.IsUnknownTypedPayloadEstimate(value) ? JournalEntryPayload.MeasureValueBytes(value) : _typed.MeasureValue(value);

    /// <inheritdoc />
    public long EstimateBytesForMeasuredValue(CacheKey key, NodeCacheEntry<object?> entry, long measuredValue) =>
        MemoryAdmissionPayloadClassifier.IsUnknownTypedPayloadEstimate(entry.Value)
            ? EstimateEncodedBytes(key, entry, JournalEntryPayload.MeasureSerializedBytes(entry, Convert.ToInt32(measuredValue, CultureInfo.InvariantCulture)))
            : _typed.EstimateBytesForMeasuredValue(key, entry, measuredValue);

    /// <inheritdoc />
    public bool HasUnknownPayloadMagnitude(NodeCacheEntry<object?> entry, bool isCounter) => false;

    private long EstimateEncodedBytes(CacheKey key, NodeCacheEntry<object?> entry, int encodedLength) =>
        _typed.EstimateBytes(key, NullPayloadShell, false) + encodedLength + CacheEntrySizeEstimator<object?>.DeadlineIndexBytesOf(entry);
}
