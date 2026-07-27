using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization;

namespace Squirix.Server.TestKit;

/// <summary>
/// A simple metrics sink based on <see cref="MeterListener" /> that captures
/// measurements from a specified meter for assertions in tests.
/// </summary>
public sealed class NodeMeasurementSink : IDisposable
{
    private readonly ConcurrentQueue<CapturedMeasurement> _events = new();
    private readonly MeterListener _listener = new();
    private readonly string _meterName;

    /// <summary>
    /// Initializes a new instance of the <see cref="NodeMeasurementSink" /> class that listens to the specified meter name.
    /// </summary>
    /// <param name="name">Meter name to subscribe to (e.g., "Squirix").</param>
    public NodeMeasurementSink(string name)
    {
        _meterName = name;
        _listener.InstrumentPublished = OnInstrumentPublished;
        _listener.SetMeasurementEventCallback<long>(static (instrument, _, tags, state) => Enqueue(state, instrument.Name, tags));
        _listener.SetMeasurementEventCallback<double>(static (instrument, _, tags, state) => Enqueue(state, instrument.Name, tags));
        _listener.Start();
    }

    /// <summary>Determines whether a measurement event with the specified instrument name has been observed.</summary>
    /// <param name="instrumentName">The instrument name (e.g., counter or histogram name).</param>
    /// <returns><see langword="true" /> if a matching event was captured; otherwise, <see langword="false" />.</returns>
    public bool HasEvent(string instrumentName) => HasEventCore(_events, instrumentName);

    /// <summary>Determines whether a measurement event with the specified instrument name and tags has been observed.</summary>
    /// <param name="instrumentName">The instrument name (e.g., counter or histogram name).</param>
    /// <param name="tag1">Expected tag key/value pair that must be present on the measurement.</param>
    /// <returns><see langword="true" /> if a matching event was captured; otherwise, <see langword="false" />.</returns>
    public bool HasEvent(string instrumentName, (string Key, string Value) tag1) => HasEventCore(_events, instrumentName, tag1);

    /// <summary>Determines whether a measurement event with the specified instrument name and tags has been observed.</summary>
    /// <param name="instrumentName">The instrument name (e.g., counter or histogram name).</param>
    /// <param name="tag1">First expected tag key/value pair.</param>
    /// <param name="tag2">Second expected tag key/value pair.</param>
    /// <returns><see langword="true" /> if a matching event was captured; otherwise, <see langword="false" />.</returns>
    public bool HasEvent(string instrumentName, (string Key, string Value) tag1, (string Key, string Value) tag2) => HasEventCore(_events, instrumentName, tag1, tag2);

    /// <summary>Determines whether a measurement event with the specified instrument name and tags has been observed.</summary>
    /// <param name="instrumentName">The instrument name (e.g., counter or histogram name).</param>
    /// <param name="tag1">First expected tag key/value pair.</param>
    /// <param name="tag2">Second expected tag key/value pair.</param>
    /// <param name="tag3">Third expected tag key/value pair.</param>
    /// <returns><see langword="true" /> if a matching event was captured; otherwise, <see langword="false" />.</returns>
    public bool HasEvent(string instrumentName, (string Key, string Value) tag1, (string Key, string Value) tag2, (string Key, string Value) tag3) =>
        HasEventCore(_events, instrumentName, tag1, tag2, tag3);

    /// <summary>
    /// Disposes the underlying <see cref="MeterListener" /> and releases resources.
    /// </summary>
    public void Dispose() => _listener.Dispose();

    private static void Enqueue(object? state, string instrumentName, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        if (state is not ConcurrentQueue<CapturedMeasurement> events)
            throw new InvalidOperationException("Measurement callback state must be a ConcurrentQueue<CapturedMeasurement>.");

        events.Enqueue(CapturedMeasurement.Capture(instrumentName, tags));
    }

    private static bool HasEventCore(ConcurrentQueue<CapturedMeasurement> events, string instrumentName)
    {
        foreach (var measurement in events)
        {
            if (string.Equals(measurement.InstrumentName, instrumentName, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    private static bool HasEventCore(ConcurrentQueue<CapturedMeasurement> events, string instrumentName, (string Key, string Value) tag1)
    {
        foreach (var measurement in events)
        {
            if (string.Equals(measurement.InstrumentName, instrumentName, StringComparison.OrdinalIgnoreCase) && MeasurementHasTag(in measurement, tag1.Key, tag1.Value))
                return true;
        }

        return false;
    }

    private static bool HasEventCore(ConcurrentQueue<CapturedMeasurement> events, string instrumentName, (string Key, string Value) tag1, (string Key, string Value) tag2)
    {
        foreach (var measurement in events)
        {
            if (!string.Equals(measurement.InstrumentName, instrumentName, StringComparison.OrdinalIgnoreCase))
                continue;

            if (MeasurementHasTag(in measurement, tag1.Key, tag1.Value) && MeasurementHasTag(in measurement, tag2.Key, tag2.Value))
                return true;
        }

        return false;
    }

    private static bool HasEventCore(
        ConcurrentQueue<CapturedMeasurement> events,
        string instrumentName,
        (string Key, string Value) tag1,
        (string Key, string Value) tag2,
        (string Key, string Value) tag3)
    {
        foreach (var measurement in events)
        {
            if (!string.Equals(measurement.InstrumentName, instrumentName, StringComparison.OrdinalIgnoreCase))
                continue;

            if (MeasurementHasTag(in measurement, tag1.Key, tag1.Value) && MeasurementHasTag(in measurement, tag2.Key, tag2.Value) &&
                MeasurementHasTag(in measurement, tag3.Key, tag3.Value))
                return true;
        }

        return false;
    }

    private static bool MeasurementHasTag(in CapturedMeasurement measurement, string key, string expectedValue)
    {
        if (measurement.OverflowTags is not null)
        {
            foreach (var tag in measurement.OverflowTags)
            {
                if (string.Equals(tag.Key, key, StringComparison.OrdinalIgnoreCase) && TagValueEquals(tag.Value, expectedValue))
                    return true;
            }

            return false;
        }

        for (var i = 0; i < measurement.TagCount; i++)
        {
            measurement.GetTag(i, out var tagKey, out var tagValue);
            if (string.Equals(tagKey, key, StringComparison.OrdinalIgnoreCase) && TagValueEquals(tagValue, expectedValue))
                return true;
        }

        return false;
    }

    private static bool TagValueEquals(object? tagValue, string expected) => tagValue switch
    {
        null => string.IsNullOrEmpty(expected),
        string s => string.Equals(s, expected, StringComparison.OrdinalIgnoreCase),
        _ => string.Equals(Convert.ToString(tagValue, CultureInfo.InvariantCulture), expected, StringComparison.OrdinalIgnoreCase),
    };

    private void OnInstrumentPublished(Instrument instrument, MeterListener listener)
    {
        if (string.Equals(instrument.Meter.Name, _meterName, StringComparison.OrdinalIgnoreCase))
            listener.EnableMeasurementEvents(instrument, _events);
    }

    private readonly struct CapturedMeasurement
    {
        private readonly string? _tagKey0;
        private readonly string? _tagKey1;
        private readonly string? _tagKey2;
        private readonly object? _tagValue0;
        private readonly object? _tagValue1;
        private readonly object? _tagValue2;

        private CapturedMeasurement(
            string instrumentName,
            int tagCount,
            string? tagKey0,
            string? tagKey1,
            string? tagKey2,
            object? tagValue0,
            object? tagValue1,
            object? tagValue2,
            KeyValuePair<string, object?>[]? overflowTags)
        {
            InstrumentName = instrumentName;
            TagCount = tagCount;
            _tagKey0 = tagKey0;
            _tagKey1 = tagKey1;
            _tagKey2 = tagKey2;
            _tagValue0 = tagValue0;
            _tagValue1 = tagValue1;
            _tagValue2 = tagValue2;
            OverflowTags = overflowTags;
        }

        internal string InstrumentName { get; }

        internal KeyValuePair<string, object?>[]? OverflowTags { get; }

        internal int TagCount { get; }

        internal static CapturedMeasurement Capture(string instrumentName, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            if (tags.Length is 0)
                return new CapturedMeasurement(instrumentName, 0, null, null, null, null, null, null, null);

            if (tags.Length <= 3)
            {
                return new CapturedMeasurement(
                    instrumentName,
                    tags.Length,
                    tags.Length > 0 ? tags[0].Key : null,
                    tags.Length > 1 ? tags[1].Key : null,
                    tags.Length > 2 ? tags[2].Key : null,
                    tags.Length > 0 ? tags[0].Value : null,
                    tags.Length > 1 ? tags[1].Value : null,
                    tags.Length > 2 ? tags[2].Value : null,
                    null);
            }

            var overflow = new KeyValuePair<string, object?>[tags.Length];
            tags.CopyTo(overflow);
            return new CapturedMeasurement(instrumentName, tags.Length, null, null, null, null, null, null, overflow);
        }

        internal void GetTag(int index, out string key, out object? value)
        {
            switch (index)
            {
                case 0:
                    key = _tagKey0 ?? string.Empty;
                    value = _tagValue0;
                    return;
                case 1:
                    key = _tagKey1 ?? string.Empty;
                    value = _tagValue1;
                    return;
                case 2:
                    key = _tagKey2 ?? string.Empty;
                    value = _tagValue2;
                    return;
                default:
                    throw new ArgumentOutOfRangeException(nameof(index));
            }
        }
    }
}
