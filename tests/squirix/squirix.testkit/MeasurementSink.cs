using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization;

namespace Squirix.TestKit;

/// <summary>
/// A simple metrics sink based on <see cref="MeterListener" /> that captures
/// measurements from a specified meter for assertions in tests.
/// </summary>
public sealed class MeasurementSink : IDisposable
{
    private readonly ConcurrentQueue<CapturedMeasurement> _events = new();
    private readonly MeterListener _listener = new();
    private readonly string _meterName;

    /// <summary>
    /// Initializes a new instance of the <see cref="MeasurementSink" /> class that listens to the specified meter name.
    /// </summary>
    /// <param name="name">Meter name to subscribe to (e.g., "Squirix").</param>
    public MeasurementSink(string name)
    {
        _meterName = name;
        _listener.InstrumentPublished = OnInstrumentPublished;
        _listener.SetMeasurementEventCallback<long>(static (instrument, _, tags, state) => Enqueue(state, instrument.Name, tags));
        _listener.SetMeasurementEventCallback<double>(static (instrument, _, tags, state) => Enqueue(state, instrument.Name, tags));
        _listener.Start();
    }

    /// <summary>Determines whether a measurement event with the specified instrument name and tags has been observed.</summary>
    /// <param name="instrumentName">The instrument name (e.g., counter or histogram name).</param>
    /// <param name="tag1">First expected tag key/value pair.</param>
    /// <param name="tag2">Second expected tag key/value pair.</param>
    /// <returns><see langword="true" /> if a matching event was captured; otherwise, <see langword="false" />.</returns>
    public bool HasEvent(string instrumentName, (string Key, string Value) tag1, (string Key, string Value) tag2) => HasEventCore(_events, instrumentName, tag1, tag2);

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

    private static bool HasEventCore(ConcurrentQueue<CapturedMeasurement> events, string instrumentName, (string Key, string Value) tag1, (string Key, string Value) tag2)
    {
        foreach (var measurement in events)
        {
            if (!string.Equals(measurement.InstrumentName, instrumentName, StringComparison.OrdinalIgnoreCase))
                continue;

            if (HasTag(measurement, tag1.Key, tag1.Value) && HasTag(measurement, tag2.Key, tag2.Value))
                return true;
        }

        return false;
    }

    private static bool HasTag(CapturedMeasurement measurement, string key, string expectedValue)
    {
        if (measurement.OverflowTags is not null)
        {
            foreach (var tag in measurement.OverflowTags)
                if (string.Equals(tag.Key, key, StringComparison.OrdinalIgnoreCase) && TagValueEquals(tag.Value, expectedValue))
                    return true;

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

    private sealed record CapturedMeasurement
    {
        private readonly string? _tagKey0;
        private readonly string? _tagKey1;
        private readonly string? _tagKey2;
        private readonly object? _tagValue0;
        private readonly object? _tagValue1;
        private readonly object? _tagValue2;

        private CapturedMeasurement(string instrumentName, int tagCount, InlineTags? inline, KeyValuePair<string, object?>[]? overflowTags)
        {
            InstrumentName = instrumentName;
            TagCount = tagCount;
            _tagKey0 = inline?.Key0;
            _tagKey1 = inline?.Key1;
            _tagKey2 = inline?.Key2;
            _tagValue0 = inline?.Value0;
            _tagValue1 = inline?.Value1;
            _tagValue2 = inline?.Value2;
            OverflowTags = overflowTags;
        }

        internal string InstrumentName { get; }

        internal KeyValuePair<string, object?>[]? OverflowTags { get; }

        internal int TagCount { get; }

        internal static CapturedMeasurement Capture(string instrumentName, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            if (tags.Length is 0)
                return new CapturedMeasurement(instrumentName, 0, null, null);

            if (tags.Length <= 3)
            {
                var inline = new InlineTags(
                    tags.Length > 0 ? tags[0].Key : null,
                    tags.Length > 1 ? tags[1].Key : null,
                    tags.Length > 2 ? tags[2].Key : null,
                    tags.Length > 0 ? tags[0].Value : null,
                    tags.Length > 1 ? tags[1].Value : null,
                    tags.Length > 2 ? tags[2].Value : null);
                return new CapturedMeasurement(instrumentName, tags.Length, inline, null);
            }

            var overflow = new KeyValuePair<string, object?>[tags.Length];
            tags.CopyTo(overflow);
            return new CapturedMeasurement(instrumentName, tags.Length, null, overflow);
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

        private sealed record InlineTags
        {
            internal InlineTags(string? key0, string? key1, string? key2, object? value0, object? value1, object? value2)
            {
                Key0 = key0;
                Key1 = key1;
                Key2 = key2;
                Value0 = value0;
                Value1 = value1;
                Value2 = value2;
            }

            internal string? Key0 { get; }

            internal string? Key1 { get; }

            internal string? Key2 { get; }

            internal object? Value0 { get; }

            internal object? Value1 { get; }

            internal object? Value2 { get; }
        }
    }
}
