using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Globalization;
using Squirix.Attributes;

namespace Squirix.Server.TestKit;

/// <summary>
/// A simple metrics sink based on <see cref="MeterListener" /> that captures
/// measurements from a specified meter for assertions in tests.
/// </summary>
[Immutable]
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
            if (string.Equals(measurement.InstrumentName, instrumentName, StringComparison.OrdinalIgnoreCase) && MeasurementHasTag(tag1.Key, tag1.Value, in measurement))
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

            if (MeasurementHasTag(tag1.Key, tag1.Value, in measurement) && MeasurementHasTag(tag2.Key, tag2.Value, in measurement))
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

            if (MeasurementHasTag(tag1.Key, tag1.Value, in measurement) && MeasurementHasTag(tag2.Key, tag2.Value, in measurement) &&
                MeasurementHasTag(tag3.Key, tag3.Value, in measurement))
                return true;
        }

        return false;
    }

    private static bool MeasurementHasTag(string key, string expectedValue, in CapturedMeasurement measurement)
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

    [Immutable]
    private readonly struct CapturedMeasurement : IEquatable<CapturedMeasurement>
    {
        internal readonly string InstrumentName;
        internal readonly KeyValuePair<string, object?>[]? OverflowTags;
        internal readonly int TagCount;
        private readonly InlineTags _inlineTags;

        private CapturedMeasurement(
            string instrumentName,
            int tagCount,
            InlineTags inlineTags,
            KeyValuePair<string, object?>[]? overflowTags)
        {
            InstrumentName = instrumentName;
            TagCount = tagCount;
            _inlineTags = inlineTags;
            OverflowTags = overflowTags;
        }

        public static bool operator ==(CapturedMeasurement left, CapturedMeasurement right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(CapturedMeasurement left, CapturedMeasurement right)
        {
            return !left.Equals(right);
        }

        public bool Equals(CapturedMeasurement other)
        {
            return string.Equals(InstrumentName, other.InstrumentName, StringComparison.Ordinal) &&
                TagCount == other.TagCount &&
                _inlineTags.Equals(other._inlineTags) &&
                Equals(OverflowTags, other.OverflowTags);
        }

        public override bool Equals([NotNullWhen(true)] object? obj) => obj is CapturedMeasurement other && Equals(other);

        public override int GetHashCode() => HashCode.Combine(InstrumentName, TagCount, _inlineTags, OverflowTags);

        internal static CapturedMeasurement Capture(string instrumentName, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            if (tags.Length is 0)
                return new CapturedMeasurement(instrumentName, 0, default, null);

            if (tags.Length <= 3)
            {
                return new CapturedMeasurement(
                    instrumentName,
                    tags.Length,
                    new InlineTags(tags),
                    null);
            }

            var overflow = new KeyValuePair<string, object?>[tags.Length];
            tags.CopyTo(overflow);
            return new CapturedMeasurement(instrumentName, tags.Length, default, overflow);
        }

        internal void GetTag(int index, out string key, out object? value)
        {
            if (OverflowTags is not null)
            {
                var tag = OverflowTags[index];
                key = tag.Key;
                value = tag.Value;
                return;
            }

            _inlineTags.GetTag(index, out key, out value);
        }

        [Immutable]
        private readonly struct InlineTags : IEquatable<InlineTags>
        {
            private readonly string? _key0;
            private readonly string? _key1;
            private readonly string? _key2;
            private readonly object? _value0;
            private readonly object? _value1;
            private readonly object? _value2;

            internal InlineTags(ReadOnlySpan<KeyValuePair<string, object?>> tags)
            {
                _key0 = tags.Length > 0 ? tags[0].Key : null;
                _key1 = tags.Length > 1 ? tags[1].Key : null;
                _key2 = tags.Length > 2 ? tags[2].Key : null;
                _value0 = tags.Length > 0 ? tags[0].Value : null;
                _value1 = tags.Length > 1 ? tags[1].Value : null;
                _value2 = tags.Length > 2 ? tags[2].Value : null;
            }

            public bool Equals(InlineTags other)
            {
                return string.Equals(_key0, other._key0, StringComparison.Ordinal) &&
                    string.Equals(_key1, other._key1, StringComparison.Ordinal) &&
                    string.Equals(_key2, other._key2, StringComparison.Ordinal) &&
                    Equals(_value0, other._value0) &&
                    Equals(_value1, other._value1) &&
                    Equals(_value2, other._value2);
            }

            public override bool Equals([NotNullWhen(true)] object? obj) => obj is InlineTags other && Equals(other);

            public override int GetHashCode() => HashCode.Combine(_key0, _key1, _key2, _value0, _value1, _value2);

            internal void GetTag(int index, out string key, out object? value)
            {
                switch (index)
                {
                    case 0:
                        key = _key0 ?? string.Empty;
                        value = _value0;
                        return;
                    case 1:
                        key = _key1 ?? string.Empty;
                        value = _value1;
                        return;
                    case 2:
                        key = _key2 ?? string.Empty;
                        value = _value2;
                        return;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(index));
                }
            }
        }
    }
}
