using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization;

namespace Squirix.Testing;

/// <summary>
/// A simple metrics sink based on <see cref="MeterListener" /> that captures
/// measurements from a specified meter for assertions in tests.
/// </summary>
public sealed class MeasurementSink : IDisposable
{
    private readonly ConcurrentQueue<CapturedMeasurement> _events = new();
    private readonly MeterListener _listener = new();
    private readonly Meter? _meter;
    private readonly string? _meterName;

    /// <summary>Initializes a new instance of the <see cref="MeasurementSink" /> class that listens to the specified meter name.</summary>
    /// <param name="name">Meter name to subscribe to (e.g., "Squirix").</param>
    public MeasurementSink(string name)
    {
        _meterName = name;
        _listener.InstrumentPublished = OnInstrumentPublished;
        _listener.SetMeasurementEventCallback<int>(static (instrument, _, tags, state) => Enqueue(state, instrument.Name, tags));
        _listener.SetMeasurementEventCallback<long>(static (instrument, _, tags, state) => Enqueue(state, instrument.Name, tags));
        _listener.SetMeasurementEventCallback<double>(static (instrument, _, tags, state) => Enqueue(state, instrument.Name, tags));
        _listener.Start();
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MeasurementSink" /> class that listens to measurements from the
    /// specified meter instance only.
    /// </summary>
    /// <param name="meter">Meter instance to subscribe to.</param>
    public MeasurementSink(Meter meter)
    {
        ArgumentNullException.ThrowIfNull(meter);
        _meter = meter;
        _listener.InstrumentPublished = OnInstrumentPublished;
        _listener.SetMeasurementEventCallback<int>(static (instrument, _, tags, state) => Enqueue(state, instrument.Name, tags));
        _listener.SetMeasurementEventCallback<long>(static (instrument, _, tags, state) => Enqueue(state, instrument.Name, tags));
        _listener.SetMeasurementEventCallback<double>(static (instrument, _, tags, state) => Enqueue(state, instrument.Name, tags));
        _listener.Start();
    }

    /// <summary>Determines whether an event with the instrument name and all expected tags was captured.</summary>
    /// <param name="instrumentName">The instrument name.</param>
    /// <param name="tags">Expected tags on the same event.</param>
    /// <returns>Whether a matching event was captured.</returns>
    public bool HasEvent(string instrumentName, params ReadOnlySpan<(string Key, string Value)> tags)
    {
        foreach (var measurement in _events)
        {
            if (string.Equals(measurement.InstrumentName, instrumentName, StringComparison.OrdinalIgnoreCase) && HasTags(in measurement, tags))
                return true;
        }

        return false;
    }

    /// <summary>Disposes the underlying <see cref="MeterListener" /> and releases resources.</summary>
    public void Dispose() => _listener.Dispose();

    private static void Enqueue(object? state, string instrumentName, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        if (state is not ConcurrentQueue<CapturedMeasurement> events)
            throw new InvalidOperationException("Measurement callback state must be a ConcurrentQueue<CapturedMeasurement>.");

        events.Enqueue(CapturedMeasurement.Capture(instrumentName, tags));
    }

    private static bool HasTags(in CapturedMeasurement measurement, ReadOnlySpan<(string Key, string Value)> tags)
    {
        foreach (var (key, value) in tags)
        {
            if (!MeasurementHasTag(key, value, in measurement))
                return false;
        }

        return true;
    }

    private static bool MeasurementHasTag(string key, string expectedValue, in CapturedMeasurement measurement)
    {
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
        if (_meter != null)
        {
            if (ReferenceEquals(instrument.Meter, _meter))
                listener.EnableMeasurementEvents(instrument, _events);
            return;
        }

        if (string.Equals(instrument.Meter.Name, _meterName, StringComparison.OrdinalIgnoreCase))
            listener.EnableMeasurementEvents(instrument, _events);
    }

    private readonly record struct CapturedMeasurement
    {
        internal readonly string InstrumentName;
        internal readonly KeyValuePair<string, object?>[]? OverflowTags;
        internal readonly int TagCount;
        private readonly InlineTags _inlineTags;

        private CapturedMeasurement(string instrumentName, int tagCount, in InlineTags inlineTags, KeyValuePair<string, object?>[]? overflowTags)
        {
            InstrumentName = instrumentName;
            TagCount = tagCount;
            _inlineTags = inlineTags;
            OverflowTags = overflowTags;
        }

        internal static CapturedMeasurement Capture(string instrumentName, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            if (tags.Length == 0)
                return new CapturedMeasurement(instrumentName, 0, default, null);

            if (tags.Length <= 3)
                return new CapturedMeasurement(instrumentName, tags.Length, new InlineTags(tags), null);

            var overflow = new KeyValuePair<string, object?>[tags.Length];
            tags.CopyTo(overflow);
            return new CapturedMeasurement(instrumentName, tags.Length, default, overflow);
        }

        internal void GetTag(int index, out string key, out object? value)
        {
            if (OverflowTags != null)
            {
                var tag = OverflowTags[index];
                key = tag.Key;
                value = tag.Value;
                return;
            }

            _inlineTags.GetTag(index, out key, out value);
        }

        private readonly record struct InlineTags
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
