using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization;

namespace Squirix.Server.TestKit.Diagnostics;

/// <summary>
/// A simple metrics sink based on <see cref="MeterListener" /> that captures
/// measurements from a specified meter for assertions in tests.
/// </summary>
public sealed class MeasurementSink : IDisposable
{
    private readonly ConcurrentQueue<(string InstrumentName, object Value, KeyValuePair<string, object?>[] Tags)> _events = new();
    private readonly MeterListener _listener = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="MeasurementSink" /> class that listens to the specified meter name.
    /// </summary>
    /// <param name="name">Meter name to subscribe to (e.g., "Squirix").</param>
    public MeasurementSink(string name)
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (string.Equals(instrument.Meter.Name, name, StringComparison.OrdinalIgnoreCase))
                listener.EnableMeasurementEvents(instrument);
        };

        _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => { _events.Enqueue((instrument.Name, value, CloneTags(tags))); });
        _listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => { _events.Enqueue((instrument.Name, value, CloneTags(tags))); });

        _listener.Start();
    }

    /// <summary>Determines whether a measurement event with the specified instrument name and tags has been observed.</summary>
    /// <param name="instrumentName">The instrument name (e.g., counter or histogram name).</param>
    /// <param name="expectedTags">Expected tag key/value pairs that must be present on the measurement.</param>
    /// <returns><see langword="true"/> if a matching event was captured; otherwise, <see langword="false"/>.</returns>
    public bool HasEvent(string instrumentName, params (string Key, string Value)[] expectedTags)
    {
        ArgumentNullException.ThrowIfNull(expectedTags);

        foreach (var (eventInstrumentName, _, eventTags) in _events)
        {
            if (string.Equals(eventInstrumentName, instrumentName, StringComparison.OrdinalIgnoreCase) && HasTags(eventTags, expectedTags))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Disposes the underlying <see cref="MeterListener" /> and releases resources.
    /// </summary>
    public void Dispose() => _listener.Dispose();

    private static KeyValuePair<string, object?>[] CloneTags(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        var arr = new KeyValuePair<string, object?>[tags.Length];
        for (var i = 0; i < tags.Length; i++)
            arr[i] = tags[i];
        return arr;
    }

    private static bool HasTags(KeyValuePair<string, object?>[] tags, (string Key, string Value)[] expected)
    {
        foreach (var (k, v) in expected)
        {
            var found = false;
            foreach (var tag in tags)
            {
                if (!string.Equals(tag.Key, k, StringComparison.OrdinalIgnoreCase) || !TagValueEquals(tag.Value, v))
                    continue;
                found = true;
                break;
            }

            if (!found)
                return false;
        }

        return true;
    }

    private static bool TagValueEquals(object? tagValue, string expected) =>
        tagValue switch
        {
            null => string.IsNullOrEmpty(expected),
            string s => string.Equals(s, expected, StringComparison.OrdinalIgnoreCase),
            _ => string.Equals(Convert.ToString(tagValue, CultureInfo.InvariantCulture), expected, StringComparison.OrdinalIgnoreCase),
        };
}
