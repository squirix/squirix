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

    /// <summary>Determines whether a measurement event with the specified instrument name has been observed.</summary>
    /// <param name="instrumentName">The instrument name (e.g., counter or histogram name).</param>
    /// <returns><see langword="true"/> if a matching event was captured; otherwise, <see langword="false"/>.</returns>
    public bool HasEvent(string instrumentName) => HasEventCore(_events, instrumentName, ReadOnlySpan<(string Key, string Value)>.Empty);

    /// <summary>Determines whether a measurement event with the specified instrument name and tags has been observed.</summary>
    /// <param name="instrumentName">The instrument name (e.g., counter or histogram name).</param>
    /// <param name="tag1">Expected tag key/value pair that must be present on the measurement.</param>
    /// <returns><see langword="true"/> if a matching event was captured; otherwise, <see langword="false"/>.</returns>
    public bool HasEvent(string instrumentName, (string Key, string Value) tag1) => HasEventCore(_events, instrumentName, [tag1]);

    /// <summary>Determines whether a measurement event with the specified instrument name and tags has been observed.</summary>
    /// <param name="instrumentName">The instrument name (e.g., counter or histogram name).</param>
    /// <param name="tag1">First expected tag key/value pair.</param>
    /// <param name="tag2">Second expected tag key/value pair.</param>
    /// <returns><see langword="true"/> if a matching event was captured; otherwise, <see langword="false"/>.</returns>
    public bool HasEvent(string instrumentName, (string Key, string Value) tag1, (string Key, string Value) tag2) => HasEventCore(_events, instrumentName, [tag1, tag2]);

    /// <summary>Determines whether a measurement event with the specified instrument name and tags has been observed.</summary>
    /// <param name="instrumentName">The instrument name (e.g., counter or histogram name).</param>
    /// <param name="tag1">First expected tag key/value pair.</param>
    /// <param name="tag2">Second expected tag key/value pair.</param>
    /// <param name="tag3">Third expected tag key/value pair.</param>
    /// <returns><see langword="true"/> if a matching event was captured; otherwise, <see langword="false"/>.</returns>
    public bool HasEvent(string instrumentName, (string Key, string Value) tag1, (string Key, string Value) tag2, (string Key, string Value) tag3) =>
        HasEventCore(_events, instrumentName, [tag1, tag2, tag3]);

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

    private static bool HasTags(KeyValuePair<string, object?>[] tags, ReadOnlySpan<(string Key, string Value)> expected)
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

    private static bool HasEventCore(
        ConcurrentQueue<(string InstrumentName, object Value, KeyValuePair<string, object?>[] Tags)> events,
        string instrumentName,
        ReadOnlySpan<(string Key, string Value)> expectedTags)
    {
        foreach (var (eventInstrumentName, _, eventTags) in events)
        {
            if (string.Equals(eventInstrumentName, instrumentName, StringComparison.OrdinalIgnoreCase) && HasTags(eventTags, expectedTags))
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
}
