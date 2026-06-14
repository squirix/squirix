using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;

namespace Squirix.Server.TestKit;

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

    /// <summary>
    /// Determines whether a measurement event with the specified instrument name and tags has been observed.
    /// </summary>
    /// <param name="instrumentName">The instrument name (e.g., counter or histogram name).</param>
    /// <param name="expectedTags">Expected tag key/value pairs that must be present on the measurement.</param>
    /// <returns><c>true</c> if a matching event was captured; otherwise, <c>false</c>.</returns>
    public bool HasEvent(string instrumentName, params (string Key, string Value)[] expectedTags) => _events.Any(e =>
        string.Equals(e.InstrumentName, instrumentName, StringComparison.OrdinalIgnoreCase) && HasTags(e.Tags, expectedTags));

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
            if (!tags.Any(t => string.Equals(t.Key, k, StringComparison.OrdinalIgnoreCase) &&
                               string.Equals(t.Value?.ToString() ?? string.Empty, v, StringComparison.OrdinalIgnoreCase)))
            {
                return false;
            }
        }

        return true;
    }
}
