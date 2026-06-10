using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;
using System.Threading;

namespace Squirix.Server.Node.Observability.Metrics;

internal sealed class PrometheusMetricsScraper : IDisposable
{
    public static readonly PrometheusMetricsScraper Instance = new();
    private readonly Dictionary<string, Dictionary<TagSet, double>> _last = new(StringComparer.Ordinal);
    private readonly MeterListener _listener;
    private readonly Lock _lock = new();
    private readonly Dictionary<string, Dictionary<TagSet, double>> _sums = new(StringComparer.Ordinal);

    internal PrometheusMetricsScraper(bool isolatedForTests)
    {
        if (!isolatedForTests)
            throw new InvalidOperationException("Test-only constructor.");

        _listener = CreateListener();
    }

    private PrometheusMetricsScraper()
    {
        _listener = CreateListener();
        _listener.Start();
    }

    public string Scrape(PrometheusScrapeProfile profile = PrometheusScrapeProfile.Public) => profile switch
    {
        PrometheusScrapeProfile.Public => ScrapePublic(),
        _ => throw new ArgumentOutOfRangeException(nameof(profile), profile, "Unsupported scrape profile."),
    };

    public void Dispose() => _listener.Dispose();

    internal void RecordMeasurementForTests(string metric, KeyValuePair<string, object?>[] tags, double value) =>
        RecordMeasurement(metric, tags, value);

    private static void AggregateForExport(
        Dictionary<string, Dictionary<TagSet, double>> source,
        Dictionary<string, Dictionary<string, double>> destination,
        bool sumValues)
    {
        foreach (var (metric, byTags) in source)
        {
            foreach (var (tags, value) in byTags)
            {
                var exportLabels = PrometheusScrapeLabelPolicy.BuildLabelKey(PrometheusScrapeLabelPolicy.FilterPublicTags(tags.Tags));
                if (!destination.TryGetValue(metric, out var byLabels))
                    destination[metric] = byLabels = new Dictionary<string, double>(StringComparer.Ordinal);

                if (sumValues)
                    byLabels[exportLabels] = byLabels.GetValueOrDefault(exportLabels) + value;
                else
                    byLabels[exportLabels] = Math.Max(byLabels.GetValueOrDefault(exportLabels), value);
            }
        }
    }

    private static void AppendMetricLine(StringBuilder sb, string metric, string labels, double value)
    {
        _ = sb.Append(metric);
        if (labels.Length > 0)
        {
            _ = sb.Append('{');
            _ = sb.Append(labels);
            _ = sb.Append('}');
        }

        _ = sb.Append(' ');
        _ = sb.Append(value.ToString(CultureInfo.InvariantCulture));
        _ = sb.Append('\n');
    }

    private static MeterListener CreateListener()
    {
        var listener = new MeterListener
        {
            InstrumentPublished = static (instrument, listener) =>
            {
                if (instrument.Meter.Name == "Squirix")
                    listener.EnableMeasurementEvents(instrument);
            },
        };

        listener.SetMeasurementEventCallback<long>(static (inst, m, tags, _) => Instance.RecordMeasurement(inst.Name, tags, m));
        listener.SetMeasurementEventCallback<int>(static (inst, m, tags, _) => Instance.RecordMeasurement(inst.Name, tags, m));
        listener.SetMeasurementEventCallback<double>(static (inst, m, tags, _) => Instance.RecordMeasurement(inst.Name, tags, m));
        return listener;
    }

    private string ScrapePublic()
    {
        var exportedSums = new Dictionary<string, Dictionary<string, double>>(StringComparer.Ordinal);
        var exportedLast = new Dictionary<string, Dictionary<string, double>>(StringComparer.Ordinal);

        lock (_lock)
        {
            AggregateForExport(_sums, exportedSums, sumValues: true);
            AggregateForExport(_last, exportedLast, sumValues: false);
        }

        var sb = new StringBuilder();
        foreach (var (metric, byLabels) in exportedSums)
        {
            foreach (var (labels, value) in byLabels)
                AppendMetricLine(sb, metric, labels, value);
        }

        foreach (var (metric, byLabels) in exportedLast)
        {
            foreach (var (labels, value) in byLabels)
                AppendMetricLine(sb, metric + "_last", labels, value);
        }

        return sb.ToString();
    }

    private void RecordMeasurement(string metric, ReadOnlySpan<KeyValuePair<string, object?>> tags, double value)
    {
        var tagSet = new TagSet(tags);
        lock (_lock)
        {
            if (!_sums.TryGetValue(metric, out var byTags))
                _sums[metric] = byTags = new Dictionary<TagSet, double>();
            byTags[tagSet] = byTags.GetValueOrDefault(tagSet) + value;

            if (!_last.TryGetValue(metric, out var lastByTags))
                _last[metric] = lastByTags = new Dictionary<TagSet, double>();
            lastByTags[tagSet] = value;
        }
    }

    private readonly struct TagSet : IEquatable<TagSet>
    {
        private readonly KeyValuePair<string, object?>[] _tags;

        public TagSet(ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            _tags = tags.ToArray();
            Array.Sort(_tags, static (a, b) => string.CompareOrdinal(a.Key, b.Key));
        }

        public ReadOnlySpan<KeyValuePair<string, object?>> Tags => _tags;

        public bool Equals(TagSet other)
        {
            if (_tags.Length != other._tags.Length)
                return false;

            for (var i = 0; i < _tags.Length; i++)
            {
                if (!string.Equals(_tags[i].Key, other._tags[i].Key, StringComparison.Ordinal))
                    return false;

                if (!Equals(_tags[i].Value, other._tags[i].Value))
                    return false;
            }

            return true;
        }

        public override bool Equals(object? obj) => obj is TagSet other && Equals(other);

        public override int GetHashCode()
        {
            var hash = default(HashCode);
            foreach (var tag in _tags)
            {
                hash.Add(tag.Key, StringComparer.Ordinal);
                hash.Add(tag.Value);
            }

            return hash.ToHashCode();
        }
    }
}
