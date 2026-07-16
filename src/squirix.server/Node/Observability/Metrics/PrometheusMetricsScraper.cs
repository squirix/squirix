using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;
using System.Threading;

namespace Squirix.Server.Node.Observability.Metrics;

internal sealed class PrometheusMetricsScraper : IDisposable
{
    internal static readonly PrometheusMetricsScraper Instance = new(false);
    private readonly Dictionary<string, Dictionary<string, double>> _last = new(StringComparer.Ordinal);
    private readonly MeterListener _listener;
    private readonly Lock _lock = new();
    private readonly Dictionary<string, Dictionary<string, double>> _sums = new(StringComparer.Ordinal);

    internal PrometheusMetricsScraper(bool isolated)
    {
        _listener = CreateListener();
        if (!isolated)
            _listener.Start();
    }

    public void Dispose() => _listener.Dispose();

    internal string Scrape(PrometheusScrapeProfile profile = PrometheusScrapeProfile.Public) => profile switch
    {
        PrometheusScrapeProfile.Public => ScrapePublic(),
        _ => throw new ArgumentOutOfRangeException(nameof(profile), "Unsupported scrape profile."),
    };

    private static void AppendExportedMetrics(StringBuilder sb, Dictionary<string, Dictionary<string, double>> exported, string? suffix)
    {
        foreach (var (metric, byLabels) in exported)
        {
            var metricName = suffix is null ? metric : metric + suffix;
            foreach (var (labels, value) in byLabels)
                AppendMetricLine(sb, metricName, labels, value);
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

    private static Dictionary<string, Dictionary<string, double>> CloneMetrics(Dictionary<string, Dictionary<string, double>> source)
    {
        var clone = new Dictionary<string, Dictionary<string, double>>(source.Count, StringComparer.Ordinal);
        foreach (var (metric, byLabels) in source)
            clone[metric] = new Dictionary<string, double>(byLabels, StringComparer.Ordinal);

        return clone;
    }

    private static int CountExportLines(Dictionary<string, Dictionary<string, double>> exported)
    {
        var count = 0;
        foreach (var pair in exported)
            count += pair.Value.Count;

        return count;
    }

    private static MeterListener CreateListener()
    {
        var listener = new MeterListener
        {
            InstrumentPublished = static (instrument, listener) =>
            {
                if (string.Equals(instrument.Meter.Name, "Squirix", StringComparison.OrdinalIgnoreCase))
                    listener.EnableMeasurementEvents(instrument);
            },
        };

        listener.SetMeasurementEventCallback<long>(static (inst, m, tags, _) => Instance.RecordMeasurement(inst.Name, tags, m));
        listener.SetMeasurementEventCallback<int>(static (inst, m, tags, _) => Instance.RecordMeasurement(inst.Name, tags, m));
        listener.SetMeasurementEventCallback<double>(static (inst, m, tags, _) => Instance.RecordMeasurement(inst.Name, tags, m));
        return listener;
    }

    private void RecordMeasurement(string metric, ReadOnlySpan<KeyValuePair<string, object?>> tags, double value)
    {
        var exportLabels = PrometheusScrapeLabelPolicy.BuildLabelKey(PrometheusScrapeLabelPolicy.FilterPublicTags(tags));
        lock (_lock)
        {
            if (!_sums.TryGetValue(metric, out var byLabels))
                _sums[metric] = byLabels = new Dictionary<string, double>(StringComparer.Ordinal);
            byLabels[exportLabels] = byLabels.GetValueOrDefault(exportLabels) + value;

            if (!_last.TryGetValue(metric, out var lastByLabels))
                _last[metric] = lastByLabels = new Dictionary<string, double>(StringComparer.Ordinal);
            lastByLabels[exportLabels] = Math.Max(lastByLabels.GetValueOrDefault(exportLabels), value);
        }
    }

    private string ScrapePublic()
    {
        var (exportedSums, exportedLast, lineCount) = SnapshotForExport();
        var sb = new StringBuilder(lineCount * 64);
        AppendExportedMetrics(sb, exportedSums, null);
        AppendExportedMetrics(sb, exportedLast, "_last");
        return sb.ToString();
    }

    private (Dictionary<string, Dictionary<string, double>> Sums, Dictionary<string, Dictionary<string, double>> Last, int LineCount) SnapshotForExport()
    {
        lock (_lock)
        {
            var exportedSums = CloneMetrics(_sums);
            var exportedLast = CloneMetrics(_last);
            return (exportedSums, exportedLast, CountExportLines(exportedSums) + CountExportLines(exportedLast));
        }
    }
}
