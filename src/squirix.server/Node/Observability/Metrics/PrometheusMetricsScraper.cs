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
    private readonly Dictionary<string, Dictionary<string, double>> _last = new(StringComparer.Ordinal);
    private readonly MeterListener _listener;
    private readonly Lock _lock = new();
    private readonly Dictionary<string, Dictionary<string, double>> _sums = new(StringComparer.Ordinal);

    private PrometheusMetricsScraper()
    {
        _listener = new MeterListener
        {
            InstrumentPublished = static (instrument, listener) =>
            {
                if (instrument.Meter.Name == "Squirix")
                    listener.EnableMeasurementEvents(instrument);
            },
        };

        _listener.SetMeasurementEventCallback<long>(static (inst, m, tags, _) => Instance.RecordMeasurement(inst.Name, tags, m));
        _listener.SetMeasurementEventCallback<int>(static (inst, m, tags, _) => Instance.RecordMeasurement(inst.Name, tags, m));
        _listener.SetMeasurementEventCallback<double>(static (inst, m, tags, _) => Instance.RecordMeasurement(inst.Name, tags, m));

        _listener.Start();
    }

    public string Scrape()
    {
        var sb = new StringBuilder();
        lock (_lock)
        {
            foreach (var (metric, byLabels) in _sums)
            {
                foreach (var (labels, value) in byLabels)
                {
                    AppendMetricLine(sb, metric, labels, value);
                }
            }

            foreach (var (metric, byLabels) in _last)
            {
                foreach (var (labels, value) in byLabels)
                {
                    AppendMetricLine(sb, metric + "_last", labels, value);
                }
            }
        }

        return sb.ToString();
    }

    public void Dispose() => _listener.Dispose();

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

    private static string BuildLabelKey(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        if (tags.Length == 0)
            return string.Empty;

        var arr = tags.ToArray();
        Array.Sort(arr, static (a, b) => string.CompareOrdinal(a.Key, b.Key));
        var sb = new StringBuilder();
        for (var i = 0; i < arr.Length; i++)
        {
            if (i > 0)
                _ = sb.Append(',');
            _ = sb.Append(arr[i].Key);
            _ = sb.Append("=\"");
            _ = sb.Append(Escape(arr[i].Value?.ToString() ?? string.Empty));
            _ = sb.Append('"');
        }

        return sb.ToString();
    }

    private static string Escape(string s) => s.Replace("\\", @"\\").Replace("\n", "\\n").Replace("\"", "\\\"");

    private void RecordMeasurement(string metric, ReadOnlySpan<KeyValuePair<string, object?>> tags, double value)
    {
        var labelKey = BuildLabelKey(tags);
        lock (_lock)
        {
            if (!_sums.TryGetValue(metric, out var byLabels))
                _sums[metric] = byLabels = new Dictionary<string, double>(StringComparer.Ordinal);
            byLabels[labelKey] = byLabels.GetValueOrDefault(labelKey) + value;

            if (!_last.TryGetValue(metric, out var lastByLabels))
                _last[metric] = lastByLabels = new Dictionary<string, double>(StringComparer.Ordinal);
            lastByLabels[labelKey] = value;
        }
    }
}
