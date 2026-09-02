using System;
using System.Buffers;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability.Metrics;

/// <summary>Scrapes <see cref="System.Diagnostics.Metrics" /> instruments from the <c language="csharp">Squirix</c> meter.</summary>
[Immutable]
internal sealed class PrometheusMetricsScraper : IDisposable
{
    internal static readonly PrometheusMetricsScraper Instance = new(false);
    private readonly Dictionary<string, Dictionary<string, double>> _last = new(StringComparer.Ordinal);
    private readonly MeterListener _listener;
    private readonly Lock _lock = new();
    private readonly Dictionary<string, Dictionary<string, double>> _sums = new(StringComparer.Ordinal);

    private PrometheusMetricsScraper(bool isolated)
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

    private static void AppendExportedMetrics(StringBuilder sb, HashSet<string> emitted, Dictionary<string, Dictionary<string, double>> exported, string? suffix)
    {
        foreach (var (metric, byLabels) in exported)
            foreach (var (labels, value) in byLabels)
            {
                var key = BuildSeriesKey(metric, suffix, labels);
                if (!emitted.Add(key))
                    continue;

                AppendMetricLine(sb, metric, suffix, labels, value);
            }
    }

    private static void AppendMetricLine(StringBuilder sb, string metric, string? suffix, string labels, double value)
    {
        PrometheusScrapeLabelPolicy.AppendSanitizedName(sb, metric, true);
        if (suffix != null)
            _ = sb.Append(suffix);
        if (labels.Length > 0)
        {
            _ = sb.Append('{');
            _ = sb.Append(labels);
            _ = sb.Append('}');
        }

        _ = sb.Append(' ');
        _ = sb.Append(FormatValue(value));
        _ = sb.Append('\n');
    }

    private static string BuildSeriesKey(string metric, string? suffix, string labels)
    {
        var sb = new StringBuilder(metric.Length + (suffix?.Length ?? 0) + labels.Length);
        PrometheusScrapeLabelPolicy.AppendSanitizedName(sb, metric, true);
        if (suffix != null)
            _ = sb.Append(suffix);
        if (labels.Length == 0)
            return sb.ToString();
        _ = sb.Append('{');
        _ = sb.Append(labels);
        _ = sb.Append('}');

        return sb.ToString();
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

        listener.SetMeasurementEventCallback<long>(static (inst, m, tags, _) => Instance.RecordMeasurement(inst.Name, inst.IsObservable, tags, m));
        listener.SetMeasurementEventCallback<int>(static (inst, m, tags, _) => Instance.RecordMeasurement(inst.Name, inst.IsObservable, tags, m));
        listener.SetMeasurementEventCallback<double>(static (inst, m, tags, _) => Instance.RecordMeasurement(inst.Name, inst.IsObservable, tags, m));
        return listener;
    }

    private static string FormatValue(double value)
    {
        if (double.IsNaN(value))
            return "NaN";
        if (double.IsPositiveInfinity(value))
            return "+Inf";
        if (double.IsNegativeInfinity(value))
            return "-Inf";
        return value.ToString(CultureInfo.InvariantCulture);
    }

    private void RecordMeasurement(string metric, bool isObservable, ReadOnlySpan<KeyValuePair<string, object?>> tags, double value)
    {
        var exportLabels = PrometheusScrapeLabelPolicy.BuildPublicLabelKey(tags);
        lock (_lock)
        {
            if (!_sums.TryGetValue(metric, out var byLabels))
                _sums[metric] = byLabels = new Dictionary<string, double>(StringComparer.Ordinal);
            byLabels[exportLabels] = isObservable ? value : byLabels.GetValueOrDefault(exportLabels) + value;

            if (!_last.TryGetValue(metric, out var lastByLabels))
                _last[metric] = lastByLabels = new Dictionary<string, double>(StringComparer.Ordinal);
            lastByLabels[exportLabels] = value;
        }
    }

    private string ScrapePublic()
    {
        _listener.RecordObservableInstruments();
        var (exportedSums, exportedLast, lineCount) = SnapshotForExport();
        var sb = new StringBuilder(lineCount * 64);
        var emitted = new HashSet<string>(StringComparer.Ordinal);
        AppendExportedMetrics(sb, emitted, exportedSums, null);
        AppendExportedMetrics(sb, emitted, exportedLast, "_last");
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

    /// <summary>Label filtering for the public HTTP Prometheus scrape profile.</summary>
    private static class PrometheusScrapeLabelPolicy
    {
        private static readonly FrozenSet<string> ExcludedLabelNames = new[] { "cache", "exception_type" }.ToFrozenSet(StringComparer.Ordinal);

        /// <summary>Builds a Prometheus label set string from public tags (sorted by key).</summary>
        /// <param name="tags">Full instrument tags.</param>
        /// <returns>Prometheus label set without outer braces.</returns>
        internal static string BuildPublicLabelKey(ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            if (tags.Length == 0)
                return string.Empty;

            var rented = ArrayPool<KeyValuePair<string, object?>>.Shared.Rent(tags.Length);
            try
            {
                var writeIndex = 0;
                foreach (var tag in tags)
                {
                    if (ExcludedLabelNames.Contains(tag.Key))
                        continue;

                    rented[writeIndex++] = tag;
                }

                if (writeIndex == 0)
                    return string.Empty;

                var filtered = rented.AsSpan(0, writeIndex);
                filtered.Sort(static (a, b) => string.CompareOrdinal(a.Key, b.Key));
                return BuildLabelKey(filtered);
            }
            finally
            {
                ArrayPool<KeyValuePair<string, object?>>.Shared.Return(rented, true);
            }
        }

        internal static void AppendSanitizedName(StringBuilder sb, string name, bool allowColon)
        {
            if (name.Length == 0)
                return;

            if (name[0] >= '0' && name[0] <= '9')
                _ = sb.Append('_');

            for (var i = 0; i < name.Length; i++)
            {
                var ch = name[i];
                if (ch is >= 'a' and <= 'z' or >= 'A' and <= 'Z' or >= '0' and <= '9' || ch == '_' || (allowColon && ch == ':'))
                    _ = sb.Append(ch);
                else
                    _ = sb.Append('_');
            }
        }

        private static void AppendEscaped(StringBuilder sb, string s)
        {
            if (s.Length == 0)
                return;

            var needsEscape = false;
            for (var i = 0; i < s.Length; i++)
            {
                var ch = s[i];
                if (ch is not ('\\' or '\n' or '"'))
                    continue;
                needsEscape = true;
                break;
            }

            if (!needsEscape)
            {
                _ = sb.Append(s);
                return;
            }

            for (var i = 0; i < s.Length; i++)
            {
                _ = s[i] switch
                {
                    '\\' => sb.Append(@"\\"),
                    '\n' => sb.Append(@"\n"),
                    '"' => sb.Append(@"\"""),
                    _ => sb.Append(s[i]),
                };
            }
        }

        /// <summary>Builds a Prometheus label set string from sorted tags.</summary>
        /// <param name="tags">Sorted tag list.</param>
        /// <returns>Prometheus label set without outer braces.</returns>
        private static string BuildLabelKey(ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            var sb = new StringBuilder(tags.Length * 24);
            for (var i = 0; i < tags.Length; i++)
            {
                if (i > 0)
                    _ = sb.Append(',');
                AppendSanitizedName(sb, tags[i].Key, false);
                _ = sb.Append("=\"");
                AppendEscaped(sb, Convert.ToString(tags[i].Value, CultureInfo.InvariantCulture) ?? string.Empty);
                _ = sb.Append('"');
            }

            return sb.ToString();
        }
    }
}
