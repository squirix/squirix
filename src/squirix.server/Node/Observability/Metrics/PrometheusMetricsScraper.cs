using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability.Metrics;

/// <summary>Scrapes <see cref="System.Diagnostics.Metrics" /> instruments from the <c language="csharp">Squirix</c> meter.</summary>
[Immutable]
internal sealed class PrometheusMetricsScraper : IDisposable
{
    internal static readonly PrometheusMetricsScraper Instance = new(false);

    private const int ExpectedLineLength = 64;
    private readonly Lock _creationLock = new();
    private readonly MeterListener _listener;
    private readonly ConcurrentDictionary<string, MetricSeries> _metrics = new(StringComparer.Ordinal);
    private readonly List<MetricSeries> _metricsInOrder = [];

    private PrometheusMetricsScraper(bool isolated)
    {
        _listener = CreateListener();
        if (!isolated)
            _listener.Start();
    }

    public void Dispose() => _listener.Dispose();

    internal static PrometheusMetricsScraper CreateIsolated() => new(true);

    /// <summary>Records one measurement into the series identified by the metric name and the public label set.</summary>
    /// <param name="metric">The instrument name.</param>
    /// <param name="isObservable">Whether the instrument is observable (its value replaces the sum instead of adding to it).</param>
    /// <param name="tags">Full instrument tags.</param>
    /// <param name="value">The measured value.</param>
    internal void RecordMeasurement(string metric, bool isObservable, ReadOnlySpan<KeyValuePair<string, object?>> tags, double value)
    {
        var labels = PrometheusScrapeLabelPolicy.LabelBuffer.Rent();
        try
        {
            PrometheusScrapeLabelPolicy.BuildPublicLabelKey(tags, labels);
            var key = labels.AsSpan();

            if (!_metrics.TryGetValue(metric, out var series))
                series = GetOrAddMetric(metric);

            if (series.ByLabels.GetAlternateLookup<ReadOnlySpan<char>>().TryGetValue(key, out var existing))
                existing.Record(value, isObservable);
            else
                series.AddOrRecord(key, value, isObservable, _creationLock);
        }
        finally
        {
            PrometheusScrapeLabelPolicy.LabelBuffer.Return(labels);
        }
    }

    internal string Scrape(PrometheusScrapeProfile profile = PrometheusScrapeProfile.Public) => profile switch
    {
        PrometheusScrapeProfile.Public => ScrapePublic(),
        _ => throw new ArgumentOutOfRangeException(nameof(profile), "Unsupported scrape profile."),
    };

    private static void AppendExportedMetrics(StringBuilder sb, HashSet<string> emitted, List<(string Metric, string Labels, double Sum, double Last)> exported, string? suffix, bool useSum)
    {
        foreach (var (metric, labels, sum, last) in CollectionsMarshal.AsSpan(exported))
        {
            var key = BuildSeriesKey(metric, suffix, labels);
            if (!emitted.Add(key))
                continue;

            AppendMetricLine(sb, metric, suffix, labels, useSum ? sum : last);
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

    private static string FormatValue(double value) => value switch
    {
        _ when double.IsNaN(value) => "NaN",
        _ when double.IsPositiveInfinity(value) => "+Inf",
        _ when double.IsNegativeInfinity(value) => "-Inf",
        _ => value.ToString(CultureInfo.InvariantCulture),
    };

    private MetricSeries GetOrAddMetric(string metric)
    {
        lock (_creationLock)
        {
            if (_metrics.TryGetValue(metric, out var series))
                return series;

            series = new MetricSeries(metric);
            _metricsInOrder.Add(series);
            _metrics[metric] = series;
            return series;
        }
    }

    private string ScrapePublic()
    {
        _listener.RecordObservableInstruments();
        var exported = SnapshotForExport();
        var sb = new StringBuilder(exported.Count * 2 * ExpectedLineLength);
        var emitted = new HashSet<string>(StringComparer.Ordinal);
        AppendExportedMetrics(sb, emitted, exported, null, true);
        AppendExportedMetrics(sb, emitted, exported, "_last", false);
        return sb.ToString();
    }

    /// <summary>
    /// Copies every series in creation order. Sum and last are read separately without a lock, so a series seen
    /// mid-update may pair a sum and a last value from adjacent moments; each is a value that was actually recorded.
    /// </summary>
    /// <returns>The series values at the time of the call.</returns>
    private List<(string Metric, string Labels, double Sum, double Last)> SnapshotForExport()
    {
        MetricSeries[] metrics;
        lock (_creationLock)
            metrics = [.. _metricsInOrder];

        var result = new List<(string Metric, string Labels, double Sum, double Last)>();
        foreach (var metric in metrics)
            foreach (var series in metric.CopySeries(_creationLock))
                result.Add((metric.Name, series.Labels, series.ReadSum(), series.ReadLast()));

        return result;
    }

    /// <summary>Label filtering for the public HTTP Prometheus scrape profile.</summary>
    private static class PrometheusScrapeLabelPolicy
    {
        private const int FormatScratchChars = 64;
        private const int MaxStackTags = 16;

        private static readonly FrozenSet<string> ExcludedLabelNames = new[] { "cache", "exception_type" }.ToFrozenSet(StringComparer.Ordinal);

        /// <summary>Writes the Prometheus label set of the public tags (sorted by key) into <paramref name="buffer" />.</summary>
        /// <param name="tags">Full instrument tags.</param>
        /// <param name="buffer">Receives the label set without outer braces.</param>
        internal static void BuildPublicLabelKey(ReadOnlySpan<KeyValuePair<string, object?>> tags, LabelBuffer buffer)
        {
            if (tags.Length == 0)
                return;

            int[]? rented = null;
            var order = tags.Length <= MaxStackTags ? stackalloc int[MaxStackTags] : RentOrder(tags.Length, out rented);

            try
            {
                var count = 0;
                for (var i = 0; i < tags.Length; i++)
                {
                    if (ExcludedLabelNames.Contains(tags[i].Key))
                        continue;

                    var j = count++;
                    while (j > 0 && string.CompareOrdinal(tags[order[j - 1]].Key, tags[i].Key) > 0)
                    {
                        order[j] = order[j - 1];
                        j--;
                    }

                    order[j] = i;
                }

                Span<char> scratch = stackalloc char[FormatScratchChars];
                for (var i = 0; i < count; i++)
                {
                    if (i > 0)
                        buffer.Append(',');
                    var tag = tags[order[i]];
                    AppendSanitizedName(buffer, tag.Key);
                    buffer.Append("=\"");
                    AppendValue(buffer, tag.Value, scratch);
                    buffer.Append('"');
                }
            }
            finally
            {
                if (rented != null)
                    ArrayPool<int>.Shared.Return(rented);
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
                if (ch is (>= 'a' and <= 'z') or (>= 'A' and <= 'Z') or (>= '0' and <= '9') || ch == '_' || (allowColon && ch == ':'))
                    _ = sb.Append(ch);
                else
                    _ = sb.Append('_');
            }
        }

        private static void AppendEscaped(LabelBuffer buffer, ReadOnlySpan<char> s)
        {
            if (s.IndexOfAny('\\', '\n', '"') < 0)
            {
                buffer.Append(s);
                return;
            }

            foreach (var ch in s)
            {
                switch (ch)
                {
                    case '\\':
                        buffer.Append(@"\\");
                        break;
                    case '\n':
                        buffer.Append(@"\n");
                        break;
                    case '"':
                        buffer.Append(@"\""");
                        break;
                    default:
                        buffer.Append(ch);
                        break;
                }
            }
        }

        private static void AppendSanitizedName(LabelBuffer buffer, string name)
        {
            if (name.Length == 0)
                return;

            if (name[0] >= '0' && name[0] <= '9')
                buffer.Append('_');

            for (var i = 0; i < name.Length; i++)
            {
                var ch = name[i];
                buffer.Append(ch is (>= 'a' and <= 'z') or (>= 'A' and <= 'Z') or (>= '0' and <= '9') || ch == '_' ? ch : '_');
            }
        }

        /// <summary>
        /// Appends the label value text exactly as <c language="csharp">Convert.ToString(value, InvariantCulture)</c>
        /// renders it, formatting common value types without allocating a string.
        /// </summary>
        /// <param name="buffer">The destination buffer.</param>
        /// <param name="value">The tag value.</param>
        /// <param name="scratch">Scratch space for span formatting.</param>
        private static void AppendValue(LabelBuffer buffer, object? value, scoped Span<char> scratch)
        {
            switch (value)
            {
                case null:
                    return;
                case string text:
                    AppendEscaped(buffer, text);
                    return;
                case bool flag:
                    buffer.Append(flag ? bool.TrueString : bool.FalseString);
                    return;
                case char ch:
                    AppendEscaped(buffer, new ReadOnlySpan<char>(in ch));
                    return;
                case ISpanFormattable formattable when formattable.TryFormat(scratch, out var written, default, CultureInfo.InvariantCulture):
                    AppendEscaped(buffer, scratch[..written]);
                    return;
                default:
                    AppendEscaped(buffer, Convert.ToString(value, CultureInfo.InvariantCulture));
                    return;
            }
        }

        private static Span<int> RentOrder(int length, out int[] rented)
        {
            rented = ArrayPool<int>.Shared.Rent(length);
            return rented;
        }

        /// <summary>Append-only char buffer reused per thread so the record path does not allocate.</summary>
        internal sealed class LabelBuffer
        {
            private const int InitialCapacity = 512;
            private const int MaxRetainedCapacity = 8192;

            private static readonly ThreadLocal<LabelBuffer> Cache = new(static () => new LabelBuffer());

            private char[] _chars = new char[InitialCapacity];
            private int _length;

            internal static LabelBuffer Rent()
            {
                var buffer = Cache.Value!;
                buffer._length = 0;
                return buffer;
            }

            internal static void Return(LabelBuffer buffer)
            {
                if (buffer._chars.Length > MaxRetainedCapacity)
                    buffer._chars = new char[InitialCapacity];
            }

            internal void Append(char ch)
            {
                if (_length == _chars.Length)
                    Grow(1);
                _chars[_length++] = ch;
            }

            internal void Append(ReadOnlySpan<char> text)
            {
                if (text.Length > _chars.Length - _length)
                    Grow(text.Length);
                text.CopyTo(_chars.AsSpan(_length));
                _length += text.Length;
            }

            internal ReadOnlySpan<char> AsSpan() => _chars.AsSpan(0, _length);

            private void Grow(int additional) => Array.Resize(ref _chars, Math.Max(_chars.Length * 2, _length + additional));
        }
    }

    [Immutable]
    private sealed class MetricSeries
    {
        private readonly List<Series> _inOrder = [];

        internal MetricSeries(string name)
        {
            Name = name;
        }

        internal ConcurrentDictionary<string, Series> ByLabels { get; } = new(StringComparer.Ordinal);

        internal string Name { get; }

        internal void AddOrRecord(ReadOnlySpan<char> labels, double value, bool isObservable, Lock creationLock)
        {
            lock (creationLock)
            {
                if (ByLabels.GetAlternateLookup<ReadOnlySpan<char>>().TryGetValue(labels, out var existing))
                {
                    existing.Record(value, isObservable);
                    return;
                }

                var key = labels.ToString();
                var series = new Series(key, value);
                _inOrder.Add(series);
                ByLabels[key] = series;
            }
        }

        internal Series[] CopySeries(Lock creationLock)
        {
            lock (creationLock)
                return [.. _inOrder];
        }
    }

    /// <summary>Sum and last value of one label set; both are updated lock-free.</summary>
    private sealed class Series
    {
        private long _lastBits;
        private long _sumBits;

        internal Series(string labels, double value)
        {
            Labels = labels;
            _sumBits = BitConverter.DoubleToInt64Bits(0d + value);
            _lastBits = BitConverter.DoubleToInt64Bits(value);
        }

        internal string Labels { get; }

        internal double ReadLast() => BitConverter.Int64BitsToDouble(Volatile.Read(ref _lastBits));

        internal double ReadSum() => BitConverter.Int64BitsToDouble(Volatile.Read(ref _sumBits));

        internal void Record(double value, bool isObservable)
        {
            if (isObservable)
            {
                Volatile.Write(ref _sumBits, BitConverter.DoubleToInt64Bits(value));
            }
            else
            {
                var current = Volatile.Read(ref _sumBits);
                while (true)
                {
                    var updated = BitConverter.DoubleToInt64Bits(BitConverter.Int64BitsToDouble(current) + value);
                    var observed = Interlocked.CompareExchange(ref _sumBits, updated, current);
                    if (observed == current)
                        break;

                    current = observed;
                }
            }

            Volatile.Write(ref _lastBits, BitConverter.DoubleToInt64Bits(value));
        }
    }
}
