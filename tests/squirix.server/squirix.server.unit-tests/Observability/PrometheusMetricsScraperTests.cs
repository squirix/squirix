using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability.Metrics;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>Verifies the built-in Prometheus scraper snapshots the latest observed gauge value.</summary>
[Immutable]
[NotInParallel]
public sealed class PrometheusMetricsScraperTests
{
    private const int ConcurrentIterations = 20000;

    /// <summary>
    /// Verifies concurrent recording from several threads into shared and distinct label sets loses no update:
    /// each series sum equals the number of additions, and its last value is the last recorded one.
    /// </summary>
    [Test]
    public async Task ConcurrentRecordingProducesExactSums()
    {
        const string metricName = "squirix_test_concurrent_sums";
        const int threadCount = 8;
        const int shardCount = 4;
        using var scraper = PrometheusMetricsScraper.CreateIsolated();
        using var barrier = new Barrier(threadCount);
        var workers = new RecordingWorker[threadCount];
        for (var t = 0; t < threadCount; t++)
        {
            workers[t] = new RecordingWorker(scraper, barrier, metricName);
            workers[t].Start();
        }

        for (var t = 0; t < threadCount; t++)
            await workers[t].Completion;

        var body = scraper.Scrape();

        const int expectedPerShard = threadCount * ConcurrentIterations / shardCount;
        for (var shard = 0; shard < shardCount; shard++)
        {
            var expectedLine = metricName + "{shard=\"" + shard.ToString(CultureInfo.InvariantCulture) + "\"} "
                + expectedPerShard.ToString(CultureInfo.InvariantCulture) + "\n";
            _ = await Assert.That(body).Contains(expectedLine, StringComparison.Ordinal);
        }

        _ = await Assert.That(CountSeriesLines(body, metricName + "{shard=\"0\"}")).IsEqualTo(1);
        _ = await Assert.That(body).Contains(metricName + "_last{shard=\"3\"} 1\n", StringComparison.Ordinal);
    }

    /// <summary>
    /// Verifies the label key renders each value type exactly as invariant-culture string conversion would:
    /// escaped strings, numbers, booleans, enums and null, sorted by key with excluded and unsafe names handled.
    /// </summary>
    [Test]
    public async Task LabelKeyFormatsMixedValueTypes()
    {
        const string metricName = "squirix_test_label_mix";
        using var scraper = PrometheusMetricsScraper.CreateIsolated();
        var tags = new KeyValuePair<string, object?>[]
        {
            new("h-x", "v"),
            new("g_enum", DayOfWeek.Friday),
            new("f_null", null),
            new("e_bool", true),
            new("d_double", 1234.5d),
            new("cache", "ignored"),
            new("c_long", 9876543210L),
            new("b_int", -42),
            new("exception_type", "ignored"),
            new("a_str", "say \"hi\"\nend\\"),
            new("i_char", '"'),
            new("j_false", false),
        };

        scraper.RecordMeasurement(metricName, false, tags, 1d);
        var body = scraper.Scrape();

        const string expectedLabels = "a_str=\"say \\\"hi\\\"\\nend\\\\\",b_int=\"-42\",c_long=\"9876543210\",d_double=\"1234.5\","
            + "e_bool=\"True\",f_null=\"\",g_enum=\"Friday\",h_x=\"v\",i_char=\"\\\"\",j_false=\"False\"";
        _ = await Assert.That(body).Contains(metricName + "{" + expectedLabels + "} 1\n", StringComparison.Ordinal);
    }

    /// <summary>
    /// Verifies a label set larger than the stack buffer and tag count above the inline limit is still rendered
    /// completely and in key order.
    /// </summary>
    [Test]
    public async Task LabelKeyHandlesLargeLabelSets()
    {
        const string metricName = "squirix_test_label_large";
        const int tagCount = 40;
        using var scraper = PrometheusMetricsScraper.CreateIsolated();
        var tags = new KeyValuePair<string, object?>[tagCount];
        var expected = new StringBuilder();
        for (var i = 0; i < tagCount; i++)
        {
            var index = tagCount - 1 - i;
            tags[i] = new KeyValuePair<string, object?>("k" + index.ToString("D2", CultureInfo.InvariantCulture), new string('v', 20) + index.ToString(CultureInfo.InvariantCulture));
        }

        for (var i = 0; i < tagCount; i++)
        {
            if (i > 0)
                _ = expected.Append(',');
            _ = expected.Append('k').Append(i.ToString("D2", CultureInfo.InvariantCulture)).Append("=\"").Append('v', 20).Append(i.ToString(CultureInfo.InvariantCulture)).Append('"');
        }

        scraper.RecordMeasurement(metricName, false, tags, 2d);
        var body = scraper.Scrape();

        _ = await Assert.That(body).Contains(metricName + "{" + expected + "} 2\n", StringComparison.Ordinal);
    }

    /// <summary>
    /// Verifies observable gauge callbacks are evaluated on scrape (via
    /// <c language="csharp">RecordObservableInstruments</c>) so the current value is captured.
    /// </summary>
    [Test]
    public async Task ScrapeCapturesObservableGaugeValue()
    {
        const string metricName = "squirix_test_observable_gauge_current";
        _ = PrometheusMetricsScraper.Instance;
        using var meter = new Meter("Squirix");
        _ = meter.CreateObservableGauge(metricName, static () => 42);

        var body = PrometheusMetricsScraper.Instance.Scrape();

        var lastValue = await Assert.That(FindMetricLastValue(body, metricName)).IsNotNull();
        _ = await Assert.That(lastValue).IsEqualTo(42d);
    }

    /// <summary>
    /// Verifies that when a raw metric name ends in <c language="csharp">_last</c>, its sum series does not
    /// collide ambiguously with the derived <c language="csharp">_last</c> series of another metric: the
    /// authoritative sum wins and only one physical line is emitted.
    /// </summary>
    [Test]
    public async Task ScrapeDeduplicatesCollidingLastSeries()
    {
        const string baseName = "squirix_test_collision";
        const string suffixedName = baseName + "_last";
        _ = PrometheusMetricsScraper.Instance;
        using var meter = new Meter("Squirix");
        var baseCounter = meter.CreateCounter<double>(baseName);
        var suffixedCounter = meter.CreateCounter<double>(suffixedName);

        baseCounter.Add(1);
        suffixedCounter.Add(2);

        var body = PrometheusMetricsScraper.Instance.Scrape();

        _ = await Assert.That(CountSeriesLines(body, suffixedName)).IsEqualTo(1);
        _ = await Assert.That(body).Contains(suffixedName + " 2", StringComparison.Ordinal);
    }

    /// <summary>
    /// Verifies non-finite measurement values are exported with Prometheus spellings (<c language="csharp">NaN</c>,
    /// <c language="csharp">+Inf</c>, <c language="csharp">-Inf</c>) instead of invariant-culture forms.
    /// </summary>
    /// <param name="metricName">The instrument name.</param>
    /// <param name="value">The non-finite value being measured.</param>
    /// <param name="expected">The expected Prometheus spelling.</param>
    [Test]
    [Arguments("squirix_test_nonfinite_nan", double.NaN, "NaN")]
    [Arguments("squirix_test_nonfinite_posinf", double.PositiveInfinity, "+Inf")]
    [Arguments("squirix_test_nonfinite_neginf", double.NegativeInfinity, "-Inf")]
    public async Task ScrapeFormatsNonFiniteValues(string metricName, double value, string expected)
    {
        _ = PrometheusMetricsScraper.Instance;
        using var meter = new Meter("Squirix");
        var counter = meter.CreateCounter<double>(metricName);

        counter.Add(value);

        var body = PrometheusMetricsScraper.Instance.Scrape();

        _ = await Assert.That(body).Contains(metricName + "_last " + expected, StringComparison.Ordinal);
    }

    /// <summary>
    /// Verifies a label value containing a tab is preserved literally rather than emitted with the
    /// unsupported <c language="csharp">\t</c> escape, keeping the exported exposition valid.
    /// </summary>
    [Test]
    public async Task ScrapePreservesTabInLabelValueLiterally()
    {
        const string metricName = "squirix_test_tab_label";
        _ = PrometheusMetricsScraper.Instance;
        using var meter = new Meter("Squirix");
        var counter = meter.CreateCounter<double>(metricName);
        var measurementTags = new KeyValuePair<string, object?>[] { new("scheme", "a\tb") };

        counter.Add(1, measurementTags);

        var body = PrometheusMetricsScraper.Instance.Scrape();

        _ = await Assert.That(body).Contains(metricName + "{scheme=\"a\tb\"} ", StringComparison.Ordinal);
        _ = await Assert.That(body).DoesNotContain(@"\t", StringComparison.Ordinal);
    }

    /// <summary>
    /// Verifies the <c language="csharp">_last</c> series records the latest observed value rather than the
    /// running maximum, so a decreasing gauge is reflected in the scrape output.
    /// </summary>
    [Test]
    public async Task ScrapeReflectsDecreasedGaugeLastValue()
    {
        const string metricName = "squirix_test_gauge_last_delta";
        _ = PrometheusMetricsScraper.Instance;
        using var meter = new Meter("Squirix");
        var gauge = meter.CreateCounter<double>(metricName);

        gauge.Add(5);
        gauge.Add(3);

        var body = PrometheusMetricsScraper.Instance.Scrape();

        var lastValue = await Assert.That(FindMetricLastValue(body, metricName)).IsNotNull();
        _ = await Assert.That(lastValue).IsEqualTo(3d);
    }

    /// <summary>
    /// Verifies metric and label names outside the Prometheus alphabet are sanitized on scrape
    /// (colons preserved in metric names, dropped in label names), keeping the exposition valid.
    /// </summary>
    [Test]
    public async Task ScrapeSanitizesNames()
    {
        _ = PrometheusMetricsScraper.Instance;
        using var meter = new Meter("Squirix");
        var dotted = meter.CreateCounter<double>("squirix.Test.Sanitize.Upper9");
        var colon = meter.CreateCounter<double>("squirix:test:sanitize");
        var tagged = meter.CreateCounter<double>("squirix_test_sanitize_tags");
        var taggedTags = new KeyValuePair<string, object?>[] { new("a:b", "v") };
        dotted.Add(1);
        colon.Add(2);
        tagged.Add(3, taggedTags);

        var body = PrometheusMetricsScraper.Instance.Scrape();

        _ = await Assert.That(body).Contains("squirix_Test_Sanitize_Upper9", StringComparison.Ordinal);
        _ = await Assert.That(body).Contains("squirix:test:sanitize", StringComparison.Ordinal);
        _ = await Assert.That(body).Contains("squirix_test_sanitize_tags{a_b=\"v\"} ", StringComparison.Ordinal);
    }

    private static int CountSeriesLines(string body, string metricName)
    {
        var count = 0;
        var prefix = metricName + " ";
        var remaining = body.AsSpan();
        while (!remaining.IsEmpty)
        {
            var eol = remaining.IndexOfAny('\r', '\n');
            var line = eol < 0 ? remaining : remaining[..eol];
            if (eol < 0)
            {
                remaining = [];
            }
            else
            {
                var skip = eol + 1;
                if (remaining[eol] == '\r' && skip < remaining.Length && remaining[skip] == '\n')
                    skip++;
                remaining = remaining[skip..];
            }

            if (line.StartsWith(prefix, StringComparison.Ordinal))
                count++;
        }

        return count;
    }

    private static double? FindMetricLastValue(string body, string metricName)
    {
        var prefix = metricName + "_last ";
        var remaining = body.AsSpan();
        while (!remaining.IsEmpty)
        {
            var eol = remaining.IndexOfAny('\r', '\n');
            var line = eol < 0 ? remaining : remaining[..eol];
            if (eol < 0)
            {
                remaining = [];
            }
            else
            {
                var skip = eol + 1;
                if (remaining[eol] == '\r' && skip < remaining.Length && remaining[skip] == '\n')
                    skip++;
                remaining = remaining[skip..];
            }

            if (line.StartsWith(prefix, StringComparison.Ordinal))
                return double.Parse(line[prefix.Length..], CultureInfo.InvariantCulture);
        }

        return null;
    }

    private sealed class RecordingWorker
    {
        private readonly Barrier _barrier;
        private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly string _metricName;
        private readonly PrometheusMetricsScraper _scraper;

        internal RecordingWorker(PrometheusMetricsScraper scraper, Barrier barrier, string metricName)
        {
            _scraper = scraper;
            _barrier = barrier;
            _metricName = metricName;
        }

        internal Task Completion => _completion.Task;

        internal void Start()
        {
            var thread = new Thread(Run) { IsBackground = true };
            thread.Start();
        }

        private void Run()
        {
            try
            {
                _ = _barrier.SignalAndWait(TimeSpan.FromSeconds(30), CancellationToken.None);
                for (var i = 0; i < ConcurrentIterations; i++)
                {
                    var tags = new KeyValuePair<string, object?>[] { new("shard", i % 4), new("cache", "ignored") };
                    _scraper.RecordMeasurement(_metricName, false, tags, 1d);
                }

                _completion.SetResult();
            }
            catch (Exception ex) when (_completion.TrySetException(ex))
            {
                // The failure is surfaced through the completion task.
            }
        }
    }
}
