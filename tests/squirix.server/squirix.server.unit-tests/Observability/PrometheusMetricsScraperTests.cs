using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization;
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
}
