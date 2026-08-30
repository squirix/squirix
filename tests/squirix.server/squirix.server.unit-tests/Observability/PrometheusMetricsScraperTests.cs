using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability.Metrics;
using Xunit;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>Verifies the built-in Prometheus scraper snapshots the latest observed gauge value.</summary>
[Immutable]
public sealed class PrometheusMetricsScraperTests
{
    /// <summary>
    /// Verifies observable gauge callbacks are evaluated on scrape (via
    /// <c language="csharp">RecordObservableInstruments</c>) so the current value is captured.
    /// </summary>
    [Fact]
    public void ScrapeCapturesObservableGaugeValue()
    {
        const string metricName = "squirix_test_observable_gauge_current";
        _ = PrometheusMetricsScraper.Instance;
        using var meter = new Meter("Squirix");
        _ = meter.CreateObservableGauge(metricName, static () => 42);

        var body = PrometheusMetricsScraper.Instance.Scrape();

        var lastValue = Assert.NotNull(FindMetricLastValue(body, metricName));
        Assert.Equal(42d, lastValue);
    }

    /// <summary>
    /// Verifies that when a raw metric name ends in <c language="csharp">_last</c>, its sum series does not
    /// collide ambiguously with the derived <c language="csharp">_last</c> series of another metric: the
    /// authoritative sum wins and only one physical line is emitted.
    /// </summary>
    [Fact]
    public void ScrapeDeduplicatesCollidingLastSeries()
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

        Assert.Equal(1, CountSeriesLines(body, suffixedName));
        Assert.Contains(suffixedName + " 2", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// Verifies non-finite measurement values are exported with Prometheus spellings (<c language="csharp">NaN</c>,
    /// <c language="csharp">+Inf</c>, <c language="csharp">-Inf</c>) instead of invariant-culture forms.
    /// </summary>
    /// <param name="metricName">The instrument name.</param>
    /// <param name="value">The non-finite value being measured.</param>
    /// <param name="expected">The expected Prometheus spelling.</param>
    [Theory]
    [InlineData("squirix_test_nonfinite_nan", double.NaN, "NaN")]
    [InlineData("squirix_test_nonfinite_posinf", double.PositiveInfinity, "+Inf")]
    [InlineData("squirix_test_nonfinite_neginf", double.NegativeInfinity, "-Inf")]
    public void ScrapeFormatsNonFiniteValues(string metricName, double value, string expected)
    {
        _ = PrometheusMetricsScraper.Instance;
        using var meter = new Meter("Squirix");
        var counter = meter.CreateCounter<double>(metricName);

        counter.Add(value);

        var body = PrometheusMetricsScraper.Instance.Scrape();

        Assert.Contains(metricName + "_last " + expected, body, StringComparison.Ordinal);
    }

    /// <summary>
    /// Verifies a label value containing a tab is preserved literally rather than emitted with the
    /// unsupported <c language="csharp">\t</c> escape, keeping the exported exposition valid.
    /// </summary>
    [Fact]
    public void ScrapePreservesTabInLabelValueLiterally()
    {
        const string metricName = "squirix_test_tab_label";
        _ = PrometheusMetricsScraper.Instance;
        using var meter = new Meter("Squirix");
        var counter = meter.CreateCounter<double>(metricName);
        var measurementTags = new KeyValuePair<string, object?>[] { new("scheme", "a\tb") };

        counter.Add(1, measurementTags);

        var body = PrometheusMetricsScraper.Instance.Scrape();

        Assert.Contains(metricName + "{scheme=\"a\tb\"} ", body, StringComparison.Ordinal);
        Assert.DoesNotContain(@"\t", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// Verifies the <c language="csharp">_last</c> series records the latest observed value rather than the
    /// running maximum, so a decreasing gauge is reflected in the scrape output.
    /// </summary>
    [Fact]
    public void ScrapeReflectsDecreasedGaugeLastValue()
    {
        const string metricName = "squirix_test_gauge_last_delta";
        _ = PrometheusMetricsScraper.Instance;
        using var meter = new Meter("Squirix");
        var gauge = meter.CreateCounter<double>(metricName);

        gauge.Add(5);
        gauge.Add(3);

        var body = PrometheusMetricsScraper.Instance.Scrape();

        var lastValue = Assert.NotNull(FindMetricLastValue(body, metricName));
        Assert.Equal(3d, lastValue);
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
