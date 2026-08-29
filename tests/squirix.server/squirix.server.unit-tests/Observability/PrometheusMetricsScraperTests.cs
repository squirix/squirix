using System;
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
    /// Verifies the <c language="csharp">_last</c> series records the latest observed value rather than the
    /// running maximum, so a decreasing gauge is reflected in the scrape output.
    /// </summary>
    [Fact]
    public void ScrapeReflectsDecreasedGaugeLastValue()
    {
        const string metricName = "squirix_test_gauge_last_delta";
        _ = EndpointExtensions.PrometheusMetricsScraper.Instance;
        using var meter = new Meter("Squirix");
        var gauge = meter.CreateCounter<double>(metricName);

        gauge.Add(5);
        gauge.Add(3);

        var body = EndpointExtensions.PrometheusMetricsScraper.Instance.Scrape();

        var lastValue = Assert.NotNull(FindMetricLastValue(body, metricName));
        Assert.Equal(3d, lastValue);
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
