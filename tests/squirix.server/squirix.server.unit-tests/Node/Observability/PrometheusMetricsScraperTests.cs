using Squirix.Server.Node.Observability.Metrics;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Observability;

/// <summary>
/// Unit tests for public HTTP Prometheus scrape export redaction.
/// </summary>
public sealed class PrometheusMetricsScraperTests
{
    /// <summary>
    /// Verifies public scrape output omits cache labels and aggregates across cache namespaces.
    /// </summary>
    [Fact]
    public void ScrapePublicOmitsCacheLabelAndAggregatesAcrossNamespaces()
    {
        using var scraper = new PrometheusMetricsScraper(isolatedForTests: true);

        scraper.RecordMeasurementForTests(
            "squirix_ops_total",
            [new("cache", "cache-a"), new("operation", "get"), new("result", "ok")],
            2);
        scraper.RecordMeasurementForTests(
            "squirix_ops_total",
            [new("cache", "cache-b"), new("operation", "get"), new("result", "ok")],
            3);

        var body = scraper.Scrape();

        Assert.Contains("squirix_ops_total{operation=\"get\",result=\"ok\"} 5", body);
        Assert.DoesNotContain("cache=", body);
    }

    /// <summary>
    /// Verifies public scrape output omits exception_type labels.
    /// </summary>
    [Fact]
    public void ScrapePublicOmitsExceptionTypeLabel()
    {
        using var scraper = new PrometheusMetricsScraper(isolatedForTests: true);

        scraper.RecordMeasurementForTests(
            "squirix_serializer_failures_total",
            [new("op", "serialize"), new("exception_type", "System.InvalidOperationException"), new("impl", "json")],
            1);

        var body = scraper.Scrape();

        Assert.Contains("squirix_serializer_failures_total{impl=\"json\",op=\"serialize\"} 1", body);
        Assert.DoesNotContain("exception_type=", body);
    }
}
