using System;
using System.Collections.Generic;
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
        using var scraper = PrometheusMetricsScraper.CreateIsolatedForTests();

        scraper.RecordMeasurementForTests(
            "squirix_ops_total",
            [new KeyValuePair<string, object?>("cache", "cache-a"), new KeyValuePair<string, object?>("operation", "get"), new KeyValuePair<string, object?>("result", "ok")],
            2);
        scraper.RecordMeasurementForTests(
            "squirix_ops_total",
            [new KeyValuePair<string, object?>("cache", "cache-b"), new KeyValuePair<string, object?>("operation", "get"), new KeyValuePair<string, object?>("result", "ok")],
            3);

        var body = scraper.Scrape();

        Assert.Contains("squirix_ops_total{operation=\"get\",result=\"ok\"} 5", body, StringComparison.InvariantCulture);
        Assert.DoesNotContain("cache=", body, StringComparison.InvariantCulture);
    }

    /// <summary>
    /// Verifies public scrape output omits exception_type labels.
    /// </summary>
    [Fact]
    public void ScrapePublicOmitsExceptionTypeLabel()
    {
        using var scraper = PrometheusMetricsScraper.CreateIsolatedForTests();

        scraper.RecordMeasurementForTests(
            "squirix_serializer_failures_total",
            [
                new KeyValuePair<string, object?>("op", "serialize"), new KeyValuePair<string, object?>("exception_type", "System.InvalidOperationException"),
                new KeyValuePair<string, object?>("impl", "json"),
            ],
            1);

        var body = scraper.Scrape();

        Assert.Contains("squirix_serializer_failures_total{impl=\"json\",op=\"serialize\"} 1", body, StringComparison.InvariantCulture);
        Assert.DoesNotContain("exception_type=", body, StringComparison.InvariantCulture);
    }
}
