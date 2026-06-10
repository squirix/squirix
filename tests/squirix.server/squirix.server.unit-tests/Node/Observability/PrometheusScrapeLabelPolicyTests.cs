using System.Collections.Generic;
using Squirix.Server.Node.Observability.Metrics;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Observability;

/// <summary>
/// Unit tests for public HTTP Prometheus scrape label filtering.
/// </summary>
public sealed class PrometheusScrapeLabelPolicyTests
{
    /// <summary>
    /// Verifies cache and exception_type labels are removed from public export tags.
    /// </summary>
    [Fact]
    public void FilterPublicTagsRemovesIdentifyingLabels()
    {
        KeyValuePair<string, object?>[] tags =
        [
            new("cache", "tenant-secret"),
            new("exception_type", "System.InvalidOperationException"),
            new("operation", "get"),
            new("result", "ok"),
        ];

        var filtered = PrometheusScrapeLabelPolicy.FilterPublicTags(tags);

        Assert.Equal(2, filtered.Length);
        Assert.Equal("operation", filtered[0].Key);
        Assert.Equal("get", filtered[0].Value);
        Assert.Equal("result", filtered[1].Key);
        Assert.Equal("ok", filtered[1].Value);
    }

    /// <summary>
    /// Verifies filtered tags render to a Prometheus label set without stripped names.
    /// </summary>
    [Fact]
    public void BuildLabelKeyOmitsStrippedLabelsInPublicExport()
    {
        KeyValuePair<string, object?>[] tags =
        [
            new("cache", "secret-cache"),
            new("operation", "set"),
            new("result", "ok"),
        ];

        var labelKey = PrometheusScrapeLabelPolicy.BuildLabelKey(PrometheusScrapeLabelPolicy.FilterPublicTags(tags));

        Assert.Equal("operation=\"set\",result=\"ok\"", labelKey);
        Assert.DoesNotContain("cache=", labelKey);
    }
}
