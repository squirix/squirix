using System.Threading.Tasks;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>Verifies Prometheus metrics settings deserialization and merge via <see cref="PrometheusMetricsBootstrap" />.</summary>
public sealed class PrometheusMetricsSettingsTests
{
    /// <summary>
    /// Verifies System.Text.Json binds private <c>path</c>/<c>enabled</c> properties
    /// (via <see cref="System.Text.Json.Serialization.JsonIncludeAttribute" />) and merge overrides the baseline.
    /// </summary>
    [Fact]
    public async Task DeserializeAndMergeIntoAppliesJsonOverrides()
    {
        var baseline = new PrometheusMetricsEndpointOptions
        {
            Enabled = true,
            Path = "/metrics",
        };

        using var settings = await TempSettingsFile.WriteAsync(
            "squirix-prom-",
            """{"PrometheusMetrics":{"path":"/custom-metrics","enabled":false}}""",
            TestContext.Current.CancellationToken);
        var (found, merged) = await PrometheusMetricsBootstrap.TryMergeFromSettingsFilePathAsync(settings.Path, baseline, TestContext.Current.CancellationToken);

        Assert.True(found);
        Assert.False(merged.Enabled);
        Assert.Equal("/custom-metrics", merged.Path);
    }

    /// <summary>Verifies a partial JSON section overrides only present fields and keeps baseline for absent ones.</summary>
    [Fact]
    public async Task DeserializeAndMergeKeepsBaselineForAbsentFields()
    {
        var baseline = new PrometheusMetricsEndpointOptions
        {
            Enabled = true,
            Path = "/metrics",
        };

        using var settings = await TempSettingsFile.WriteAsync("squirix-prom-", """{"PrometheusMetrics":{"enabled":false}}""", TestContext.Current.CancellationToken);
        var (found, merged) = await PrometheusMetricsBootstrap.TryMergeFromSettingsFilePathAsync(settings.Path, baseline, TestContext.Current.CancellationToken);

        Assert.True(found);
        Assert.False(merged.Enabled);
        Assert.Equal("/metrics", merged.Path);
    }

    /// <summary>Verifies merge preserves baseline values when settings properties are null (absent from JSON).</summary>
    [Fact]
    public async Task MergeIntoPreservesBaselineWhenPropertiesAreNull()
    {
        var baseline = new PrometheusMetricsEndpointOptions
        {
            Enabled = true,
            Path = "/metrics",
        };

        using var settings = await TempSettingsFile.WriteAsync("squirix-prom-", """{"PrometheusMetrics":{}}""", TestContext.Current.CancellationToken);
        var (found, merged) = await PrometheusMetricsBootstrap.TryMergeFromSettingsFilePathAsync(settings.Path, baseline, TestContext.Current.CancellationToken);

        Assert.True(found);
        Assert.True(merged.Enabled);
        Assert.Equal("/metrics", merged.Path);
    }
}
