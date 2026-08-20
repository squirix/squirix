using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>Verifies Prometheus metrics settings deserialization and merge via <see cref="PrometheusMetricsBootstrap" />.</summary>
[Immutable]
public sealed class PrometheusMetricsSettingsTests : ServerUnitTestBase
{
    /// <summary>
    /// Verifies System.Text.Json binds private <c>path</c>/<c>enabled</c> properties
    /// (via <see cref="System.Text.Json.Serialization.JsonIncludeAttribute" />) and merge overrides the baseline.
    /// </summary>
    [Fact]
    public async Task DeserializeAndMergeIntoAppliesJsonOverridesAsync()
    {
        var baseline = new PrometheusMetricsEndpointOptions
        {
            Enabled = true,
            Path = "/metrics",
        };

        using var settings = await TempSettingsFile.WriteAsync("squirix-prom-", """{"PrometheusMetrics":{"path":"/custom-metrics","enabled":false}}""", DefaultCancellationToken);
        var (found, merged) = await PrometheusMetricsBootstrap.TryMergeFromSettingsFilePathAsync(settings.Path, baseline, DefaultCancellationToken);

        Assert.True(found);
        Assert.False(merged.Enabled);
        Assert.Equal("/custom-metrics", merged.Path);
    }

    /// <summary>Verifies a partial JSON section overrides only present fields and keeps baseline for absent ones.</summary>
    [Fact]
    public async Task DeserializeAndMergeKeepsBaselineAsync()
    {
        var baseline = new PrometheusMetricsEndpointOptions
        {
            Enabled = true,
            Path = "/metrics",
        };

        using var settings = await TempSettingsFile.WriteAsync("squirix-prom-", """{"PrometheusMetrics":{"enabled":false}}""", DefaultCancellationToken);
        var (found, merged) = await PrometheusMetricsBootstrap.TryMergeFromSettingsFilePathAsync(settings.Path, baseline, DefaultCancellationToken);

        Assert.True(found);
        Assert.False(merged.Enabled);
        Assert.Equal("/metrics", merged.Path);
    }

    /// <summary>Verifies merge preserves baseline values when settings properties are null (absent from JSON).</summary>
    [Fact]
    public async Task MergeIntoPreservesBaselineAsync()
    {
        var baseline = new PrometheusMetricsEndpointOptions
        {
            Enabled = true,
            Path = "/metrics",
        };

        using var settings = await TempSettingsFile.WriteAsync("squirix-prom-", """{"PrometheusMetrics":{}}""", DefaultCancellationToken);
        var (found, merged) = await PrometheusMetricsBootstrap.TryMergeFromSettingsFilePathAsync(settings.Path, baseline, DefaultCancellationToken);

        Assert.True(found);
        Assert.True(merged.Enabled);
        Assert.Equal("/metrics", merged.Path);
    }
}
