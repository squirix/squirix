using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Node.Observability.Metrics;
using Xunit;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>Verifies Prometheus metrics settings deserialization and merge via <see cref="PrometheusMetricsBootstrap" />.</summary>
public sealed class PrometheusMetricsSettingsTests
{
    /// <summary>Verifies merge preserves baseline values when settings properties are null (absent from JSON).</summary>
    [Fact]
    public async Task MergeIntoPreservesBaselineWhenPropertiesAreNull()
    {
        var baseline = new PrometheusMetricsEndpointOptions
        {
            Enabled = true,
            Path = "/metrics",
        };

        var path = await WriteSettingsAsync("""{"PrometheusMetrics":{}}""").ConfigureAwait(true);
        try
        {
            var (found, merged) = await PrometheusMetricsBootstrap.TryMergeFromSettingsFilePathAsync(path, baseline, TestContext.Current.CancellationToken).ConfigureAwait(true);

            Assert.True(found);
            Assert.True(merged.Enabled);
            Assert.Equal("/metrics", merged.Path);
        }
        finally
        {
            File.Delete(path);
        }
    }

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

        var path = await WriteSettingsAsync("""{"PrometheusMetrics":{"path":"/custom-metrics","enabled":false}}""").ConfigureAwait(true);
        try
        {
            var (found, merged) = await PrometheusMetricsBootstrap.TryMergeFromSettingsFilePathAsync(path, baseline, TestContext.Current.CancellationToken).ConfigureAwait(true);

            Assert.True(found);
            Assert.False(merged.Enabled);
            Assert.Equal("/custom-metrics", merged.Path);
        }
        finally
        {
            File.Delete(path);
        }
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

        var path = await WriteSettingsAsync("""{"PrometheusMetrics":{"enabled":false}}""").ConfigureAwait(true);
        try
        {
            var (found, merged) = await PrometheusMetricsBootstrap.TryMergeFromSettingsFilePathAsync(path, baseline, TestContext.Current.CancellationToken).ConfigureAwait(true);

            Assert.True(found);
            Assert.False(merged.Enabled);
            Assert.Equal("/metrics", merged.Path);
        }
        finally
        {
            File.Delete(path);
        }
    }

    private static async Task<string> WriteSettingsAsync(string json)
    {
        var path = Path.Join(Path.GetTempPath(), "squirix-prom-" + Path.GetRandomFileName() + ".json");
        await File.WriteAllTextAsync(path, json, TestContext.Current.CancellationToken).ConfigureAwait(true);
        return path;
    }
}
