using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Node.Observability.Metrics;
using Xunit;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>Verifies Prometheus metrics settings deserialization and merge via <see cref="PrometheusMetricsBootstrap" />.</summary>
public sealed class PrometheusMetricsSettingsTests
{
    /// <summary>
    /// Verifies <see cref="PrometheusMetricsSettings.MergeInto" /> preserves baseline values
    /// when settings properties are null (absent from JSON).
    /// </summary>
    [Fact]
    public async Task DeserializeAndMergeKeepsBaselineForAbsentFields()
    {
        var baseline = new PrometheusMetricsEndpointOptions
        {
            Enabled = true,
            Path = "/metrics",
        };

        var path = await WriteSettingsAsync("""{"PrometheusMetrics":{"enabled":false}}""");
        try
        {
            var (found, merged) = await PrometheusMetricsBootstrap.TryMergeFromSettingsFilePathAsync(path, baseline, TestContext.Current.CancellationToken);

            Assert.True(found);
            Assert.False(merged.Enabled);
            Assert.Equal("/metrics", merged.Path);
        }
        finally
        {
            File.Delete(path);
        }
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

        var path = await WriteSettingsAsync("""{"PrometheusMetrics":{}}""");
        try
        {
            var (found, merged) = await PrometheusMetricsBootstrap.TryMergeFromSettingsFilePathAsync(path, baseline, TestContext.Current.CancellationToken);

            Assert.True(found);
            Assert.True(merged.Enabled);
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
        await File.WriteAllTextAsync(path, json, TestContext.Current.CancellationToken);
        return path;
    }
}
