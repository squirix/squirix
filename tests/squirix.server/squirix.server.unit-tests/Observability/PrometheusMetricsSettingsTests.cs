using Squirix.Server.Node.Observability.Metrics;
using Xunit;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>
/// Verifies <see cref="PrometheusMetricsSettings" /> deserialization shape and merge logic.
/// </summary>
public sealed class PrometheusMetricsSettingsTests
{
    /// <summary>
    /// Verifies <see cref="PrometheusMetricsSettings.MergeInto" /> preserves baseline values
    /// when settings properties are null (absent from JSON).
    /// </summary>
    [Fact]
    public void MergeIntoPreservesBaselineWhenPropertiesAreNull()
    {
        var baseline = new PrometheusMetricsEndpointOptions
        {
            Enabled = true,
            Path = "/metrics",
        };

        var settings = new PrometheusMetricsSettings();

        var merged = settings.MergeInto(baseline);

        Assert.True(merged.Enabled);
        Assert.Equal("/metrics", merged.Path);
    }
}
