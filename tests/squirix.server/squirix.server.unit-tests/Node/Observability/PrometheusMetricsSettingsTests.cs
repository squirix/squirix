using Squirix.Server.Node.Observability.Metrics;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Observability;

/// <summary>
/// Verifies <see cref="PrometheusMetricsSettings" /> deserialization shape and merge logic.
/// </summary>
public sealed class PrometheusMetricsSettingsTests
{
    /// <summary>
    /// Verifies all properties default to <see langword="null" /> (JSON absent fields).
    /// </summary>
    [Fact]
    public void DefaultPropertiesAreNull()
    {
        var settings = new PrometheusMetricsSettings();

        Assert.Null(settings.Enabled);
        Assert.Null(settings.Path);
    }

    /// <summary>
    /// Verifies init-only properties are settable (as JSON deserialization requires).
    /// </summary>
    [Fact]
    public void InitPropertiesAreSettable()
    {
        var settings = new PrometheusMetricsSettings
        {
            Enabled = true,
            Path = "/custom-metrics",
        };

        Assert.True(settings.Enabled);
        Assert.Equal("/custom-metrics", settings.Path);
    }

    /// <summary>
    /// Verifies whitespace-only <see cref="PrometheusMetricsSettings.Path" /> falls back to baseline.
    /// </summary>
    [Fact]
    public void MergeIntoFallsBackToBaselinePathWhenWhitespace()
    {
        var baseline = new PrometheusMetricsEndpointOptions { Path = "/metrics" };
        var settings = new PrometheusMetricsSettings { Path = "   " };

        var merged = settings.MergeInto(baseline);

        Assert.Equal("/metrics", merged.Path);
    }

    /// <summary>
    /// Verifies <see cref="PrometheusMetricsSettings.MergeInto" /> overrides baseline values
    /// when settings properties are non-null.
    /// </summary>
    [Fact]
    public void MergeIntoOverridesBaselineWhenPropertiesAreSet()
    {
        var baseline = new PrometheusMetricsEndpointOptions
        {
            Enabled = true,
            Path = "/metrics",
        };

        var settings = new PrometheusMetricsSettings
        {
            Enabled = false,
            Path = "/prom",
        };

        var merged = settings.MergeInto(baseline);

        Assert.False(merged.Enabled);
        Assert.Equal("/prom", merged.Path);
    }

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
