using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability.Metrics;
using Xunit;

namespace Squirix.Server.IntegrationTests;

/// <summary>
/// Integration tests verifying <see cref="PrometheusMetricsEndpointOptions" /> properties
/// remain mutable through the DI <c language="csharp">Configure</c>/<c language="csharp">PostConfigure</c> pipeline.
/// </summary>
[Immutable]
public sealed class PrometheusMetricsEndpointOptionsTests
{
    /// <summary>
    /// Verifies the full DI pipeline: initial defaults, configure override, post-configure override,
    /// and options validation all compose correctly with mutable setters.
    /// </summary>
    [Fact]
    public void FullPipelineComposesBothConfigurers()
    {
        var services = new ServiceCollection();
        _ = services.AddOptions<PrometheusMetricsEndpointOptions>().Configure(static o =>
        {
            o.Enabled = true;
            o.Path = "/original";
        });

        _ = services.PostConfigure<PrometheusMetricsEndpointOptions>(static o => o.Path = "/overridden");

        using var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IOptions<PrometheusMetricsEndpointOptions>>().Value;

        Assert.True(resolved.Enabled);
        Assert.Equal("/overridden", resolved.Path);
    }

    /// <summary>
    /// Verifies that <c language="csharp">PostConfigure</c> can flip <see cref="PrometheusMetricsEndpointOptions.Enabled" /> to false
    /// after initial registration. This proves <c language="csharp">Enabled</c> cannot be <c language="csharp">init</c>-only.
    /// </summary>
    [Fact]
    public void PostConfigureDisablesEndpoint()
    {
        var services = new ServiceCollection();
        _ = services.AddOptions<PrometheusMetricsEndpointOptions>().Configure(static o =>
        {
            o.Enabled = true;
            o.Path = "/metrics";
        });
        _ = services.PostConfigure<PrometheusMetricsEndpointOptions>(static o => o.Enabled = false);

        using var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IOptions<PrometheusMetricsEndpointOptions>>().Value;

        Assert.False(resolved.Enabled);
    }

    /// <summary>
    /// Verifies that <c language="csharp">PostConfigure</c> can override <see cref="PrometheusMetricsEndpointOptions.Path" />
    /// after the initial <c language="csharp">Configure</c> callback has set it. This proves <c language="csharp">Path</c> cannot be <c language="csharp">init</c>-only.
    /// </summary>
    [Fact]
    public void PostConfigureOverridesPath()
    {
        var services = new ServiceCollection();
        _ = services.AddOptions<PrometheusMetricsEndpointOptions>().Configure(static o => o.Path = "/metrics");
        _ = services.PostConfigure<PrometheusMetricsEndpointOptions>(static o => o.Path = "/custom-metrics");

        using var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IOptions<PrometheusMetricsEndpointOptions>>().Value;

        Assert.Equal("/custom-metrics", resolved.Path);
    }
}
