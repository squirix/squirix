using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Squirix.Server.Node.Observability.Metrics;
using Xunit;

namespace Squirix.Server.IntegrationTests.Options;

/// <summary>
/// Integration tests verifying <see cref="PrometheusMetricsEndpointOptions" /> properties
/// remain mutable through the DI <c>Configure</c>/<c>PostConfigure</c> pipeline.
/// </summary>
public sealed class PrometheusMetricsEndpointOptionsTests
{
    /// <summary>
    /// Verifies the full DI pipeline: initial defaults, configure override, post-configure override,
    /// and options validation all compose correctly with mutable setters.
    /// </summary>
    [Fact]
    public void FullPipelineComposesConfigureAndPostConfigure()
    {
        var services = new ServiceCollection();
        _ = services.AddOptions<PrometheusMetricsEndpointOptions>().Configure(static o =>
        {
            o.Enabled = true;
            o.Path = "/original";
        });

        _ = services.PostConfigure<PrometheusMetricsEndpointOptions>(static o =>
        {
            o.Path = "/overridden";
        });

        using var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IOptions<PrometheusMetricsEndpointOptions>>().Value;

        Assert.True(resolved.Enabled);
        Assert.Equal("/overridden", resolved.Path);
    }

    /// <summary>
    /// Verifies that <c>PostConfigure</c> can flip <see cref="PrometheusMetricsEndpointOptions.Enabled" /> to false
    /// after initial registration. This proves <c>Enabled</c> cannot be <c>init</c>-only.
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
        _ = services.PostConfigure<PrometheusMetricsEndpointOptions>(static o => { o.Enabled = false; });

        using var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IOptions<PrometheusMetricsEndpointOptions>>().Value;

        Assert.False(resolved.Enabled);
    }

    /// <summary>
    /// Verifies that <c>PostConfigure</c> can override <see cref="PrometheusMetricsEndpointOptions.Path" />
    /// after the initial <c>Configure</c> callback has set it. This proves <c>Path</c> cannot be <c>init</c>-only.
    /// </summary>
    [Fact]
    public void PostConfigureOverridesPath()
    {
        var services = new ServiceCollection();
        _ = services.AddOptions<PrometheusMetricsEndpointOptions>().Configure(static o => { o.Path = "/metrics"; });
        _ = services.PostConfigure<PrometheusMetricsEndpointOptions>(static o => { o.Path = "/custom-metrics"; });

        using var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IOptions<PrometheusMetricsEndpointOptions>>().Value;

        Assert.Equal("/custom-metrics", resolved.Path);
    }
}
