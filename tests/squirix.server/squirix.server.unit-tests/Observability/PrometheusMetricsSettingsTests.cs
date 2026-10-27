using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>Verifies Prometheus metrics settings deserialization and merge via <see cref="PrometheusMetricsBootstrap" />.</summary>
[Immutable]
public sealed class PrometheusMetricsSettingsTests : ServerUnitTestBase
{
    /// <summary>Verifies a partial JSON section overrides only present fields and keeps baseline for absent ones.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DeserializeAndMergeKeepsBaselineAsync(CancellationToken cancellationToken)
    {
        var baseline = new PrometheusMetricsEndpointOptions
        {
            Enabled = true,
            Path = "/metrics",
        };

        using var settings = await TempSettingsFile.WriteAsync("squirix-prom-", """{"PrometheusMetrics":{"enabled":false}}""", cancellationToken);
        var (found, merged) = await PrometheusMetricsBootstrap.MergeFromSettingsFilePathAsync(settings.Path, baseline, cancellationToken);

        _ = await Assert.That(found).IsTrue();
        _ = await Assert.That(merged.Enabled).IsFalse();
        _ = await Assert.That(merged.Path).IsEqualTo("/metrics");
    }

    /// <summary>
    /// Verifies System.Text.Json binds private <c language="csharp">path</c>/<c language="csharp">enabled</c> properties
    /// (via <see cref="System.Text.Json.Serialization.JsonIncludeAttribute" />) and merge overrides the baseline.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MergeAppliesJsonOverridesAsync(CancellationToken cancellationToken)
    {
        var baseline = new PrometheusMetricsEndpointOptions
        {
            Enabled = true,
            Path = "/metrics",
        };

        using var settings = await TempSettingsFile.WriteAsync("squirix-prom-", """{"PrometheusMetrics":{"path":"/custom-metrics","enabled":false}}""", cancellationToken);
        var (found, merged) = await PrometheusMetricsBootstrap.MergeFromSettingsFilePathAsync(settings.Path, baseline, cancellationToken);

        _ = await Assert.That(found).IsTrue();
        _ = await Assert.That(merged.Enabled).IsFalse();
        _ = await Assert.That(merged.Path).IsEqualTo("/custom-metrics");
    }

    /// <summary>Verifies merge preserves baseline values when settings properties are null (absent from JSON).</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MergeIntoPreservesBaselineAsync(CancellationToken cancellationToken)
    {
        var baseline = new PrometheusMetricsEndpointOptions
        {
            Enabled = true,
            Path = "/metrics",
        };

        using var settings = await TempSettingsFile.WriteAsync("squirix-prom-", """{"PrometheusMetrics":{}}""", cancellationToken);
        var (found, merged) = await PrometheusMetricsBootstrap.MergeFromSettingsFilePathAsync(settings.Path, baseline, cancellationToken);

        _ = await Assert.That(found).IsTrue();
        _ = await Assert.That(merged.Enabled).IsTrue();
        _ = await Assert.That(merged.Path).IsEqualTo("/metrics");
    }
}
