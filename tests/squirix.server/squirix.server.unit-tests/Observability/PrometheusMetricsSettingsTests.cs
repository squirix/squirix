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
    /// <summary>Verifies public server options check the scrape path when Prometheus export is enabled.</summary>
    /// <param name="enabled">Whether Prometheus export is enabled.</param>
    /// <param name="path">The configured scrape path.</param>
    /// <param name="expectedError">The validation error, or null on success.</param>
    [Test]
    [Arguments(true, "", "Prometheus metrics Path must be non-empty when the endpoint is enabled.")]
    [Arguments(true, " ", "Prometheus metrics Path must be non-empty when the endpoint is enabled.")]
    [Arguments(true, "metrics", "Prometheus metrics Path must start with '/'.")]
    [Arguments(true, "/custom-metrics", null)]
    [Arguments(false, "metrics", null)]
    public async Task PublicOptionsValidateEnabledExportPath(bool enabled, string path, string? expectedError)
    {
        var options = new SquirixServerOptions { PrometheusMetricsEnabled = enabled, PrometheusMetricsPath = path };
        var valid = options.TryValidate(out var errors);

        _ = await Assert.That(valid).IsEqualTo(expectedError == null);
        _ = await Assert.That(errors.Count).IsEqualTo(expectedError == null ? 0 : 1);
        if (expectedError != null)
            _ = await Assert.That(errors[0]).IsEqualTo(expectedError);
    }

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
