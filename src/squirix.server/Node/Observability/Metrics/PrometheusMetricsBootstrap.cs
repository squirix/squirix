using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Observability.Metrics;

internal static class PrometheusMetricsBootstrap
{
    internal static async Task<PrometheusMetricsEndpointOptions> LoadAsync(CancellationToken cancellationToken = default)
    {
        var baseline = new PrometheusMetricsEndpointOptions();
        var path = SettingsJson.FindSettingsPath();
        if (path == null)
            return baseline;

        var (_, merged) = await MergeFromSettingsFilePathAsync(path, baseline, cancellationToken).ConfigureAwait(false);
        return merged;
    }

    /// <summary>Merges <c language="csharp">PrometheusMetrics</c> from a specific settings file path.</summary>
    /// <param name="file">Full path to a JSON settings file.</param>
    /// <param name="baseline">Baseline options when the section is absent.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// A tuple where <c language="csharp">Found</c> is <see langword="true" /> when the file exists and defines a <c language="csharp">PrometheusMetrics</c> object,
    /// and <c language="csharp">Merged</c> is the merged result.
    /// </returns>
    internal static async Task<(bool Found, PrometheusMetricsEndpointOptions Merged)> MergeFromSettingsFilePathAsync(
        string file,
        PrometheusMetricsEndpointOptions baseline,
        CancellationToken cancellationToken = default)
    {
        if (!File.Exists(file))
            return (false, baseline);

        return await SettingsJson.WithSquirixRootAsync(
            file,
            baseline,
            static (root, baseline) =>
            {
                if (!root.TryGetProperty("PrometheusMetrics", out var prometheusMetrics))
                    return (false, baseline);

                var section = ServerJsonSerializer.Deserialize<PrometheusMetricsSettings>(prometheusMetrics.GetRawText());
                var merged = section == null ? baseline : section.MergeInto(baseline);
                return (true, merged);
            },
            cancellationToken).ConfigureAwait(false);
    }
}
