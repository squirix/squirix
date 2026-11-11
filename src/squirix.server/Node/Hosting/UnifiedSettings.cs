using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Squirix.Server.Core;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Hosting;

/// <summary>
/// Loads unified settings from "Squirix.settings.json" (if present).
/// Looks first in CurrentDirectory, then in AppContext.BaseDirectory.
/// </summary>
internal static class UnifiedSettings
{
    /// <summary>Loads snapshot trigger options from the discovered settings file, if present.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The snapshot trigger options.</returns>
    internal static async Task<TriggerOptions> LoadSnapshotOptionsAsync(CancellationToken cancellationToken)
    {
        var baseline = new TriggerOptions();
        var path = SettingsJson.FindSettingsPath();
        if (path == null)
            return baseline;

        var (_, merged) = await MergeAsync(path, baseline, cancellationToken).ConfigureAwait(false);
        return merged;
    }

    /// <summary>
    /// Validates optional <c language="csharp">MemoryPressure</c>, <c language="csharp">Snapshot</c>, and <c language="csharp">PrometheusMetrics</c> sections when present.
    /// </summary>
    /// <param name="settingsFilePath">Settings JSON path.</param>
    /// <param name="failures">Collected validation failures.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes after optional sections are validated.</returns>
    internal static async Task ValidateOptionalSectionsAsync(string settingsFilePath, List<string> failures, CancellationToken cancellationToken = default)
    {
        var (found, pressure) = await PressureBootstrap.MergeFromSettingsFilePathAsync(settingsFilePath, new UnresolvedMemoryPressureOptions(), cancellationToken)
                                                       .ConfigureAwait(false);
        if (found)
        {
            try
            {
                _ = OptionsResolver.Resolve(pressure, GcMemoryBudgetProvider.Instance);
            }
            catch (InvalidOperationException ex)
            {
                failures.Add(ex.Message);
            }
        }

        var (snapshotFound, snapshot) = await MergeAsync(settingsFilePath, new TriggerOptions(), cancellationToken).ConfigureAwait(false);
        if (snapshotFound)
        {
            var snapshotValidator = new TriggerOptionsValidator();
            var snapshotResult = snapshotValidator.Validate(Options.DefaultName, snapshot);
            if (snapshotResult.Failed)
                failures.AddRange(snapshotResult.Failures);
        }

        var options = new PrometheusMetricsEndpointOptions();
        var (prometheusFound, prometheus) = await PrometheusMetricsBootstrap.MergeFromSettingsFilePathAsync(settingsFilePath, options, cancellationToken).ConfigureAwait(false);
        if (!prometheusFound)
            return;

        var validator = new PrometheusEndpointOptionsValidator();
        var result = validator.Validate(Options.DefaultName, prometheus);
        if (result.Failed)
            failures.AddRange(result.Failures);
    }

    private static async Task<(bool Found, TriggerOptions Merged)> MergeAsync(string path, TriggerOptions baseline, CancellationToken cancellationToken = default)
    {
        if (!File.Exists(path))
            return (false, baseline);

        return await SettingsJson.WithSquirixRootAsync(
            path,
            baseline,
            static (root, baseline) =>
            {
                if (!root.TryGetProperty("Snapshot", out var snapshot))
                    return (false, baseline);

                var section = ServerJsonSerializer.Deserialize<TriggerOptions>(snapshot.GetRawText());
                return (true, section ?? baseline);
            },
            cancellationToken).ConfigureAwait(false);
    }
}
