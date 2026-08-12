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
    /// <summary>
    /// Merges the <c>Snapshot</c> JSON section onto <paramref name="baseline" /> when the settings file exists and contains that section.
    /// </summary>
    /// <param name="baseline">Baseline options when the section is absent.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// A tuple where <c>Found</c> is <see langword="true" /> when the settings file exists and defines a <c>Snapshot</c> object,
    /// and <c>Merged</c> is the merged result.
    /// </returns>
    internal static async Task<(bool Found, TriggerOptions Merged)> TryMergeSnapshotFromFileAsync(TriggerOptions baseline, CancellationToken cancellationToken = default)
    {
        var path = SettingsJson.FindSettingsPath();
        return path is null ? (false, baseline) : await TryMergeSnapshotFromSettingsFilePathAsync(path, baseline, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Validates optional <c>MemoryPressure</c>, <c>Snapshot</c>, and <c>PrometheusMetrics</c> sections when present.
    /// </summary>
    /// <param name="settingsFilePath">Settings JSON path.</param>
    /// <param name="failures">Collected validation failures.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes after optional sections are validated.</returns>
    internal static async Task ValidateOptionalSectionsAsync(string settingsFilePath, List<string> failures, CancellationToken cancellationToken = default)
    {
        var (found, pressure) = await PressureBootstrap.TryMergeFromSettingsFilePathAsync(settingsFilePath, new UnresolvedMemoryPressureOptions(), cancellationToken)
                                                       .ConfigureAwait(false);
        if (found)
            try
            {
                _ = OptionsResolver.Resolve(pressure, GcMemoryBudgetProvider.Instance);
            }
            catch (InvalidOperationException ex)
            {
                failures.Add(ex.Message);
            }

        var (snapshotFound, snapshot) = await TryMergeSnapshotFromSettingsFilePathAsync(settingsFilePath, new TriggerOptions(), cancellationToken).ConfigureAwait(false);
        if (snapshotFound)
        {
            var snapshotValidator = new TriggerOptionsValidator();
            var snapshotResult = snapshotValidator.Validate(Options.DefaultName, snapshot);
            if (snapshotResult.Failed)
                failures.AddRange(snapshotResult.Failures);
        }

        var options = new PrometheusMetricsEndpointOptions();
        var (prometheusFound, prometheus) = await PrometheusMetricsBootstrap.TryMergeFromSettingsFilePathAsync(settingsFilePath, options, cancellationToken).ConfigureAwait(false);
        if (!prometheusFound)
            return;

        var validator = new PrometheusEndpointOptionsValidator();
        var result = validator.Validate(Options.DefaultName, prometheus);
        if (result.Failed)
            failures.AddRange(result.Failures);
    }

    private static async Task<(bool Found, TriggerOptions Merged)> TryMergeSnapshotFromSettingsFilePathAsync(
        string path,
        TriggerOptions baseline,
        CancellationToken cancellationToken = default)
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

                var section = SerializerProvider.Instance.Deserialize<TriggerOptions>(snapshot.GetRawText());
                return (true, section ?? baseline);
            },
            cancellationToken).ConfigureAwait(false);
    }
}
