using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.Serialization;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Bootstrap;

/// <summary>
/// Loads unified settings from "Squirix.settings.json" (if present).
/// Looks first in CurrentDirectory, then in AppContext.BaseDirectory.
/// </summary>
internal static class UnifiedSettings
{
    private static readonly JsonDocumentOptions JsonOptions = new() { AllowTrailingCommas = true, CommentHandling = JsonCommentHandling.Skip };

    /// <summary>
    /// Merges the <c>MemoryPressure</c> JSON section onto <paramref name="baseline" /> when the settings file exists and contains that section.
    /// </summary>
    /// <param name="baseline">Baseline options when the section is absent.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// A tuple where <c>Found</c> is <see langword="true" /> when the settings file exists and defines a <c>MemoryPressure</c> object,
    /// and <c>Merged</c> is the merged result.
    /// </returns>
    public static async Task<(bool Found, UnresolvedMemoryPressureOptions Merged)> TryMergeMemoryPressureFromFileAsync(
        UnresolvedMemoryPressureOptions baseline,
        CancellationToken cancellationToken = default)
    {
        var path = ResolveSettingsPath();
        return path is null ? (false, baseline) : await TryMergeMemoryPressureFromSettingsFilePathAsync(path, baseline, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Merges the <c>PrometheusMetrics</c> JSON section onto <paramref name="baseline" /> when the settings file exists and contains that section.
    /// </summary>
    /// <param name="baseline">Baseline options when the section is absent.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// A tuple where <c>Found</c> is <see langword="true" /> when the settings file exists and defines a <c>PrometheusMetrics</c> object,
    /// and <c>Merged</c> is the merged result.
    /// </returns>
    public static async Task<(bool Found, PrometheusMetricsEndpointOptions Merged)> TryMergePrometheusMetricsFromFileAsync(
        PrometheusMetricsEndpointOptions baseline,
        CancellationToken cancellationToken = default)
    {
        var path = ResolveSettingsPath();
        return path is null ? (false, baseline) : await TryMergePrometheusMetricsFromSettingsFilePathAsync(path, baseline, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Merges the <c>Snapshot</c> JSON section onto <paramref name="baseline" /> when the settings file exists and contains that section.
    /// </summary>
    /// <param name="baseline">Baseline options when the section is absent.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// A tuple where <c>Found</c> is <see langword="true" /> when the settings file exists and defines a <c>Snapshot</c> object,
    /// and <c>Merged</c> is the merged result.
    /// </returns>
    public static async Task<(bool Found, SnapshotTriggerOptions Merged)> TryMergeSnapshotFromFileAsync(
        SnapshotTriggerOptions baseline,
        CancellationToken cancellationToken = default)
    {
        var path = ResolveSettingsPath();
        return path is null ? (false, baseline) : await TryMergeSnapshotFromSettingsFilePathAsync(path, baseline, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Loads <c>Squirix:Cluster</c> from a specific settings JSON file path (used by tests and explicit file resolution).
    /// </summary>
    /// <param name="settingsFilePath">Full path to a JSON file with optional <c>Squirix.Cluster</c> section.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// A tuple where <c>Found</c> is <see langword="true" /> when the file exists and defines a <c>Cluster</c> object,
    /// and <c>Config</c> is the loaded cluster configuration.
    /// </returns>
    internal static async Task<(bool Found, ClusterConfig? Config)> TryLoadClusterConfigFromSettingsFilePathAsync(
        string settingsFilePath,
        CancellationToken cancellationToken = default)
    {
        var (success, options, _) = await SquirixServerConfiguration.TryLoadFromFileAsync(settingsFilePath, cancellationToken).ConfigureAwait(false);
        if (!success || options is null)
            return (false, null);

        return (true, SquirixServerConfiguration.ToClusterConfig(options));
    }

    /// <summary>
    /// Merges <c>MemoryPressure</c> from a specific settings file path (used by tests and file resolution).
    /// </summary>
    /// <param name="path">Full path to a JSON file with optional <c>Squirix.MemoryPressure</c> section.</param>
    /// <param name="baseline">Baseline options when the section is absent.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true" /> in <c>Found</c> when the file exists and defines a <c>MemoryPressure</c> object.</returns>
    internal static async Task<(bool Found, UnresolvedMemoryPressureOptions Merged)> TryMergeMemoryPressureFromSettingsFilePathAsync(
        string path,
        UnresolvedMemoryPressureOptions baseline,
        CancellationToken cancellationToken = default)
    {
        if (!File.Exists(path))
            return (false, baseline);

        return await WithSquirixRootAsync(
            path,
            root =>
            {
                if (!root.TryGetProperty("MemoryPressure", out var memoryPressure))
                    return (false, baseline);

                var section = SerializationProvider.Instance.Deserialize<MemoryPressureSettings>(memoryPressure.GetRawText());
                var merged = section is null ? baseline : section.MergeInto(baseline);
                return (true, merged);
            },
            cancellationToken).ConfigureAwait(false);
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
        var (memoryPressureFound, memoryPressure) = await TryMergeMemoryPressureFromSettingsFilePathAsync(
            settingsFilePath,
            new UnresolvedMemoryPressureOptions(),
            cancellationToken).ConfigureAwait(false);
        if (memoryPressureFound)
        {
            try
            {
                _ = MemoryPressureOptionsResolver.Resolve(memoryPressure, GcMemoryBudgetProvider.Instance);
            }
            catch (InvalidOperationException ex)
            {
                failures.Add(ex.Message);
            }
        }

        var (snapshotFound, snapshot) = await TryMergeSnapshotFromSettingsFilePathAsync(settingsFilePath, new SnapshotTriggerOptions(), cancellationToken).ConfigureAwait(false);
        if (snapshotFound)
        {
            var snapshotValidator = new SquirixOptionsValidators.SnapshotTriggerOptionsValidator();
            var snapshotResult = snapshotValidator.Validate(Options.DefaultName, snapshot);
            if (snapshotResult.Failed)
                failures.AddRange(snapshotResult.Failures);
        }

        var (prometheusFound, prometheus) = await TryMergePrometheusMetricsFromSettingsFilePathAsync(settingsFilePath, new PrometheusMetricsEndpointOptions(), cancellationToken)
           .ConfigureAwait(false);
        if (!prometheusFound)
            return;

        var validator = new SquirixOptionsValidators.PrometheusMetricsEndpointOptionsValidator();
        var result = validator.Validate(Options.DefaultName, prometheus);
        if (result.Failed)
            failures.AddRange(result.Failures);
    }

    internal static async Task<(bool Found, SnapshotTriggerOptions Merged)> TryMergeSnapshotFromSettingsFilePathAsync(
        string settingsFilePath,
        SnapshotTriggerOptions baseline,
        CancellationToken cancellationToken = default)
    {
        if (!File.Exists(settingsFilePath))
            return (false, baseline);

        return await WithSquirixRootAsync(
            settingsFilePath,
            root =>
            {
                if (!root.TryGetProperty("Snapshot", out var snapshot))
                    return (false, baseline);

                var section = SerializationProvider.Instance.Deserialize<SnapshotTriggerOptions>(snapshot.GetRawText());
                return (true, section ?? baseline);
            },
            cancellationToken).ConfigureAwait(false);
    }

    private static string? ResolveSettingsPath() => SquirixServerConfiguration.ResolveSettingsPath();

    private static async Task<(bool Found, PrometheusMetricsEndpointOptions Merged)> TryMergePrometheusMetricsFromSettingsFilePathAsync(
        string settingsFilePath,
        PrometheusMetricsEndpointOptions baseline,
        CancellationToken cancellationToken = default)
    {
        if (!File.Exists(settingsFilePath))
            return (false, baseline);

        return await WithSquirixRootAsync(
            settingsFilePath,
            root =>
            {
                if (!root.TryGetProperty("PrometheusMetrics", out var prometheusMetrics))
                    return (false, baseline);

                var section = SerializationProvider.Instance.Deserialize<PrometheusMetricsSettings>(prometheusMetrics.GetRawText());
                var merged = section is null ? baseline : section.MergeInto(baseline);
                return (true, merged);
            },
            cancellationToken).ConfigureAwait(false);
    }

    private static async Task<T> WithSquirixRootAsync<T>(string settingsFilePath, Func<JsonElement, T> action, CancellationToken cancellationToken)
    {
        var bytes = await File.ReadAllBytesAsync(settingsFilePath, cancellationToken).ConfigureAwait(false);
        using var doc = JsonDocument.Parse(bytes, JsonOptions);
        var root = doc.RootElement;
        if (root.TryGetProperty("Squirix", out var squirix))
            root = squirix;

        return action(root);
    }
}
