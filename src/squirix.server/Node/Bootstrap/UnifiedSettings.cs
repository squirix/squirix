using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
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
    internal static async Task<(bool Found, UnresolvedMemoryPressureOptions Merged)> TryMergeMemoryPressureFromFileAsync(
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
    internal static async Task<(bool Found, PrometheusMetricsEndpointOptions Merged)> TryMergePrometheusMetricsFromFileAsync(
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
    internal static async Task<(bool Found, TriggerOptions Merged)> TryMergeSnapshotFromFileAsync(TriggerOptions baseline, CancellationToken cancellationToken = default)
    {
        var path = ResolveSettingsPath();
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
        var (memoryPressureFound, memoryPressure) = await TryMergeMemoryPressureFromSettingsFilePathAsync(
            settingsFilePath,
            new UnresolvedMemoryPressureOptions(),
            cancellationToken).ConfigureAwait(false);
        if (memoryPressureFound)
        {
            try
            {
                _ = OptionsResolver.Resolve(memoryPressure, GcMemoryBudgetProvider.Instance);
            }
            catch (InvalidOperationException ex)
            {
                failures.Add(ex.Message);
            }
        }

        var (snapshotFound, snapshot) = await TryMergeSnapshotFromSettingsFilePathAsync(settingsFilePath, new TriggerOptions(), cancellationToken).ConfigureAwait(false);
        if (snapshotFound)
        {
            var snapshotValidator = new OptionsValidators.SnapshotTriggerOptionsValidator();
            var snapshotResult = snapshotValidator.Validate(Options.DefaultName, snapshot);
            if (snapshotResult.Failed)
                failures.AddRange(snapshotResult.Failures);
        }

        var (prometheusFound, prometheus) = await TryMergePrometheusMetricsFromSettingsFilePathAsync(settingsFilePath, new PrometheusMetricsEndpointOptions(), cancellationToken)
           .ConfigureAwait(false);
        if (!prometheusFound)
            return;

        var validator = new OptionsValidators.PrometheusEndpointOptionsValidator();
        var result = validator.Validate(Options.DefaultName, prometheus);
        if (result.Failed)
            failures.AddRange(result.Failures);
    }

    private static string? ResolveSettingsPath() => Configurator.ResolveSettingsPath();

    /// <summary>
    /// Merges <c>MemoryPressure</c> from a specific settings file path (used by tests and file resolution).
    /// </summary>
    /// <param name="path">Full path to a JSON file with optional <c>Squirix.MemoryPressure</c> section.</param>
    /// <param name="baseline">Baseline options when the section is absent.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true" /> in <c>Found</c> when the file exists and defines a <c>MemoryPressure</c> object.</returns>
    private static async Task<(bool Found, UnresolvedMemoryPressureOptions Merged)> TryMergeMemoryPressureFromSettingsFilePathAsync(
        string path,
        UnresolvedMemoryPressureOptions baseline,
        CancellationToken cancellationToken = default)
    {
        if (!File.Exists(path))
            return (false, baseline);

        return await WithSquirixRootAsync(
            path,
            baseline,
            static (root, baseline) =>
            {
                if (!root.TryGetProperty("MemoryPressure", out var memoryPressure))
                    return (false, baseline);

                var section = ServerSerializationProvider.Instance.Deserialize<PressureSettings>(memoryPressure.GetRawText());
                var merged = section is null ? baseline : section.MergeInto(baseline);
                return (true, merged);
            },
            cancellationToken).ConfigureAwait(false);
    }

    private static async Task<(bool Found, PrometheusMetricsEndpointOptions Merged)> TryMergePrometheusMetricsFromSettingsFilePathAsync(
        string file,
        PrometheusMetricsEndpointOptions baseline,
        CancellationToken cancellationToken = default)
    {
        if (!File.Exists(file))
            return (false, baseline);

        return await WithSquirixRootAsync(
            file,
            baseline,
            static (root, baseline) =>
            {
                if (!root.TryGetProperty("PrometheusMetrics", out var prometheusMetrics))
                    return (false, baseline);

                var section = ServerSerializationProvider.Instance.Deserialize<PrometheusMetricsSettings>(prometheusMetrics.GetRawText());
                var merged = section is null ? baseline : section.MergeInto(baseline);
                return (true, merged);
            },
            cancellationToken).ConfigureAwait(false);
    }

    private static async Task<(bool Found, TriggerOptions Merged)> TryMergeSnapshotFromSettingsFilePathAsync(
        string path,
        TriggerOptions baseline,
        CancellationToken cancellationToken = default)
    {
        if (!File.Exists(path))
            return (false, baseline);

        return await WithSquirixRootAsync(
            path,
            baseline,
            static (root, baseline) =>
            {
                if (!root.TryGetProperty("Snapshot", out var snapshot))
                    return (false, baseline);

                var section = ServerSerializationProvider.Instance.Deserialize<TriggerOptions>(snapshot.GetRawText());
                return (true, section ?? baseline);
            },
            cancellationToken).ConfigureAwait(false);
    }

    private static async Task<T> WithSquirixRootAsync<TState, T>(string settingsFilePath, TState state, Func<JsonElement, TState, T> action, CancellationToken cancellationToken)
    {
        var bytes = await File.ReadAllBytesAsync(settingsFilePath, cancellationToken).ConfigureAwait(false);
        using var doc = JsonDocument.Parse(bytes, JsonOptions);
        var root = doc.RootElement;
        if (root.TryGetProperty("Squirix", out var squirix))
            root = squirix;

        return action(root, state);
    }
}
