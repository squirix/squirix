using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using Microsoft.Extensions.Options;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.Serialization;

namespace Squirix.Server.Node.Bootstrap;

/// <summary>
/// Loads unified settings from "Squirix.settings.json" (if present).
/// Looks first in CurrentDirectory, then in AppContext.BaseDirectory.
/// </summary>
internal static class UnifiedSettings
{
    /// <summary>
    /// Merges the <c>MemoryPressure</c> JSON section onto <paramref name="baseline" /> when the settings file exists and contains that section.
    /// </summary>
    /// <param name="baseline">Baseline options when the section is absent.</param>
    /// <param name="merged">The merged result; equal to <paramref name="baseline" /> when the section is absent.</param>
    /// <returns><see langword="true" /> when the settings file exists and defines a <c>MemoryPressure</c> object.</returns>
    public static bool TryMergeMemoryPressureFromFile(MemoryPressureOptions baseline, out MemoryPressureOptions merged)
    {
        merged = baseline;
        var path = ResolveSettingsPath();
        return path is not null && TryMergeMemoryPressureFromSettingsFilePath(path, baseline, out merged);
    }

    /// <summary>
    /// Merges the <c>PrometheusMetrics</c> JSON section onto <paramref name="baseline" /> when the settings file exists and contains that section.
    /// </summary>
    /// <param name="baseline">Baseline options when the section is absent.</param>
    /// <param name="merged">The merged result; equal to <paramref name="baseline" /> when the section is absent.</param>
    /// <returns><see langword="true" /> when the settings file exists and defines a <c>PrometheusMetrics</c> object.</returns>
    public static bool TryMergePrometheusMetricsFromFile(PrometheusMetricsEndpointOptions baseline, out PrometheusMetricsEndpointOptions merged)
    {
        merged = baseline;
        var path = ResolveSettingsPath();
        return path is not null && TryMergePrometheusMetricsFromSettingsFilePath(path, baseline, out merged);
    }

    /// <summary>
    /// Loads <c>Squirix:Cluster</c> from a specific settings JSON file path (used by tests and explicit file resolution).
    /// </summary>
    /// <param name="settingsFilePath">Full path to a JSON file with optional <c>Squirix.Cluster</c> section.</param>
    /// <param name="config">The loaded cluster configuration when the method returns <see langword="true" />.</param>
    /// <returns>
    /// <see langword="true" /> when the file exists and defines a <c>Cluster</c> object; otherwise <see langword="false" />.
    /// </returns>
    internal static bool TryLoadClusterConfigFromSettingsFilePath(string settingsFilePath, out ClusterConfig config)
    {
        config = null!;
        if (!SquirixServerConfiguration.TryLoadFromFile(settingsFilePath, out var options, out _))
            return false;

        config = SquirixServerConfiguration.ToClusterConfig(options);
        return true;
    }

    /// <summary>
    /// Merges <c>MemoryPressure</c> from a specific settings file path (used by tests and file resolution).
    /// </summary>
    /// <param name="settingsFilePath">Full path to a JSON file with optional <c>Squirix.MemoryPressure</c> section.</param>
    /// <param name="baseline">Baseline options when the section is absent.</param>
    /// <param name="merged">The merged result.</param>
    /// <returns><see langword="true" /> when the file exists and defines a <c>MemoryPressure</c> object.</returns>
    internal static bool TryMergeMemoryPressureFromSettingsFilePath(string settingsFilePath, MemoryPressureOptions baseline, out MemoryPressureOptions merged)
    {
        merged = baseline;
        if (!File.Exists(settingsFilePath))
            return false;

        using var fs = File.OpenRead(settingsFilePath);
        using var doc = JsonDocument.Parse(fs, new JsonDocumentOptions { AllowTrailingCommas = true, CommentHandling = JsonCommentHandling.Skip });

        var root = doc.RootElement;
        if (root.TryGetProperty("Squirix", out var squirix))
            root = squirix;

        if (!root.TryGetProperty("MemoryPressure", out var memoryPressure))
            return false;

        var section = SerializationProvider.Instance.Deserialize<MemoryPressureSettings>(memoryPressure.GetRawText());
        merged = section is null ? baseline : section.MergeInto(baseline);
        return true;
    }

    /// <summary>
    /// Validates optional <c>MemoryPressure</c> and <c>PrometheusMetrics</c> sections when present.
    /// </summary>
    /// <param name="settingsFilePath">Settings JSON path.</param>
    /// <param name="failures">Collected validation failures.</param>
    internal static void ValidateOptionalSections(string settingsFilePath, List<string> failures)
    {
        if (TryMergeMemoryPressureFromSettingsFilePath(settingsFilePath, new MemoryPressureOptions(), out var memoryPressure))
        {
            try
            {
                memoryPressure.Validate();
            }
            catch (InvalidOperationException ex)
            {
                failures.Add(ex.Message);
            }
        }

        if (!TryMergePrometheusMetricsFromSettingsFilePath(settingsFilePath, new PrometheusMetricsEndpointOptions(), out var prometheus))
            return;
        var validator = new SquirixOptionsValidators.PrometheusMetricsEndpointOptionsValidator();
        var result = validator.Validate(Options.DefaultName, prometheus);
        if (result.Failed)
            failures.AddRange(result.Failures);
    }

    private static string? ResolveSettingsPath() => SquirixServerConfiguration.ResolveSettingsPath();

    /// <summary>
    /// Merges <c>PrometheusMetrics</c> from a specific settings file path.
    /// </summary>
    /// <param name="settingsFilePath">Full path to a JSON file with optional <c>Squirix.PrometheusMetrics</c> section.</param>
    /// <param name="baseline">Baseline options when the section is absent.</param>
    /// <param name="merged">The merged result.</param>
    /// <returns><see langword="true" /> when the file exists and defines a <c>PrometheusMetrics</c> object.</returns>
    private static bool TryMergePrometheusMetricsFromSettingsFilePath(
        string settingsFilePath,
        PrometheusMetricsEndpointOptions baseline,
        out PrometheusMetricsEndpointOptions merged)
    {
        merged = baseline;
        if (!File.Exists(settingsFilePath))
            return false;

        using var fs = File.OpenRead(settingsFilePath);
        using var doc = JsonDocument.Parse(fs, new JsonDocumentOptions { AllowTrailingCommas = true, CommentHandling = JsonCommentHandling.Skip });

        var root = doc.RootElement;
        if (root.TryGetProperty("Squirix", out var squirix))
            root = squirix;

        if (!root.TryGetProperty("PrometheusMetrics", out var prometheusMetrics))
            return false;

        var section = SerializationProvider.Instance.Deserialize<PrometheusMetricsSettings>(prometheusMetrics.GetRawText());
        merged = section is null ? baseline : section.MergeInto(baseline);
        return true;
    }
}
