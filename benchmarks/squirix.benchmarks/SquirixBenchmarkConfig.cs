using System;
using System.Diagnostics.CodeAnalysis;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Exporters.Csv;
using BenchmarkDotNet.Exporters.Json;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Order;

namespace Squirix.Benchmarks;

/// <summary>
/// Common BenchmarkDotNet configuration for CI runs. Ensures Markdown/JSON/CSV exporters are enabled
/// and provides a stable summary format. Artifacts path can be set by passing --artifacts in args (handled by BDN)
/// or via the BDN_ARTIFACTS environment variable.
/// </summary>
[SuppressMessage(
    "Maintainability",
    "CA1515:Consider making public types internal",
    Justification = "Shared benchmark configuration is consumed by the public benchmark entry point.")]
public static class SquirixBenchmarkConfig
{
    /// <summary>
    /// Creates a common BenchmarkDotNet configuration used by CI and local runs.
    /// Ensures stable ordering, memory diagnoser, and exporters for Markdown/HTML/JSON/CSV.
    /// Honors the BDN_ARTIFACTS environment variable for the artifacts' directory.
    /// </summary>
    /// <returns>The configured BenchmarkDotNet <see cref="IConfig" /> instance.</returns>
    public static IConfig Create()
    {
        var builder = ManualConfig.CreateEmpty().WithOptions(ConfigOptions.DisableOptimizationsValidator) // allow running in CI images
                                  .AddLogger(ConsoleLogger.Default)
                                  .AddColumn(
                                       TargetMethodColumn.Method,
                                       StatisticColumn.Mean,
                                       StatisticColumn.StdDev,
                                       StatisticColumn.P95,
                                       StatisticColumn.Min,
                                       StatisticColumn.Max,
                                       StatisticColumn.OperationsPerSecond).AddDiagnoser(MemoryDiagnoser.Default)
                                  .WithOrderer(new DefaultOrderer(SummaryOrderPolicy.SlowestToFastest)).AddExporter(MarkdownExporter.GitHub).AddExporter(HtmlExporter.Default)
                                  .AddExporter(JsonExporter.FullCompressed).AddExporter(CsvExporter.Default);

        // Respect BDN_ARTIFACTS env var if set (GitLab job sets --artifacts, which BDN also honors).
        var envArtifacts = Environment.GetEnvironmentVariable("BDN_ARTIFACTS");
        if (!string.IsNullOrWhiteSpace(envArtifacts))
            builder.ArtifactsPath = envArtifacts;

        return builder;
    }
}
