using System;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Exporters.Json;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;

namespace Squirix.E2EBenchmarks;

/// <summary>
/// BenchmarkDotNet configuration for end-to-end benchmarks.
/// </summary>
public static class SquirixE2EBenchmarkConfig
{
    /// <summary>
    /// Creates the common end-to-end benchmark configuration.
    /// </summary>
    /// <returns>The configured BenchmarkDotNet <see cref="IConfig" /> instance.</returns>
    public static IConfig Create() => DefaultConfig.Instance
        .AddJob(CreateJob())
        .AddDiagnoser(MemoryDiagnoser.Default)
        .AddExporter(JsonExporter.Full)
        .WithOptions(ConfigOptions.DisableOptimizationsValidator)
        .WithOptions(ConfigOptions.JoinSummary)
        .WithOptions(ConfigOptions.StopOnFirstError)
        .AddValidator(JitOptimizationsValidator.DontFailOnError);

    private static Job CreateJob() => string.Equals(Environment.GetEnvironmentVariable("SQUIRIX_E2E_BENCHMARK_LONG"), "1", StringComparison.Ordinal)
        ? Job.Default.WithId("Long")
        : Job.ShortRun.WithId("Short");
}
