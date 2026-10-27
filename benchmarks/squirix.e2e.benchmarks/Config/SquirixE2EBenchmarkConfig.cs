using System;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Exporters.Json;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Toolchains.InProcess.Emit;
using BenchmarkDotNet.Validators;

namespace Squirix.E2EBenchmarks.Config;

/// <summary>BenchmarkDotNet configuration for end-to-end benchmarks.</summary>
public static class SquirixE2EBenchmarkConfig
{
    /// <summary>Creates the common end-to-end benchmark configuration.</summary>
    /// <returns>The configured BenchmarkDotNet <see cref="IConfig" /> instance.</returns>
    public static IConfig Create() => DefaultConfig.Instance.AddJob(CreateJob()).AddDiagnoser(MemoryDiagnoser.Default).AddExporter(JsonExporter.Full)
                                                   .WithOptions(ConfigOptions.DisableOptimizationsValidator).WithOptions(ConfigOptions.JoinSummary)
                                                   .WithOptions(ConfigOptions.StopOnFirstError).AddValidator(JitOptimizationsValidator.DontFailOnError);

    private static Job CreateJob()
    {
        var job = string.Equals(Environment.GetEnvironmentVariable("SQUIRIX_E2E_BENCHMARK_LONG"), "1", StringComparison.Ordinal) ? Job.Default : Job.ShortRun;
        job = job.DontEnforcePowerPlan().WithId(string.Equals(Environment.GetEnvironmentVariable("SQUIRIX_E2E_BENCHMARK_LONG"), "1", StringComparison.Ordinal) ? "Long" : "Short");

        // In process, a run neither builds a project nor starts a process per benchmark; allocations are measured the same way.
        return string.Equals(Environment.GetEnvironmentVariable("SQUIRIX_E2E_BENCHMARK_IN_PROCESS"), "1", StringComparison.Ordinal)
            ? job.WithToolchain(InProcessEmitToolchain.Instance)
            : job;
    }
}
