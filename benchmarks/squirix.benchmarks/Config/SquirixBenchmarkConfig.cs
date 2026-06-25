using System;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Validators;

namespace Squirix.Benchmarks.Config;

/// <summary>
/// Common BenchmarkDotNet configuration for CI runs. Builds on <see cref="DefaultConfig" /> so exporters,
/// memory columns, and diagnosers are not registered twice. Artifacts path can be set via <c>BDN_ARTIFACTS</c>
/// or <c>--artifacts</c> on the command line.
/// </summary>
public static class SquirixBenchmarkConfig
{
    /// <summary>Creates the shared internal-benchmark configuration.</summary>
    /// <returns>The configured BenchmarkDotNet <see cref="IConfig" /> instance.</returns>
    public static IConfig Create()
    {
        var config = DefaultConfig.Instance.WithOptions(ConfigOptions.DisableOptimizationsValidator).WithOptions(ConfigOptions.JoinSummary)
                                  .AddValidator(JitOptimizationsValidator.DontFailOnError);

        var envArtifacts = Environment.GetEnvironmentVariable("BDN_ARTIFACTS");
        if (!string.IsNullOrWhiteSpace(envArtifacts))
            config = config.WithArtifactsPath(envArtifacts);

        return config;
    }
}
