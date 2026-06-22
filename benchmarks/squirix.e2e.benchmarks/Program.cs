using System.Diagnostics.CodeAnalysis;
using BenchmarkDotNet.Running;
using Squirix.E2EBenchmarks.Config;

namespace Squirix.E2EBenchmarks;

/// <summary>Entry point for running end-to-end BenchmarkDotNet suites.</summary>
public static class Program
{
    /// <summary>Discovers and executes end-to-end benchmarks in the current assembly.</summary>
    /// <param name="args">Command-line arguments passed through to BenchmarkDotNet.</param>
    [SuppressMessage(
        "Security",
        "CA1062:Validate arguments of public methods",
        Justification = "Benchmark entry point is invoked by the runtime and args is not expected to be null.")]
    public static void Main(string[] args) => BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, SquirixE2EBenchmarkConfig.Create());
}
