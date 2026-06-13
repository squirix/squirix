using System;
using System.Diagnostics.CodeAnalysis;
using BenchmarkDotNet.Running;
using Squirix.Benchmarks.Infrastructure;

namespace Squirix.Benchmarks;

/// <summary>
/// Entry point for running the BenchmarkDotNet benchmark suite in this assembly.
/// </summary>
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "BenchmarkDotNet entry point must remain public.")]
public static class Program
{
    /// <summary>
    /// Discovers and executes benchmarks in the current assembly.
    /// </summary>
    /// <param name="args">
    /// Command-line arguments passed through to BenchmarkDotNet (e.g., <c>--filter</c>).
    /// </param>
    public static void Main(string[] args)
    {
        ArgumentNullException.ThrowIfNull(args);
        BenchmarkRuntime.EnsureInitialized();

        if (args.Length == 0)
            args = ["--filter", "*"];

        var artifacts = Environment.GetEnvironmentVariable("BDN_ARTIFACTS");
        if (string.IsNullOrWhiteSpace(artifacts))
        {
            for (var i = 0; i < args.Length - 1; i++)
            {
                if (!string.Equals(args[i], "--artifacts", StringComparison.OrdinalIgnoreCase))
                    continue;

                artifacts = args[i + 1];
                break;
            }
        }

        Console.WriteLine(
            !string.IsNullOrWhiteSpace(artifacts)
                ? $"[CI] Benchmark artifacts saved to: {artifacts}"
                : "[CI] Benchmark artifacts saved to BenchmarkDotNet.Artifacts in the working directory.");

        _ = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, SquirixBenchmarkConfig.Create());
    }
}
