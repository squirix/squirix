using System;
using BenchmarkDotNet.Running;

namespace Squirix.Server.Benchmarks;

/// <summary>Runs only BenchmarkDotNet for comparisons against pinned sources.</summary>
internal static class Program
{
    /// <summary>Discovers benchmarks and forwards command-line arguments to BenchmarkDotNet.</summary>
    /// <param name="args">BenchmarkDotNet command-line arguments.</param>
    internal static void Main(string[] args)
    {
        ArgumentNullException.ThrowIfNull(args);
        _ = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}
