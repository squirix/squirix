using System.Threading.Tasks;
using BenchmarkDotNet.Running;
using Squirix.E2EBenchmarks.Cache;
using Squirix.E2EBenchmarks.Config;

namespace Squirix.E2EBenchmarks;

/// <summary>Entry point for running end-to-end BenchmarkDotNet suites.</summary>
public static class Program
{
    /// <summary>Discovers and executes end-to-end benchmarks in the current assembly.</summary>
    /// <param name="args">Command-line arguments passed through to BenchmarkDotNet.</param>
    public static async Task Main(string[] args)
    {
        if (args is ["--check-idle-buffers"])
        {
            await IdleConnectionBuffers.CheckAsync().ConfigureAwait(false);
            return;
        }

        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, SquirixE2EBenchmarkConfig.Create());
    }
}
