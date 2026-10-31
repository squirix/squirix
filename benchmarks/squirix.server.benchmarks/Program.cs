using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.Emit;

namespace Squirix.Server.Benchmarks;

internal static class Program
{
    /// <summary>Runs a BenchmarkDotNet switcher, or a measurement runner when the first argument names one (journal-flush-tails, journal-stall or entry-memory).</summary>
    /// <param name="args">Command line arguments.</param>
    /// <returns>An asynchronous operation.</returns>
    internal static async Task Main(string[] args)
    {
        ArgumentNullException.ThrowIfNull(args);
        if (args.Length > 0 && string.Equals(args[0], "journal-flush-tails", StringComparison.Ordinal))
        {
            await JournalFlushTailMeasurement.RunAsync(args[1..]).ConfigureAwait(false);
            return;
        }

        if (args.Length > 0 && string.Equals(args[0], "journal-stall", StringComparison.Ordinal))
        {
            await JournalStallMeasurement.RunAsync(args[1..]).ConfigureAwait(false);
            return;
        }

        if (args.Length > 0 && string.Equals(args[0], "entry-memory", StringComparison.Ordinal))
        {
            await EntryMemoryMeasurement.RunAsync(args[1..]).ConfigureAwait(false);
            return;
        }

        // Mutate attributed jobs too, so smoke validation never adds a full measurement job.
        var config = string.Equals(Environment.GetEnvironmentVariable("SQUIRIX_BENCHMARK_SMOKE"), "1", StringComparison.Ordinal)
            ? ManualConfig.Create(DefaultConfig.Instance).AddJob(Job.Dry.WithToolchain(InProcessEmitToolchain.Instance).DontEnforcePowerPlan().AsMutator())
            : DefaultConfig.Instance;
        _ = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
    }
}
