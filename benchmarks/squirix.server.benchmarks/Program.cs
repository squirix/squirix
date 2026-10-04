using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Running;

namespace Squirix.Server.Benchmarks;

internal static class Program
{
    /// <summary>Runs a BenchmarkDotNet switcher, or a measurement runner when the first argument names one (journal-flush-tails or journal-stall).</summary>
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

        _ = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}
