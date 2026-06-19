using System;
using BenchmarkDotNet.Running;

namespace Squirix.Wal.Benchmarks;

internal static class Program
{
    public static void Main(string[] args)
    {
        ArgumentNullException.ThrowIfNull(args);
        _ = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}
