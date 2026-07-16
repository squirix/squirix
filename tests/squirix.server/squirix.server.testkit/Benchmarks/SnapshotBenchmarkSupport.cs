using System;

namespace Squirix.Server.TestKit.Benchmarks;

/// <summary>Scaling helpers for snapshot write/read benchmarks.</summary>
public static class SnapshotBenchmarkSupport
{
    private static bool IsQuickMode => string.Equals(Environment.GetEnvironmentVariable("SQUIRIX_BENCH_QUICK"), "1", StringComparison.Ordinal);

    /// <summary>Returns snapshot entry count (default 10000; quick 1000).</summary>
    /// <param name="defaultCount">Entry count when quick mode is disabled.</param>
    /// <returns>The resolved entry count.</returns>
    public static int ResolveEntryCount(int defaultCount = 10_000) => IsQuickMode ? Math.Max(defaultCount / 10, 1_000) : defaultCount;

    /// <summary>Returns snapshot write operations per invoke (default 4; quick 2).</summary>
    /// <param name="defaultCount">Operations per invoke when quick mode is disabled.</param>
    /// <returns>The resolved operation count.</returns>
    public static int ResolveOperationsPerInvoke(int defaultCount = 4) => IsQuickMode ? Math.Max(defaultCount / 2, 2) : defaultCount;
}
