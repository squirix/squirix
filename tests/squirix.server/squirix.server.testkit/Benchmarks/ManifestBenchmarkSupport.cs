using System;

namespace Squirix.Server.TestKit.Benchmarks;

/// <summary>Scaling helpers for manifest publish and segment-roll benchmarks.</summary>
public static class ManifestBenchmarkSupport
{
    private static bool IsQuickMode => string.Equals(Environment.GetEnvironmentVariable("SQUIRIX_BENCH_QUICK"), "1", StringComparison.Ordinal);

    /// <summary>Returns publish operations per invoke (default 2000; quick 500).</summary>
    /// <param name="defaultCount">Operations per invoke when quick mode is disabled.</param>
    /// <returns>The resolved operation count.</returns>
    public static int ResolvePublishOperationsPerInvoke(int defaultCount = 2_000) => IsQuickMode ? Math.Max(defaultCount / 4, 500) : defaultCount;

    /// <summary>Returns manifest/snapshot retention count for benchmark hosts.</summary>
    /// <param name="defaultCount">Retention count when quick mode is disabled.</param>
    /// <returns>The resolved retention count.</returns>
    public static int ResolveRetentionCount(int defaultCount = 100_000) => IsQuickMode ? Math.Max(defaultCount / 10, 10_000) : defaultCount;

    /// <summary>Returns segment rolls per invoke (default 4; quick 2).</summary>
    /// <param name="defaultCount">Rolls per invoke when quick mode is disabled.</param>
    /// <returns>The resolved roll count.</returns>
    public static int ResolveRollsPerInvoke(int defaultCount = 4) => IsQuickMode ? Math.Max(defaultCount / 2, 2) : defaultCount;
}
