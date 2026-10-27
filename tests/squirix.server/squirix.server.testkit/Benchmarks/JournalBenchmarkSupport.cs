using System;

namespace Squirix.Server.TestKit.Benchmarks;

/// <summary>Shared helpers for journal benchmark quick-mode scaling.</summary>
public static class JournalBenchmarkSupport
{
    private static bool IsQuickMode => string.Equals(Environment.GetEnvironmentVariable("SQUIRIX_BENCH_QUICK"), "1", StringComparison.Ordinal);

    /// <summary>Returns group-commit operations per writer for quick local runs.</summary>
    /// <param name="defaultOperationsPerWriter">Default operations per writer when quick mode is disabled.</param>
    /// <returns>The resolved operations per writer.</returns>
    public static int ResolveGroupCommitOperationsPerWriter(int defaultOperationsPerWriter) =>
        IsQuickMode ? Math.Max(defaultOperationsPerWriter / 10, 100) : defaultOperationsPerWriter;
}
