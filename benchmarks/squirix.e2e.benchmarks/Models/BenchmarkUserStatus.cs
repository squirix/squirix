using System.Diagnostics.CodeAnalysis;

namespace Squirix.E2EBenchmarks.Models;

/// <summary>
/// User status used by custom benchmark records.
/// </summary>
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "Benchmark data enum is serialized in public benchmark payloads.")]
public enum BenchmarkUserStatus
{
    /// <summary>
    /// Active user profile.
    /// </summary>
    Active,

    /// <summary>
    /// Blocked user profile.
    /// </summary>
    Blocked,
}
