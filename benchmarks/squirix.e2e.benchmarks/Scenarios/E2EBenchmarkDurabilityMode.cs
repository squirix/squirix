using System.Diagnostics.CodeAnalysis;

namespace Squirix.E2EBenchmarks.Scenarios;

/// <summary>
/// Durability mode exposed by the E2E benchmark harness.
/// </summary>
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "Benchmark parameter enum is part of public benchmark parameterization.")]
public enum E2EBenchmarkDurabilityMode
{
    /// <summary>
    /// In-memory cache without WAL/snapshot persistence.
    /// </summary>
    Ephemeral,

    /// <summary>
    /// WAL/snapshot persistence enabled.
    /// </summary>
    Persistence,
}
