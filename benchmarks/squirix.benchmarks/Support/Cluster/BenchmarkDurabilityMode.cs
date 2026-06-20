using System.Diagnostics.CodeAnalysis;

namespace Squirix.Benchmarks.Support.Cluster;

/// <summary>Durability mode for client SDK benchmarks.</summary>
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "Benchmark parameter enum is consumed by public benchmark classes.")]
public enum BenchmarkDurabilityMode
{
    /// <summary>In-memory cache without journal/snapshot persistence.</summary>
    Ephemeral,

    /// <summary>journal/snapshot persistence enabled.</summary>
    Persistence,
}
