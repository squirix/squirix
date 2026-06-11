namespace Squirix.Benchmarks.Infrastructure;

/// <summary>
/// Durability mode for client SDK benchmarks.
/// </summary>
public enum BenchmarkDurabilityMode
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
