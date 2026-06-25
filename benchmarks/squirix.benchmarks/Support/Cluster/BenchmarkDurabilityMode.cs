namespace Squirix.Benchmarks.Support.Cluster;

/// <summary>Durability mode for client SDK benchmarks.</summary>
public enum BenchmarkDurabilityMode
{
    /// <summary>In-memory cache without journal/snapshot persistence.</summary>
    Ephemeral = 0,

    /// <summary>journal/snapshot persistence enabled.</summary>
    Persistence = 1,
}
