using System;

namespace Squirix.Server.TestKit.Benchmarks;

/// <summary>Maps release evidence phases to their replication benchmarks.</summary>
public static class ReplicationBenchmarkCatalog
{
    /// <summary>Gets the benchmark full type name responsible for the specified phase.</summary>
    /// <param name="phase">Evidence phase name.</param>
    /// <returns>Benchmark full type name.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="phase" /> is empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="phase" /> is unknown.</exception>
    public static string GetBenchmarkForPhase(string phase)
    {
        ArgumentException.ThrowIfNullOrEmpty(phase);
        return phase switch
        {
            "placement" => "Squirix.Server.Benchmarks.ReplicaPlacementBenchmarks",
            "follower-append" => "Squirix.Server.Benchmarks.FollowerAppendBenchmarks",
            "snapshot" => "Squirix.Server.Benchmarks.ReplicaSnapshotBenchmarks",
            "commit" => "Squirix.E2EBenchmarks.Cache.ReplicaCommitBenchmarks",
            "failover" => "Squirix.E2EBenchmarks.Cache.FailoverBenchmarks",
            "authority" => "Squirix.E2EBenchmarks.Cache.LeaderAuthorityBenchmarks",
            "sdk-operations" => "Squirix.E2EBenchmarks.Cache.PublicSdkOperationsBenchmarks",
            _ => throw new ArgumentOutOfRangeException(nameof(phase), phase, "Unknown performance evidence phase."),
        };
    }
}
