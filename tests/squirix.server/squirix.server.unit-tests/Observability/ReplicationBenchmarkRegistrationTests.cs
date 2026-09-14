using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.TestKit.Benchmarks;
using Squirix.Server.UnitTests.Architecture;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>Verifies required replication benchmarks are registered with per-phase evidence schemas.</summary>
public sealed class ReplicationBenchmarkRegistrationTests : ServerUnitTestBase
{
    /// <summary>All eight required benchmarks are registered and discoverable from source.</summary>
    [Fact]
    public void RequiredBenchmarksAreDiscoverable()
    {
        var required = ReplicationBenchmarkCatalog.RequiredBenchmarks;
        Assert.Equal(8, required.Count);
        Assert.All(required, static name => Assert.True(ReplicationBenchmarkCatalog.IsMappedToPhase(name)));

        var root = RepositoryPaths.FindRepositoryRoot();
        for (var i = 0; i < required.Count; i++)
            Assert.True(File.Exists(BenchmarkSourcePath(root, required[i])), $"Benchmark source is missing for '{required[i]}'.");
    }

    /// <summary>RF-parameterized benchmarks cover replica factors one, two, three, and five.</summary>
    [Fact]
    public async Task RfBenchmarksCoverOneTwoThreeAndFive()
    {
        int[] expected = [1, 2, 3, 5];
        for (var i = 0; i < expected.Length; i++)
            Assert.True(ReplicationBenchmarkCatalog.CoversReplicaFactor(expected[i]));

        var root = RepositoryPaths.FindRepositoryRoot();
        var placementPath = Path.Join(root, "benchmarks", "squirix.server.benchmarks", "ReplicaPlacementBenchmarks.cs");
        var text = await File.ReadAllTextAsync(placementPath, DefaultCancellationToken);
        Assert.Contains("GetReplicaGroupRfOne", text, StringComparison.Ordinal);
        Assert.Contains("GetReplicaGroupRfTwo", text, StringComparison.Ordinal);
        Assert.Contains("GetReplicaGroupRfThree", text, StringComparison.Ordinal);
        Assert.Contains("GetReplicaGroupRfFive", text, StringComparison.Ordinal);
    }

    /// <summary>Every benchmark phase has a stored machine-fingerprint evidence schema.</summary>
    [Fact]
    public void EveryPhaseHasStoredEvidenceSchema()
    {
        Assert.True(ReplicationBenchmarkCatalog.EveryPhaseHasStoredEvidenceSchema());

        var phases = ReplicationBenchmarkCatalog.Phases;
        Assert.Equal(8, phases.Count);
        for (var i = 0; i < phases.Count; i++)
        {
            var schema = ReplicationBenchmarkCatalog.GetEvidenceSchemaForPhase(phases[i]);
            Assert.Equal(PerformanceEvidenceGate.EvidenceSchema, schema);
            var benchmark = ReplicationBenchmarkCatalog.GetBenchmarkForPhase(phases[i]);
            Assert.True(ReplicationBenchmarkCatalog.IsMappedToPhase(benchmark));
        }
    }

    private static string BenchmarkSourcePath(string root, string benchmarkFullName)
    {
        return benchmarkFullName switch
        {
            "Squirix.Server.Benchmarks.ReplicaPlacementBenchmarks" => Path.Join(root, "benchmarks", "squirix.server.benchmarks", "ReplicaPlacementBenchmarks.cs"),
            "Squirix.Server.Benchmarks.FollowerAppendBenchmarks" => Path.Join(root, "benchmarks", "squirix.server.benchmarks", "FollowerAppendBenchmarks.cs"),
            "Squirix.Server.Benchmarks.ReplicaSnapshotBenchmarks" => Path.Join(root, "benchmarks", "squirix.server.benchmarks", "ReplicaSnapshotBenchmarks.cs"),
            "Squirix.Server.Benchmarks.ReplicaRepairBenchmarks" => Path.Join(root, "benchmarks", "squirix.server.benchmarks", "ReplicaRepairBenchmarks.cs"),
            "Squirix.E2EBenchmarks.Cache.ReplicaCommitBenchmarks" => Path.Join(root, "benchmarks", "squirix.e2e.benchmarks", "Cache", "ReplicaCommitBenchmarks.cs"),
            "Squirix.E2EBenchmarks.Cache.FailoverBenchmarks" => Path.Join(root, "benchmarks", "squirix.e2e.benchmarks", "Cache", "FailoverBenchmarks.cs"),
            "Squirix.E2EBenchmarks.Cache.LeaderAuthorityBenchmarks" => Path.Join(root, "benchmarks", "squirix.e2e.benchmarks", "Cache", "LeaderAuthorityBenchmarks.cs"),
            "Squirix.E2EBenchmarks.Cache.PublicSdkOperationsBenchmarks" => Path.Join(root, "benchmarks", "squirix.e2e.benchmarks", "Cache", "PublicSdkOperationsBenchmarks.cs"),
            _ => throw new ArgumentOutOfRangeException(nameof(benchmarkFullName), benchmarkFullName, "Unknown replication benchmark."),
        };
    }
}
