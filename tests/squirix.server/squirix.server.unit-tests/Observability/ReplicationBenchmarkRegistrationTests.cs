using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit.Benchmarks;
using Squirix.Server.UnitTests.Architecture;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>Verifies required replication benchmarks are registered with per-phase evidence schemas.</summary>
public sealed class ReplicationBenchmarkRegistrationTests : ServerUnitTestBase
{
    /// <summary>Every benchmark phase has a stored machine-fingerprint evidence schema.</summary>
    [Test]
    public async Task EveryPhaseHasStoredEvidenceSchema()
    {
        _ = await Assert.That(ReplicationBenchmarkCatalog.EveryPhaseHasStoredEvidenceSchema()).IsTrue();

        var phases = ReplicationBenchmarkCatalog.Phases;
        _ = await Assert.That(phases.Count).IsEqualTo(8);
        for (var i = 0; i < phases.Count; i++)
        {
            var schema = ReplicationBenchmarkCatalog.GetEvidenceSchemaForPhase(phases[i]);
            _ = await Assert.That(schema).IsEqualTo(PerformanceEvidenceGate.EvidenceSchema);
            var benchmark = ReplicationBenchmarkCatalog.GetBenchmarkForPhase(phases[i]);
            _ = await Assert.That(ReplicationBenchmarkCatalog.IsMappedToPhase(benchmark)).IsTrue();
        }
    }

    /// <summary>All eight required benchmarks are registered and discoverable from source.</summary>
    [Test]
    public async Task RequiredBenchmarksAreDiscoverable()
    {
        var required = ReplicationBenchmarkCatalog.RequiredBenchmarks;
        _ = await Assert.That(required.Count).IsEqualTo(8);
        _ = await Assert.That(required).All(static name => ReplicationBenchmarkCatalog.IsMappedToPhase(name));

        var root = RepositoryPaths.FindRepositoryRoot();
        for (var i = 0; i < required.Count; i++)
            _ = await Assert.That(File.Exists(BenchmarkSourcePath(root, required[i]))).IsTrue().Because($"Benchmark source is missing for '{required[i]}'.");
    }

    /// <summary>RF-parameterized benchmarks cover replica factors one, two, three, and five.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfBenchmarksCoverOneTwoThreeAndFive(CancellationToken cancellationToken)
    {
        int[] expected = [1, 2, 3, 5];
        for (var i = 0; i < expected.Length; i++)
            _ = await Assert.That(ReplicationBenchmarkCatalog.CoversReplicaFactor(expected[i])).IsTrue();

        var root = RepositoryPaths.FindRepositoryRoot();
        var placementPath = Path.Join(root, "benchmarks", "squirix.server.benchmarks", "ReplicaPlacementBenchmarks.cs");
        var text = await File.ReadAllTextAsync(placementPath, cancellationToken);
        _ = await Assert.That(text).Contains("GetReplicaGroupRfOne", StringComparison.Ordinal);
        _ = await Assert.That(text).Contains("GetReplicaGroupRfTwo", StringComparison.Ordinal);
        _ = await Assert.That(text).Contains("GetReplicaGroupRfThree", StringComparison.Ordinal);
        _ = await Assert.That(text).Contains("GetReplicaGroupRfFive", StringComparison.Ordinal);
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
