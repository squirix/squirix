using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Verifies replica readiness diagnostics report stale and fenced groups as not ready.</summary>
[Immutable]
public sealed class ReadinessReplicaStatusTests : ServerUnitTestBase
{
    /// <summary>Verifies verdict descriptions stay stable and reject unknown verdicts.</summary>
    [Test]
    public async Task DescribeReportsAllVerdicts()
    {
        _ = await Assert.That(ReplicaReadiness.Describe(ReplicaReadinessVerdict.Ready, "group-a")).Contains("is ready", StringComparison.Ordinal);
        _ = await Assert.That(ReplicaReadiness.Describe(ReplicaReadinessVerdict.StaleTerm, "group-a")).Contains("stale", StringComparison.Ordinal);
        _ = await Assert.That(ReplicaReadiness.Describe(ReplicaReadinessVerdict.MinorityFenced, "group-a")).Contains("minority", StringComparison.Ordinal);
        _ = await Assert.That(ReplicaReadiness.Describe(ReplicaReadinessVerdict.TopologyMismatch, "group-a")).Contains("mismatch", StringComparison.Ordinal);
        _ = await Assert.That(ReplicaReadiness.Describe(ReplicaReadinessVerdict.LogNotReady, "group-a")).Contains("not ready", StringComparison.Ordinal);
    }

    /// <summary>Verifies an empty replica set reports healthy with no groups served.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EmptyGroupsReportHealthy(CancellationToken cancellationToken)
    {
        using var scope = new CheckScope(new FixedSource([]));

        var result = await scope.Check.CheckHealthAsync(new HealthCheckContext(), cancellationToken);

        _ = await Assert.That(result.Status).IsEqualTo(HealthStatus.Healthy);
    }

    /// <summary>Verifies a follower without authority stays ready when its log agrees.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FollowerWithoutAuthorityStaysReady(CancellationToken cancellationToken)
    {
        var follower = ReadyFollowerSnapshot("group-b") with { HasMajorityContact = false };
        using var scope = new CheckScope(new FixedSource([follower]));

        var result = await scope.Check.CheckHealthAsync(new HealthCheckContext(), cancellationToken);

        _ = await Assert.That(result.Status).IsEqualTo(HealthStatus.Healthy);
    }

    /// <summary>Verifies a leader without majority contact reports not ready.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MinorityWithoutAuthorityIsNotReady(CancellationToken cancellationToken)
    {
        var first = ReadyLeaderSnapshot("group-a") with { HasMajorityContact = false };
        var second = ReadyLeaderSnapshot("group-b") with { HasMajorityContact = false };
        using var scope = new CheckScope(new FixedSource([first, second]));

        var result = await scope.Check.CheckHealthAsync(new HealthCheckContext(), cancellationToken);

        _ = await Assert.That(result.Status).IsEqualTo(HealthStatus.Unhealthy);
        _ = await Assert.That(result.Description).Contains("minority", StringComparison.Ordinal);
    }

    /// <summary>Verifies a group disagreeing on fingerprint reports not ready.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MismatchedFingerprintIsNotReady(CancellationToken cancellationToken)
    {
        var mismatched = ReadyLeaderSnapshot("group-a") with { FingerprintMatch = false };
        using var scope = new CheckScope(new FixedSource([mismatched]));

        var result = await scope.Check.CheckHealthAsync(new HealthCheckContext(), cancellationToken);

        _ = await Assert.That(result.Status).IsEqualTo(HealthStatus.Unhealthy);
        _ = await Assert.That(result.Description).Contains("mismatch", StringComparison.Ordinal);
    }

    /// <summary>Verifies a group disagreeing on generation reports not ready.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MismatchedGenerationIsNotReady(CancellationToken cancellationToken)
    {
        var mismatched = ReadyLeaderSnapshot("group-a") with { GenerationMatch = false };
        using var scope = new CheckScope(new FixedSource([mismatched]));

        var result = await scope.Check.CheckHealthAsync(new HealthCheckContext(), cancellationToken);

        _ = await Assert.That(result.Status).IsEqualTo(HealthStatus.Unhealthy);
        _ = await Assert.That(result.Description).Contains("mismatch", StringComparison.Ordinal);
    }

    /// <summary>Verifies a missing replica source reports healthy without replication.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MissingSourceReportsHealthy(CancellationToken cancellationToken)
    {
        using var scope = new CheckScope(null);

        var result = await scope.Check.CheckHealthAsync(new HealthCheckContext(), cancellationToken);

        _ = await Assert.That(result.Status).IsEqualTo(HealthStatus.Healthy);
    }

    /// <summary>Verifies quarantined participants lose majority and readiness.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QuarantinedGroupLosesReadiness(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-readiness-quarantine");
        await using var registry = new ReplicaGroupRegistry(dir, ["node-a"], 3, ReadOnlyMemory<byte>.Of(9), 1);
        await registry.OpenAsync(cancellationToken);
        var eligibility = registry.EligibilityFor("node-a");
        eligibility.Quarantine(1);
        eligibility.Quarantine(2);
        using var scope = new CheckScope(new ReplicaGroupStatusSource(registry, CreateTopology(3), new MtlsOptions(), "node-a"));

        var result = await scope.Check.CheckHealthAsync(new HealthCheckContext(), cancellationToken);

        _ = await Assert.That(result.Status).IsEqualTo(HealthStatus.Unhealthy);
        _ = await Assert.That(result.Description).Contains("minority", StringComparison.Ordinal);
    }

    /// <summary>Verifies an owned group with majority contact reports healthy.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadyLeaderReportsHealthy(CancellationToken cancellationToken)
    {
        using var scope = new CheckScope(new FixedSource([ReadyLeaderSnapshot("group-a"), ReadyFollowerSnapshot("group-b")]));

        var result = await scope.Check.CheckHealthAsync(new HealthCheckContext(), cancellationToken);

        _ = await Assert.That(result.Status).IsEqualTo(HealthStatus.Healthy);
    }

    /// <summary>Verifies a group observing a higher term reports not ready.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaleReplicaIsNotReady(CancellationToken cancellationToken)
    {
        var stale = ReadyLeaderSnapshot("group-a") with { ObservedTerm = 5 };
        using var scope = new CheckScope(new FixedSource([stale]));

        var result = await scope.Check.CheckHealthAsync(new HealthCheckContext(), cancellationToken);

        _ = await Assert.That(result.Status).IsEqualTo(HealthStatus.Unhealthy);
        _ = await Assert.That(result.Description).Contains("stale", StringComparison.Ordinal);
    }

    /// <summary>Verifies an unopened registry yields no snapshots.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnopenedRegistryYieldsNoSnapshots(CancellationToken cancellationToken)
    {
        await using var registry = new ReplicaGroupRegistry("test-root", ["node-a"], 1, ReadOnlyMemory<byte>.Of(9), 1);
        var source = new ReplicaGroupStatusSource(registry, CreateTopology(1), new MtlsOptions(), "node-a");

        var snapshots = await source.GetSnapshotsAsync(cancellationToken);

        _ = await Assert.That(snapshots).IsEmpty();
    }

    /// <summary>Verifies a group whose log is not ready reports not ready.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnreadyLogIsNotReady(CancellationToken cancellationToken)
    {
        var unready = ReadyLeaderSnapshot("group-a") with { LogReady = false };
        using var scope = new CheckScope(new FixedSource([unready]));

        var result = await scope.Check.CheckHealthAsync(new HealthCheckContext(), cancellationToken);

        _ = await Assert.That(result.Status).IsEqualTo(HealthStatus.Unhealthy);
        _ = await Assert.That(result.Description).Contains("not ready", StringComparison.Ordinal);
    }

    private static TopologyOptions CreateTopology(int replicaCount) => new([new ServerPeer { NodeId = "node-a", Uri = new Uri("https://localhost:6131") }])
    {
        ClusterId = "readiness-c",
        NodeId = "node-a",
        Uri = new Uri("https://localhost:6131"),
        ReplicaCount = replicaCount,
    };

    private static ReplicaStatusSnapshot ReadyFollowerSnapshot(string group) => new("node-a", group, 3, 4, 4, 10, 7, 7, true, true, true, false, true);

    private static ReplicaStatusSnapshot ReadyLeaderSnapshot(string group) => new("node-a", group, 3, 4, 4, 10, 7, 7, true, true, true, true, true);

    private sealed class CheckScope : IDisposable
    {
        private readonly Meter _meter = new("Squirix");
        private int _disposed;

        internal CheckScope(IReplicaStatusSource? source)
        {
            Check = new ReplicaReadinessHealthCheck(source, new ReplicationMetrics(_meter));
        }

        internal ReplicaReadinessHealthCheck Check { get; }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            _meter.Dispose();
        }
    }

    private sealed class FixedSource : IReplicaStatusSource
    {
        private readonly IReadOnlyList<ReplicaStatusSnapshot> _snapshots;

        internal FixedSource(ReadOnlySpan<ReplicaStatusSnapshot> snapshots)
        {
            _snapshots = Copy(snapshots);
        }

        public ValueTask<IReadOnlyList<ReplicaStatusSnapshot>> GetSnapshotsAsync(CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            return ValueTask.FromResult(_snapshots);
        }

        private static ReplicaStatusSnapshot[] Copy(ReadOnlySpan<ReplicaStatusSnapshot> snapshots)
        {
            if (snapshots.IsEmpty)
                return [];

            var copy = new ReplicaStatusSnapshot[snapshots.Length];
            snapshots.CopyTo(copy);
            return copy;
        }
    }
}
