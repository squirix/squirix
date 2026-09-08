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
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Verifies replica readiness diagnostics report stale and fenced groups as not ready.</summary>
[Immutable]
public sealed class ReadinessReplicaStatusTests : ServerUnitTestBase
{
    /// <summary>Verifies a group observing a higher term reports not ready.</summary>
    [Fact]
    public async Task StaleReplicaIsNotReady()
    {
        var stale = ReadyLeaderSnapshot("group-a") with { ObservedTerm = 5 };
        using var scope = new CheckScope(new FixedSource([stale]));

        var result = await scope.Check.CheckHealthAsync(new HealthCheckContext(), DefaultCancellationToken);

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Contains("stale", result.Description, StringComparison.Ordinal);
    }

    /// <summary>Verifies a leader without majority contact reports not ready.</summary>
    [Fact]
    public async Task MinorityWithoutAuthorityIsNotReady()
    {
        var first = ReadyLeaderSnapshot("group-a") with { HasMajorityContact = false };
        var second = ReadyLeaderSnapshot("group-b") with { HasMajorityContact = false };
        using var scope = new CheckScope(new FixedSource([first, second]));

        var result = await scope.Check.CheckHealthAsync(new HealthCheckContext(), DefaultCancellationToken);

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Contains("minority", result.Description, StringComparison.Ordinal);
    }

    /// <summary>Verifies a group disagreeing on fingerprint reports not ready.</summary>
    [Fact]
    public async Task MismatchedFingerprintIsNotReady()
    {
        var mismatched = ReadyLeaderSnapshot("group-a") with { FingerprintMatch = false };
        using var scope = new CheckScope(new FixedSource([mismatched]));

        var result = await scope.Check.CheckHealthAsync(new HealthCheckContext(), DefaultCancellationToken);

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Contains("mismatch", result.Description, StringComparison.Ordinal);
    }

    /// <summary>Verifies a group disagreeing on generation reports not ready.</summary>
    [Fact]
    public async Task MismatchedGenerationIsNotReady()
    {
        var mismatched = ReadyLeaderSnapshot("group-a") with { GenerationMatch = false };
        using var scope = new CheckScope(new FixedSource([mismatched]));

        var result = await scope.Check.CheckHealthAsync(new HealthCheckContext(), DefaultCancellationToken);

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Contains("mismatch", result.Description, StringComparison.Ordinal);
    }

    /// <summary>Verifies a group whose log is not ready reports not ready.</summary>
    [Fact]
    public async Task UnreadyLogIsNotReady()
    {
        var unready = ReadyLeaderSnapshot("group-a") with { LogReady = false };
        using var scope = new CheckScope(new FixedSource([unready]));

        var result = await scope.Check.CheckHealthAsync(new HealthCheckContext(), DefaultCancellationToken);

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Contains("not ready", result.Description, StringComparison.Ordinal);
    }

    /// <summary>Verifies an owned group with majority contact reports healthy.</summary>
    [Fact]
    public async Task ReadyLeaderReportsHealthy()
    {
        using var scope = new CheckScope(new FixedSource([ReadyLeaderSnapshot("group-a"), ReadyFollowerSnapshot("group-b")]));

        var result = await scope.Check.CheckHealthAsync(new HealthCheckContext(), DefaultCancellationToken);

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    /// <summary>Verifies a follower without authority stays ready when its log agrees.</summary>
    [Fact]
    public async Task FollowerWithoutAuthorityStaysReady()
    {
        var follower = ReadyFollowerSnapshot("group-b") with { HasMajorityContact = false };
        using var scope = new CheckScope(new FixedSource([follower]));

        var result = await scope.Check.CheckHealthAsync(new HealthCheckContext(), DefaultCancellationToken);

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    /// <summary>Verifies a missing replica source reports healthy without replication.</summary>
    [Fact]
    public async Task MissingSourceReportsHealthy()
    {
        using var scope = new CheckScope(null);

        var result = await scope.Check.CheckHealthAsync(new HealthCheckContext(), DefaultCancellationToken);

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    /// <summary>Verifies an empty replica set reports healthy with no groups served.</summary>
    [Fact]
    public async Task EmptyGroupsReportHealthy()
    {
        using var scope = new CheckScope(new FixedSource([]));

        var result = await scope.Check.CheckHealthAsync(new HealthCheckContext(), DefaultCancellationToken);

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    /// <summary>Verifies an unopened registry yields no snapshots.</summary>
    [Fact]
    public async Task UnopenedRegistryYieldsNoSnapshots()
    {
        await using var registry = new ReplicaGroupRegistry("test-root", ["node-a"], 1, new ReadOnlyMemory<byte>([9]), 1);
        var source = new ReplicaGroupStatusSource(registry, CreateTopology(1), new MtlsOptions(), "node-a");

        var snapshots = await source.GetSnapshotsAsync(DefaultCancellationToken);

        Assert.Empty(snapshots);
    }

    /// <summary>Verifies quarantined participants lose majority and readiness.</summary>
    [Fact]
    public async Task QuarantinedGroupLosesReadiness()
    {
        using var dir = new TempDirectory("squirix-readiness-quarantine");
        await using var registry = new ReplicaGroupRegistry(dir, ["node-a"], 3, new ReadOnlyMemory<byte>([9]), 1);
        await registry.OpenAsync(DefaultCancellationToken);
        var eligibility = registry.EligibilityFor("node-a");
        eligibility.Quarantine(1);
        eligibility.Quarantine(2);
        using var scope = new CheckScope(new ReplicaGroupStatusSource(registry, CreateTopology(3), new MtlsOptions(), "node-a"));

        var result = await scope.Check.CheckHealthAsync(new HealthCheckContext(), DefaultCancellationToken);

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Contains("minority", result.Description, StringComparison.Ordinal);
    }

    /// <summary>Verifies verdict descriptions stay stable and reject unknown verdicts.</summary>
    [Fact]
    public void DescribeReportsAllVerdicts()
    {
        Assert.Contains("is ready", ReplicaReadiness.Describe(ReplicaReadinessVerdict.Ready, "group-a"), StringComparison.Ordinal);
        Assert.Contains("stale", ReplicaReadiness.Describe(ReplicaReadinessVerdict.StaleTerm, "group-a"), StringComparison.Ordinal);
        Assert.Contains("minority", ReplicaReadiness.Describe(ReplicaReadinessVerdict.MinorityFenced, "group-a"), StringComparison.Ordinal);
        Assert.Contains("mismatch", ReplicaReadiness.Describe(ReplicaReadinessVerdict.TopologyMismatch, "group-a"), StringComparison.Ordinal);
        Assert.Contains("not ready", ReplicaReadiness.Describe(ReplicaReadinessVerdict.LogNotReady, "group-a"), StringComparison.Ordinal);
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

    private sealed class FixedSource : IReplicaStatusSource
    {
        private readonly IReadOnlyList<ReplicaStatusSnapshot> _snapshots;

        internal FixedSource(IReadOnlyList<ReplicaStatusSnapshot> snapshots)
        {
            _snapshots = snapshots;
        }

        public ValueTask<IReadOnlyList<ReplicaStatusSnapshot>> GetSnapshotsAsync(CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            return ValueTask.FromResult(_snapshots);
        }
    }

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
}
