using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
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
        await using var registry = CreateRegistry(dir, CreateTopology(3));
        await registry.OpenAsync(cancellationToken);
        var eligibility = registry.EligibilityFor("node-a");
        eligibility.Quarantine(1);
        eligibility.Quarantine(2);
        using var scope = new CheckScope(new ReplicaGroupStatusSource(registry, CreateTopology(3), new MtlsOptions(), "node-a"));

        var result = await scope.Check.CheckHealthAsync(new HealthCheckContext(), cancellationToken);

        _ = await Assert.That(result.Status).IsEqualTo(HealthStatus.Unhealthy);
        _ = await Assert.That(result.Description).Contains("minority", StringComparison.Ordinal);
    }

    /// <summary>Verifies the status source reports the retained size of each served group log, and readiness stays healthy.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StatusCarriesLogRetention(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-readiness-retention");
        await using var registry = CreateRegistry(dir, CreateTopology(3));
        await registry.OpenAsync(cancellationToken);
        _ = registry.TryGetLog("node-a", out var log);
        var entry = new FollowerLogEntry(1, 1, ReadOnlyMemory<byte>.Of(1, 2, 3));
        _ = await log!.AppendAsync(new FollowerLogAppendRequest("node-a", 1, 0, 0, 0, ReadOnlyMemory<FollowerLogEntry>.Of(entry)), cancellationToken);
        var source = new ReplicaGroupStatusSource(registry, CreateTopology(3), new MtlsOptions(), "node-a");
        using var scope = new CheckScope(source);

        var snapshots = await source.GetSnapshotsAsync(cancellationToken);
        var result = await scope.Check.CheckHealthAsync(new HealthCheckContext(), cancellationToken);

        var retention = await log.GetRetentionAsync(cancellationToken);
        _ = await Assert.That(snapshots).HasSingleItem();
        _ = await Assert.That((snapshots[0].LogBytes, snapshots[0].RetainedEntries, snapshots[0].SnapshotIndex)).IsEqualTo((retention.LogBytes, 1, 0UL));
        _ = await Assert.That(snapshots[0].LogBytes > 0).IsTrue();
        _ = await Assert.That(result.Status).IsEqualTo(HealthStatus.Healthy);
    }

    /// <summary>
    /// Verifies the owner leads its group statically until an election driver runs; then only a leader with authority is the leader, and
    /// the status carries the highest term the election saw and the role.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ElectionStateDecidesLeadership(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-readiness-election");
        await using var registry = CreateRegistry(dir, CreateTopology(3));
        await registry.OpenAsync(cancellationToken);
        var source = new ReplicaGroupStatusSource(registry, CreateTopology(3), new MtlsOptions(), "node-a");
        var owned = (await source.GetSnapshotsAsync(cancellationToken))[0];
        var state = registry.StateFor("node-a");
        state.SetElectionDriven(true);
        state.ObserveHigherTerm(4UL);
        var following = (await source.GetSnapshotsAsync(cancellationToken))[0];
        _ = state.BecomeLeader(4UL);
        var elected = (await source.GetSnapshotsAsync(cancellationToken))[0];
        _ = state.GrantAuthority(4UL);
        var leading = (await source.GetSnapshotsAsync(cancellationToken))[0];

        _ = await Assert.That((owned.IsLeader, owned.ObservedTerm, owned.Role)).IsEqualTo((true, 0UL, ReplicaElectionRole.AuthorizedLeader));
        _ = await Assert.That((following.IsLeader, following.HasMajorityContact, following.ObservedTerm, following.Role))
                        .IsEqualTo((false, false, 4UL, ReplicaElectionRole.Follower));
        _ = await Assert.That((elected.IsLeader, elected.Role)).IsEqualTo((false, ReplicaElectionRole.Leader));
        _ = await Assert.That((leading.IsLeader, leading.HasMajorityContact, leading.Role)).IsEqualTo((true, true, ReplicaElectionRole.AuthorizedLeader));
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
        await using var registry = new ReplicaGroupRegistry("test-root", ["node-a"], 1, ReadOnlyMemory<byte>.Of(9), 1, NullLoggerFactory.Instance);
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

    /// <summary>Creates a registry stamped with the fingerprint and generation of <paramref name="topology" />, as node startup does.</summary>
    /// <param name="dir">The node data directory.</param>
    /// <param name="topology">The configured topology.</param>
    /// <returns>The registry, not yet opened.</returns>
    private static ReplicaGroupRegistry CreateRegistry(string dir, TopologyOptions topology)
    {
        byte[] fingerprint = [.. TopologyFingerprint.CreateFromTopology(topology, new MtlsOptions()).Bytes];
        return new ReplicaGroupRegistry(dir, ["node-a"], topology.ReplicaCount, fingerprint, topology.ConfigurationGeneration, NullLoggerFactory.Instance);
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
