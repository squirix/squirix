using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Verifies the status source reports the leader's view of the other copies of its group.</summary>
[Immutable]
public sealed class ReplicaGroupCopiesTests : ServerUnitTestBase
{
    /// <summary>Verifies the leader lists each other copy with its state and lag, and its quorum follows the copies that count.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeaderListsCopiesWithLag(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-copies-lag");
        await SeededLogs.SeedGroupsAsync(dir, ["node-a"], cancellationToken);
        await using var registry = CreateRegistry(dir, CreateTopology(3));
        await registry.OpenAsync(cancellationToken);
        _ = registry.TryGetLog("node-a", out var log);
        var entry = new FollowerLogEntry(1, 1, ReadOnlyMemory<byte>.Of(1, 2, 3));
        _ = await log!.AppendAsync(new FollowerLogAppendRequest("node-a", 1, 0, 0, 0, ReadOnlyMemory<FollowerLogEntry>.Of(entry)), cancellationToken);
        var source = new ReplicaGroupStatusSource(registry, CreateTopology(3), new MtlsOptions(), new ThreeNodeGroupLocator(), "node-a");
        var eligibility = registry.EligibilityFor("node-a");

        var behind = (await source.GetSnapshotsAsync(cancellationToken))[0];
        var caughtUp = new ReplicaProgress(2, 1, 1, 1, ReadOnlyMemory<byte>.Of(9), 1);
        _ = eligibility.TryMarkReady(1, in caughtUp);
        eligibility.Forget(2);
        var mixed = (await source.GetSnapshotsAsync(cancellationToken))[0];
        eligibility.Forget(1);
        var lost = (await source.GetSnapshotsAsync(cancellationToken))[0];

        _ = await Assert.That((behind.HasAuthority, behind.WriteQuorumAvailable)).IsEqualTo((true, true));
        await SequenceAssert.EqualAsync(
            [new ReplicaPeerStatus("node-b", ReplicaParticipantState.Ready, 1), new ReplicaPeerStatus("node-c", ReplicaParticipantState.Ready, 1)],
            behind.Peers);
        await SequenceAssert.EqualAsync(
            [new ReplicaPeerStatus("node-b", ReplicaParticipantState.Ready, 0), new ReplicaPeerStatus("node-c", ReplicaParticipantState.CatchingUp, 1)],
            mixed.Peers);
        _ = await Assert.That(mixed.WriteQuorumAvailable).IsTrue();
        _ = await Assert.That(lost.WriteQuorumAvailable).IsFalse();
    }

    /// <summary>Verifies a follower lists no copies, and a leader without authority lists them but does not report authority.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FollowerListsNoCopies(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-copies-follower");
        await using var registry = CreateRegistry(dir, CreateTopology(3));
        await registry.OpenAsync(cancellationToken);
        var source = new ReplicaGroupStatusSource(registry, CreateTopology(3, true), new MtlsOptions(), new ThreeNodeGroupLocator(), "node-a");
        var state = registry.StateFor("node-a");
        state.SetElectionDriven(true);
        state.ObserveHigherTerm(4UL);
        var following = (await source.GetSnapshotsAsync(cancellationToken))[0];
        _ = state.BecomeLeader(4UL);
        var elected = (await source.GetSnapshotsAsync(cancellationToken))[0];
        _ = state.GrantAuthority(4UL);
        var leading = (await source.GetSnapshotsAsync(cancellationToken))[0];

        _ = await Assert.That((following.Peers.Count, following.HasAuthority, following.WriteQuorumAvailable)).IsEqualTo((0, false, false));
        _ = await Assert.That((elected.Peers.Count, elected.HasAuthority)).IsEqualTo((2, false));
        _ = await Assert.That((leading.Peers.Count, leading.HasAuthority)).IsEqualTo((2, true));
    }

    private static ReplicaGroupRegistry CreateRegistry(string dir, TopologyOptions topology)
    {
        byte[] fingerprint = [.. TopologyFingerprint.CreateFromTopology(topology, new MtlsOptions()).Bytes];
        return new ReplicaGroupRegistry(
            dir,
            ["node-a"],
            topology.ReplicaCount,
            fingerprint,
            topology.ConfigurationGeneration,
            NullLoggerFactory.Instance,
            new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System })
        {
            ElectionClock = TimeProvider.System,
        };
    }

    private static TopologyOptions CreateTopology(int replicaCount, bool automaticFailover = false) => new([new ServerPeer { NodeId = "node-a", Uri = new Uri("https://localhost:6131") }])
    {
        ClusterId = "copies-c",
        NodeId = "node-a",
        Uri = new Uri("https://localhost:6131"),
        ReplicaCount = replicaCount,
        AutomaticFailoverEnabled = automaticFailover,
    };
}
