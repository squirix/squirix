using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.OwnedLogTestKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The gated compaction step of an RF=3 group owner past its retention limit, where a ready follower that stays behind is left behind.</summary>
public sealed class OwnedLogRetentionLimitTests : ServerUnitTestBase
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Past the retention limit a ready follower that stays behind the commit index is taken out of the write quorum and left behind, while
    /// the leader and the other follower keep a write majority; the compaction then goes on and the follower is queued for repair.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FollowerBehindPastLimitIsLeftBehind(CancellationToken cancellationToken)
    {
        var gateway = new ScriptedGateway();
        using var dir = new TempDirectory("squirix-owned-compaction-behind-limit");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);
        await WriteAsync(committer, 1, cancellationToken);
        gateway.Set("n3", FollowerMode.Silent);
        await WriteAsync(committer, 1, cancellationToken);
        _ = await Assert.That(registry.EligibilityFor("n1").StateFor(FollowerSlot)).IsEqualTo(ReplicaParticipantState.Ready);

        _ = await Assert.That(await committer.CompactOwnedLogAsync(PastLimit, Durable(), cancellationToken)).IsEqualTo(ReplicaLogCompactionOutcome.CompactedPastFollower);

        _ = await Assert.That(await OwnedLog(registry).GetRetentionAsync(cancellationToken)).IsEqualTo(new FollowerLogRetention(HeaderLength(), 0, 0, 2UL));
        _ = await Assert.That(registry.EligibilityFor("n1").StateFor(FollowerSlot)).IsEqualTo(ReplicaParticipantState.CatchingUp);
        _ = await Assert.That(await committer.Probe.Repairs.WaitAsync(Bound, TimeProvider.System, cancellationToken)).IsEqualTo(TimedWaitResult.Signaled);
    }

    /// <summary>Past the retention limit a lagging follower is kept when leaving it behind would cost the write majority.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FollowerBehindPastLimitKeepsMajority(CancellationToken cancellationToken)
    {
        var gateway = new ScriptedGateway();
        using var dir = new TempDirectory("squirix-owned-compaction-behind-majority");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);
        await WriteAsync(committer, 1, cancellationToken);
        gateway.Set("n3", FollowerMode.Silent);
        await WriteAsync(committer, 1, cancellationToken);
        _ = registry.EligibilityFor("n1").TryMarkCatchingUp(1, default);

        _ = await Assert.That(await committer.CompactOwnedLogAsync(PastLimit, Durable(), cancellationToken)).IsEqualTo(ReplicaLogCompactionOutcome.FollowerBehind);

        _ = await Assert.That(registry.EligibilityFor("n1").StateFor(FollowerSlot)).IsEqualTo(ReplicaParticipantState.Ready);
        await AssertNotCompactedAsync(registry, cancellationToken);
    }

    /// <summary>A follower left behind by the compaction becomes ready again only through verification, once its log matches the leader.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeftBehindFollowerReturnsByVerification(CancellationToken cancellationToken)
    {
        var gateway = new ScriptedGateway();
        using var dir = new TempDirectory("squirix-owned-compaction-return");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);
        await WriteAsync(committer, 1, cancellationToken);
        gateway.Set("n3", FollowerMode.Silent);
        await WriteAsync(committer, 1, cancellationToken);
        var compacted = await committer.CompactOwnedLogAsync(PastLimit, Durable(), cancellationToken);
        var eligibility = registry.EligibilityFor("n1");

        gateway.Set("n3", FollowerMode.Mismatch);
        var refused = await committer.VerifyReplicasAsync(cancellationToken);
        var stateWhileRefused = eligibility.StateFor(FollowerSlot);
        gateway.Set("n3", FollowerMode.Match);
        var verified = await committer.VerifyReplicasAsync(cancellationToken);

        _ = await Assert.That(compacted).IsEqualTo(ReplicaLogCompactionOutcome.CompactedPastFollower);
        _ = await Assert.That(refused).IsNotEqualTo(ReplicaVerification.AllReady);
        _ = await Assert.That(stateWhileRefused).IsNotEqualTo(ReplicaParticipantState.Ready);
        _ = await Assert.That(verified).IsEqualTo(ReplicaVerification.AllReady);
        _ = await Assert.That(eligibility.StateFor(FollowerSlot)).IsEqualTo(ReplicaParticipantState.Ready);
    }

    /// <summary>A compaction that fails after the demotion leaves the follower out of the write quorum for the normal repair.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedCompactionLeavesFollowerToRepair(CancellationToken cancellationToken)
    {
        var gateway = new ScriptedGateway();
        using var dir = new TempDirectory("squirix-owned-compaction-failed");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);
        await WriteAsync(committer, 1, cancellationToken);
        gateway.Set("n3", FollowerMode.Silent);
        await WriteAsync(committer, 1, cancellationToken);
        _ = OwnedLog(registry).Idempotency.Reserve("client", "in-flight", [1], GroupRecordKind.UserMutation, 1UL, 1UL, GroupReserveKit.Unlogged);

        _ = await Assert.That(await committer.CompactOwnedLogAsync(PastLimit, Durable(), cancellationToken)).IsEqualTo(ReplicaLogCompactionOutcome.UnresolvedOutcome);

        _ = await Assert.That(registry.EligibilityFor("n1").StateFor(FollowerSlot)).IsEqualTo(ReplicaParticipantState.CatchingUp);
        _ = await Assert.That(await committer.Probe.Repairs.WaitAsync(Bound, TimeProvider.System, cancellationToken)).IsEqualTo(TimedWaitResult.Signaled);
    }

    /// <summary>A follower the advisory check named that holds the commit index under the gate is not demoted, and the compaction is an ordinary one.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AdvisedFollowerThatCaughtUpIsKept(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owned-compaction-caught-up");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway());
        await WriteAsync(committer, 2, cancellationToken);

        var outcome = await RunGatedAsync(committer, registry, 1 << FollowerSlot, cancellationToken);

        _ = await Assert.That(outcome).IsEqualTo(ReplicaLogCompactionOutcome.Compacted);
        _ = await Assert.That(registry.EligibilityFor("n1").StateFor(FollowerSlot)).IsEqualTo(ReplicaParticipantState.Ready);
    }

    /// <summary>A follower that is behind only under the gate, and was not named by the advisory check, is not demoted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnadvisedFollowerBehindIsNotDemoted(CancellationToken cancellationToken)
    {
        var gateway = new ScriptedGateway();
        using var dir = new TempDirectory("squirix-owned-compaction-unadvised");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);
        await WriteAsync(committer, 1, cancellationToken);
        gateway.Set("n3", FollowerMode.Silent);
        await WriteAsync(committer, 1, cancellationToken);

        var outcome = await RunGatedAsync(committer, registry, 1 << 1, cancellationToken);

        _ = await Assert.That(outcome).IsEqualTo(ReplicaLogCompactionOutcome.FollowerBehind);
        _ = await Assert.That(registry.EligibilityFor("n1").StateFor(FollowerSlot)).IsEqualTo(ReplicaParticipantState.Ready);
        await AssertNotCompactedAsync(registry, cancellationToken);
    }

    private static Task<ReplicaLogCompactionOutcome> RunGatedAsync(ReplicaGroupCommitter committer, ReplicaGroupRegistry registry, int advised, CancellationToken cancellationToken) =>
        ReplicaLogCompactionStep.RunAsync(
            OwnedLog(registry),
            committer.Coordinator!,
            (registry.EligibilityFor("n1"), true, advised),
            committer.Applier.AppliedIndex,
            Durable(),
            TimeProvider.System,
            cancellationToken);
}
