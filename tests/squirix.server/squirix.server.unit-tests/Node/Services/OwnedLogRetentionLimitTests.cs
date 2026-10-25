using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The gated compaction step of an RF=3 group owner past its retention limit, where a ready follower that stays behind is left behind.</summary>
public sealed class OwnedLogRetentionLimitTests : ServerUnitTestBase
{
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
        await OwnedLogCompactionTests.WriteAsync(committer, 1, cancellationToken);
        gateway.Set("n3", FollowerMode.Silent);
        await OwnedLogCompactionTests.WriteAsync(committer, 1, cancellationToken);

        _ = await Assert.That(await committer.CompactOwnedLogAsync(OwnedLogCompactionTests.PastLimit, OwnedLogCompactionTests.Durable(), cancellationToken)).IsEqualTo(ReplicaLogCompactionOutcome.CompactedPastFollower);

        _ = await Assert.That(await OwnedLogCompactionTests.OwnedLog(registry).GetRetentionAsync(cancellationToken)).IsEqualTo(new FollowerLogRetention(OwnedLogCompactionTests.HeaderLength(), 0, 0, 2UL));
        _ = await Assert.That(registry.EligibilityFor("n1").StateFor(OwnedLogCompactionTests.FollowerSlot)).IsEqualTo(ReplicaParticipantState.CatchingUp);
        _ = await committer.Probe.Repairs.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
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
        await OwnedLogCompactionTests.WriteAsync(committer, 1, cancellationToken);
        gateway.Set("n3", FollowerMode.Silent);
        await OwnedLogCompactionTests.WriteAsync(committer, 1, cancellationToken);
        _ = registry.EligibilityFor("n1").TryMarkCatchingUp(1, default);

        _ = await Assert.That(await committer.CompactOwnedLogAsync(OwnedLogCompactionTests.PastLimit, OwnedLogCompactionTests.Durable(), cancellationToken)).IsEqualTo(ReplicaLogCompactionOutcome.FollowerBehind);

        _ = await Assert.That(registry.EligibilityFor("n1").StateFor(OwnedLogCompactionTests.FollowerSlot)).IsEqualTo(ReplicaParticipantState.Ready);
        await OwnedLogCompactionTests.AssertNotCompactedAsync(registry, cancellationToken);
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
        await OwnedLogCompactionTests.WriteAsync(committer, 1, cancellationToken);
        gateway.Set("n3", FollowerMode.Silent);
        await OwnedLogCompactionTests.WriteAsync(committer, 1, cancellationToken);
        _ = await committer.CompactOwnedLogAsync(OwnedLogCompactionTests.PastLimit, OwnedLogCompactionTests.Durable(), cancellationToken);
        var eligibility = registry.EligibilityFor("n1");

        _ = await committer.CompactOwnedLogAsync(OwnedLogCompactionTests.PastLimit, OwnedLogCompactionTests.Durable(), cancellationToken);
        gateway.Set("n3", FollowerMode.Mismatch);
        var refused = await committer.VerifyReplicasAsync(cancellationToken);
        var stateWhileRefused = eligibility.StateFor(OwnedLogCompactionTests.FollowerSlot);
        gateway.Set("n3", FollowerMode.Match);
        var verified = await committer.VerifyReplicasAsync(cancellationToken);

        _ = await Assert.That(refused).IsNotEqualTo(ReplicaVerification.AllReady);
        _ = await Assert.That(stateWhileRefused).IsNotEqualTo(ReplicaParticipantState.Ready);
        _ = await Assert.That(verified).IsEqualTo(ReplicaVerification.AllReady);
        _ = await Assert.That(eligibility.StateFor(OwnedLogCompactionTests.FollowerSlot)).IsEqualTo(ReplicaParticipantState.Ready);
    }
}
