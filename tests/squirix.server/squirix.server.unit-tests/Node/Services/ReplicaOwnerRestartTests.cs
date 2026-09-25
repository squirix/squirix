using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>An RF=3 group owner restarted on durable data verifies its replica slots and regains its write quorum.</summary>
public sealed class ReplicaOwnerRestartTests : ServerUnitTestBase
{
    /// <summary>Followers holding the leader tail are verified on the first write and the write commits.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OwnerRestartCommitsWithFollowersUp(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-restart-match");
        await SeedAsync(dir, cancellationToken);
        var gateway = new ScriptedGateway();
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);

        await committer.CommitSetAsync(NewOperationId(), "cache", "k1", new NodeCacheEntry<object?> { Value = "v1", Version = 1 }, cancellationToken);

        var eligibility = registry.EligibilityFor("n1");
        _ = await Assert.That(eligibility.AllCanCountInWriteQuorum()).IsTrue();
    }

    /// <summary>A follower that is down at restart stays out of the quorum while the write still commits on the majority.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartedOwnerCommitsWithFollowerDown(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-restart-down");
        await SeedAsync(dir, cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.Set("n3", FollowerMode.Down);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);

        await committer.CommitSetAsync(NewOperationId(), "cache", "k1", new NodeCacheEntry<object?> { Value = "v1", Version = 1 }, cancellationToken);

        var eligibility = registry.EligibilityFor("n1");
        _ = await Assert.That(eligibility.CanCountInWriteQuorum(0)).IsTrue();
        _ = await Assert.That(eligibility.CanCountInWriteQuorum(1)).IsTrue();
        _ = await Assert.That(eligibility.CanCountInWriteQuorum(2)).IsFalse();
    }

    /// <summary>Followers that do not hold the leader tail are never marked ready, so no write reaches a majority.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DivergedFollowersNeverBecomeReady(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-restart-diverged");
        await SeedAsync(dir, cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Mismatch);
        gateway.Set("n3", FollowerMode.Mismatch);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);

        var write = committer.CommitSetAsync(NewOperationId(), "cache", "k1", new NodeCacheEntry<object?> { Value = "v1", Version = 1 }, cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(write);

        var eligibility = registry.EligibilityFor("n1");
        _ = await Assert.That(eligibility.StateFor(1)).IsEqualTo(ReplicaParticipantState.CatchingUp);
        _ = await Assert.That(eligibility.StateFor(2)).IsEqualTo(ReplicaParticipantState.CatchingUp);
        _ = await Assert.That(eligibility.CanCountInWriteQuorum(1)).IsFalse();
    }

    /// <summary>Verification without any reachable follower leaves every slot recovering and starts nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VerificationWithoutFollowersStaysPending(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-restart-pending");
        await SeedAsync(dir, cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Down);
        gateway.Set("n3", FollowerMode.Down);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);

        var outcome = await committer.VerifyReplicasAsync(cancellationToken);

        _ = await Assert.That(outcome).IsEqualTo(ReplicaVerification.Pending);
        var eligibility = registry.EligibilityFor("n1");
        _ = await Assert.That(eligibility.CanCountInWriteQuorum(0)).IsFalse();
        _ = await Assert.That(eligibility.CanCountInWriteQuorum(1)).IsFalse();
    }

    /// <summary>A follower that becomes reachable after the coordinator started is admitted and counts toward later writes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LateFollowerCountsAfterVerification(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-restart-late");
        await SeedAsync(dir, cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Down);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);
        await committer.CommitSetAsync(NewOperationId(), "cache", "k1", new NodeCacheEntry<object?> { Value = "v1", Version = 1 }, cancellationToken);

        gateway.Set("n2", FollowerMode.Match);
        var outcome = await committer.VerifyReplicasAsync(cancellationToken);
        _ = await Assert.That(outcome).IsEqualTo(ReplicaVerification.AllReady);

        // Only the leader and the late follower can form the majority now: its acknowledgement must count.
        gateway.Set("n3", FollowerMode.Down);
        await committer.CommitSetAsync(NewOperationId(), "cache", "k2", new NodeCacheEntry<object?> { Value = "v2", Version = 1 }, cancellationToken);
    }

    /// <summary>A write that cannot reach a majority is refused before the local append, so it leaves no uncommitted tail behind.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WriteWithoutMajorityLeavesNoTail(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-restart-notail");
        await SeedAsync(dir, cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Down);
        gateway.Set("n3", FollowerMode.Down);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);
        var refused = committer.CommitSetAsync(NewOperationId(), "cache", "k1", new NodeCacheEntry<object?> { Value = "v1", Version = 1 }, cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(refused);
        _ = registry.TryGetLog("n1", out var log);
        var status = await log!.GetStatusAsync(cancellationToken);
        _ = await Assert.That(status.LastLogIndex).IsEqualTo(status.CommitIndex);

        gateway.Set("n2", FollowerMode.Match);
        gateway.Set("n3", FollowerMode.Match);
        await committer.CommitSetAsync(NewOperationId(), "cache", "k2", new NodeCacheEntry<object?> { Value = "v2", Version = 1 }, cancellationToken);

        _ = await Assert.That(registry.EligibilityFor("n1").AllCanCountInWriteQuorum()).IsTrue();
    }

    /// <summary>A follower reporting a longer log than the leader is held back for catch-up instead of counting.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LongerFollowerIsNotReady(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-restart-longer");
        await SeedAsync(dir, cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Longer);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);

        await committer.CommitSetAsync(NewOperationId(), "cache", "k1", new NodeCacheEntry<object?> { Value = "v1", Version = 1 }, cancellationToken);

        var eligibility = registry.EligibilityFor("n1");
        _ = await Assert.That(eligibility.StateFor(1)).IsEqualTo(ReplicaParticipantState.CatchingUp);
        _ = await Assert.That(eligibility.CanCountInWriteQuorum(2)).IsTrue();
    }

    /// <summary>A follower refusing for a reason unrelated to its log leaves its slot untouched.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusedFollowerStaysRecovering(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-restart-refused");
        await SeedAsync(dir, cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Refused);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);

        await committer.CommitSetAsync(NewOperationId(), "cache", "k1", new NodeCacheEntry<object?> { Value = "v1", Version = 1 }, cancellationToken);

        _ = await Assert.That(registry.EligibilityFor("n1").StateFor(1)).IsEqualTo(ReplicaParticipantState.Recovering);
    }
}
