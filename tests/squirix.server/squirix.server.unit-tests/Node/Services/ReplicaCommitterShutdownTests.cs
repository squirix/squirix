using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaCommitterDoubles;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Disposing an RF=2 group owner while callers queue on its commit gate.</summary>
public sealed class ReplicaCommitterShutdownTests : IsolatedStorageTestBase
{
    private const string OwnedGroup = "n1";

    private static readonly byte[] Fingerprint = [9, 8, 7];

    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    /// <summary>Bounds the coordinator's wait for its parked observer: the window in which a gate released early would let the verification apply.</summary>
    private static readonly TimeSpan ObserverDrainBudget = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// A verification that passed its first disposal check and queued on the gate to retry the committed entry still pending in the
    /// coordinator does not apply it once a disposal began behind it, and fails with <see cref="ObjectDisposedException" />, because it
    /// checks the disposal again once it holds the gate.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BehindVerifyDoesNotApplyAfterDispose(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.StallThenFail);
        await using var registry = await OpenRegistryAsync(3, cancellationToken);
        var gateway = new ParkingGateway { HeldNode = "n3" };
        var committer = CreateCommitter(registry, gateway, local);
        try
        {
            // The write holds the gate while its first apply stalls; the entry is committed and stays pending when the apply fails. The
            // second follower never answers, so a background observer stays owned by the coordinator and its disposal keeps waiting for it.
            var write = committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry(), CancellationToken.None);
            await local.ApplyEntered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            // The verification retries the pending apply before it probes, so it queues on the gate behind the write at once.
            var verify = committer.VerifyReplicasAsync(CancellationToken.None);

            // The disposal queues on the gate behind the verification and marks the committer disposed first.
            var dispose = committer.DisposeAsync().AsTask();
            local.ReleaseApply();

            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(verify.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            await dispose.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            var unknown = await NodeAsyncAssert.ThrowsAsync<SquirixException>(write.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

            _ = await Assert.That(unknown.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
            _ = await Assert.That(local.ApplyAttempts).IsEqualTo(1);
        }
        finally
        {
            gateway.ReleaseHeld();
            local.ReleaseApply();
            await committer.DisposeAsync();
        }
    }

    /// <summary>
    /// A disposal queued ahead keeps the gate for the whole teardown of the coordinator, which waits for a background observer: a
    /// verification that was not behind, probed, and queues on the gate only afterwards fails with <see cref="ObjectDisposedException" />
    /// without applying the committed entry still pending.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeAheadHoldsGateDuringTeardown(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.StallThenFail);
        var gateway = new ParkingGateway { HeldNode = "n3" };
        await using var registry = await OpenRegistryAsync(3, cancellationToken);
        var committer = CreateCommitter(registry, gateway, local);
        try
        {
            // Nothing is pending yet, so the verification probes its unverified followers and parks in the probe.
            _ = registry.EligibilityFor(OwnedGroup).TryMarkCatchingUp(1, default);
            gateway.Arm();
            var verify = committer.VerifyReplicasAsync(CancellationToken.None);
            await gateway.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            // The write holds the gate while its first apply stalls. The second follower never answers, so a background observer stays
            // owned by the coordinator and its disposal keeps waiting for it: a disposal that released the gate first would let the
            // queued verification apply meanwhile.
            var write = committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry(), CancellationToken.None);
            await local.ApplyEntered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            // The disposal queues on the gate ahead of the verification, which queues only once its probe answers.
            var dispose = committer.DisposeAsync().AsTask();
            gateway.Release();
            local.ReleaseApply();

            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(verify.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            await dispose.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            var unknown = await NodeAsyncAssert.ThrowsAsync<SquirixException>(write.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

            _ = await Assert.That(unknown.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
            _ = await Assert.That(local.ApplyAttempts).IsEqualTo(1);
        }
        finally
        {
            gateway.Release();
            gateway.ReleaseHeld();
            local.ReleaseApply();
            await committer.DisposeAsync();
        }
    }

    /// <summary>
    /// A dispose that gives up on a stuck gate holder at its shutdown budget still faults the callers queued behind the holder, which
    /// wait on no token, as refused without a leader instead of leaving them queued.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TimedOutDisposeReleasesQueuedWaiters(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.Stall);
        await using var registry = await OpenRegistryAsync(cancellationToken);
        var committer = CreateCommitter(registry, local, new ParkingGateway(), TimeSpan.FromMilliseconds(200));
        Task? holder = null;
        try
        {
            // The write holds the gate while its memory apply stalls past its majority, which no budget or token ends.
            holder = committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry(), CancellationToken.None);
            await local.ApplyEntered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            var queued = committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry(), CancellationToken.None);

            await committer.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(queued.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            _ = await Assert.That(refused.StatusCode).IsEqualTo(StatusCode.Unavailable);
            _ = await Assert.That(refused.Status.Detail).IsEqualTo(ServerOpContract.NoLeaderAuthorityDetail);
        }
        finally
        {
            local.ReleaseApply();
            if (holder != null)
                _ = await Task.WhenAny(holder).WaitAsync(StallTimeout, TimeProvider.System, CancellationToken.None);

            await committer.DisposeAsync();
        }
    }

    private static ReplicaGroupCommitter CreateCommitter(
        ReplicaGroupRegistry registry,
        ScriptedApplyCache local,
        ParkingGateway gateway,
        TimeSpan? shutdownBudget = null,
        IReplicaGroupLocator? locator = null) =>
        new(registry, locator ?? new TwoNodeLocator(), gateway, local, (OwnedGroup, OwnedGroup), new ReplicaTopologyStamp(Fingerprint, 1), NullLogger<ReplicaGroupCommitter>.Instance)
        {
            Clock = TimeProvider.System,
            ShutdownTimeProvider = TimeProvider.System,
            BudgetTimeProvider = TimeProvider.System,
            Recovery = RecoveryLifecycle.Recovered(),
            Applier = new ReplicaGroupApplier(local, NullLogger.Instance, OwnedGroup, OwnedGroup),
            ShutdownBudget = shutdownBudget ?? StallTimeout,
        };

    private static NodeCacheEntry<object?> Entry() => new() { Value = "v", Version = 1 };

    private static string NewOperationId() => Guid.NewGuid().ToString("N");

    /// <summary>Creates the committer of the owned group of three replicas, whose dispose waits on a fake clock nobody moves.</summary>
    /// <param name="registry">The registry of the owner.</param>
    /// <param name="gateway">The follower transport double.</param>
    /// <param name="local">The local cache double.</param>
    /// <returns>The committer.</returns>
    /// <remarks>
    /// A dispose waits for the gate holder on a clock nobody moves, so a loaded machine cannot expire that wait before the holder is
    /// released; only the coordinator's own observer drain still runs on <see cref="ObserverDrainBudget" />.
    /// </remarks>
    private static ReplicaGroupCommitter CreateCommitter(ReplicaGroupRegistry registry, IReplicaRpcGateway gateway, ScriptedApplyCache local) =>
        new(registry, new ThreeNodeLocator(), gateway, local, (OwnedGroup, OwnedGroup), new ReplicaTopologyStamp(Fingerprint, 1), NullLogger<ReplicaGroupCommitter>.Instance)
        {
            Clock = TimeProvider.System,
            BudgetTimeProvider = TimeProvider.System,
            Recovery = RecoveryLifecycle.Recovered(),
            Applier = new ReplicaGroupApplier(local, NullLogger.Instance, OwnedGroup, OwnedGroup),
            ShutdownBudget = ObserverDrainBudget,
            ShutdownTimeProvider = new FakeTimeProvider(),
        };

    private Task<ReplicaGroupRegistry> OpenRegistryAsync(CancellationToken cancellationToken) => OpenRegistryAsync(2, cancellationToken);

    private async Task<ReplicaGroupRegistry> OpenRegistryAsync(int replicaCount, CancellationToken cancellationToken)
    {
        await SeededLogs.SeedGroupsAsync(Dir, [OwnedGroup], cancellationToken);
        var registry = new ReplicaGroupRegistry(Dir, [OwnedGroup], replicaCount, Fingerprint, 1, NullLoggerFactory.Instance, new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System }) { ElectionClock = TimeProvider.System };
        try
        {
            await registry.OpenAsync(cancellationToken);
        }
        catch
        {
            await registry.DisposeAsync();
            throw;
        }

        return registry;
    }

    [Immutable]
    private sealed class ThreeNodeLocator : IReplicaGroupLocator
    {
        public int ReplicaCount => 3;

        public void GetReplicaGroup(string originalOwnerNodeId, Span<string> destination)
        {
            destination[0] = "n1";
            destination[1] = "n2";
            destination[2] = "n3";
        }
    }
}
