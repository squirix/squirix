using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
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

    /// <summary>
    /// A verification that passed its first disposal check and then queued behind a disposal does not apply the committed entry still
    /// pending in the coordinator, and fails with <see cref="ObjectDisposedException" />, because the disposal keeps the gate until the
    /// coordinator and the gate are disposed.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QueuedVerifyDoesNotApplyAfterDispose(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.StallThenFail);
        var gateway = new ParkingGateway { HeldNode = "n3" };
        await using var registry = await OpenRegistryAsync(3, cancellationToken);
        var committer = CreateCommitter(registry, local, gateway, null, new ThreeNodeLocator());
        try
        {
            // The write holds the gate while its first apply stalls; the entry is committed and stays pending when the apply fails. The
            // second follower never answers, so a background observer stays owned by the coordinator and its disposal keeps waiting
            // for it: a disposal that released the gate first would let the queued verification apply meanwhile.
            var write = committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry(), CancellationToken.None);
            await local.ApplyEntered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            // A follower that is not ready makes the verification probe, and park, before it queues on the gate.
            _ = registry.EligibilityFor(OwnedGroup).TryMarkCatchingUp(1, default);
            gateway.Arm();
            var verify = committer.VerifyReplicasAsync(CancellationToken.None);
            await gateway.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

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
    /// wait on no token, with <see cref="ObjectDisposedException" /> instead of leaving them queued.
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

            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(queued.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
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
        new(registry, locator ?? new TwoNodeLocator(), gateway, local, OwnedGroup, new ReplicaTopologyStamp(Fingerprint, 1), NullLogger<ReplicaGroupCommitter>.Instance)
        {
            ShutdownBudget = shutdownBudget ?? StallTimeout,
        };

    private static NodeCacheEntry<object?> Entry() => new() { Value = "v", Version = 1 };

    private static string NewOperationId() => Guid.NewGuid().ToString("N");

    private Task<ReplicaGroupRegistry> OpenRegistryAsync(CancellationToken cancellationToken) => OpenRegistryAsync(2, cancellationToken);

    private async Task<ReplicaGroupRegistry> OpenRegistryAsync(int replicaCount, CancellationToken cancellationToken)
    {
        var registry = new ReplicaGroupRegistry(Dir, [OwnedGroup], replicaCount, Fingerprint, 1, NullLoggerFactory.Instance);
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
