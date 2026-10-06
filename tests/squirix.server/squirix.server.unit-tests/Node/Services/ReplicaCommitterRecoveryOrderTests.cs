using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaCommitterDoubles;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>A replicated write decides and appends only after local recovery has filled the memory it decides against.</summary>
public sealed class ReplicaCommitterRecoveryOrderTests : IsolatedStorageTestBase
{
    private const string OwnedGroup = "n1";

    private static readonly byte[] Fingerprint = [9, 8, 7];

    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    /// <summary>A conditional add waits for the startup gate before it reads memory or appends, and commits once the gate opens.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommitWaitsForRecoveryBeforePreparing(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.Fail);
        local.Recover();
        var recovery = RecoveryLifecycle.Recovering();
        await using var registry = await OpenRegistryAsync(cancellationToken);
        await using var committer = CreateCommitter(registry, local, recovery);

        var write = committer.CommitTryAddAsync(Guid.NewGuid().ToString("N"), "cache", "k1", Entry(), cancellationToken);
        await recovery.Requested.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(write.IsCompleted).IsFalse();
        _ = await Assert.That(local.EntryReads).IsEqualTo(0);
        _ = await Assert.That(await LastLogIndexAsync(registry, cancellationToken)).IsEqualTo(0UL);

        recovery.Release();
        _ = await write.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(local.EntryReads).IsGreaterThan(0);
        _ = await Assert.That(await LastLogIndexAsync(registry, cancellationToken)).IsEqualTo(1UL);
    }

    /// <summary>A write canceled while it waits for recovery fails with the cancellation and appends nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledWaitForRecoveryAppendsNothing(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.Fail);
        local.Recover();
        var recovery = RecoveryLifecycle.Recovering();
        await using var registry = await OpenRegistryAsync(cancellationToken);
        await using var committer = CreateCommitter(registry, local, recovery);
        using var canceling = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var write = committer.CommitTryAddAsync(Guid.NewGuid().ToString("N"), "cache", "k1", Entry(), canceling.Token);
        await recovery.Requested.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        await canceling.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(write.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

        _ = await Assert.That(local.EntryReads).IsEqualTo(0);
        _ = await Assert.That(await LastLogIndexAsync(registry, cancellationToken)).IsEqualTo(0UL);
    }

    /// <summary>A commit parked on recovery does not hold the commit gate, so another gate taker still runs.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ParkedCommitDoesNotHoldGate(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.Fail);
        local.Recover();
        var recovery = RecoveryLifecycle.Recovering();
        await using var registry = await OpenRegistryAsync(cancellationToken);
        await using var committer = CreateCommitter(registry, local, recovery);
        var durability = new IJournalDurabilityCoordinatorCreateExpectations();

        var write = committer.CommitTryAddAsync(Guid.NewGuid().ToString("N"), "cache", "k1", Entry(), cancellationToken);
        await recovery.Requested.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        var outcome = await committer.CompactOwnedLogAsync(new ReplicaLogCompactionPolicy(0, 1), durability.Instance(), cancellationToken)
                                     .WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(outcome).IsEqualTo(ReplicaLogCompactionOutcome.NotReady);
        _ = await Assert.That(write.IsCompleted).IsFalse();

        recovery.Release();
        _ = await write.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
    }

    /// <summary>A verification waits for the startup gate before it probes any follower, and probes once the gate opens.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VerifyWaitsForRecoveryBeforeProbing(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.Fail);
        local.Recover();
        var recovery = RecoveryLifecycle.Recovering();
        var probes = 0;
        await using var registry = await OpenRegistryAsync(cancellationToken);
        await using var committer = CreateCommitter(registry, local, recovery, new AcceptingGateway(() => _ = Interlocked.Increment(ref probes)));
        _ = registry.EligibilityFor(OwnedGroup).TryMarkCatchingUp(1, default);

        var verify = committer.VerifyReplicasAsync(cancellationToken);
        await recovery.Requested.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(verify.IsCompleted).IsFalse();
        _ = await Assert.That(Volatile.Read(ref probes)).IsEqualTo(0);
        _ = await Assert.That(registry.EligibilityFor(OwnedGroup).StateFor(1)).IsEqualTo(ReplicaParticipantState.CatchingUp);
        _ = await Assert.That(local.EntryReads).IsEqualTo(0);

        recovery.Release();
        _ = await verify.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(Volatile.Read(ref probes)).IsGreaterThan(0);
    }

    /// <summary>A verification parked on recovery fails closed when the committer is disposed meanwhile, and probes no follower.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VerifyParkedOnRecoveryFailsAfterDispose(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.Fail);
        local.Recover();
        var recovery = RecoveryLifecycle.Recovering();
        var probes = 0;
        await using var registry = await OpenRegistryAsync(cancellationToken);
        var committer = CreateCommitter(registry, local, recovery, new AcceptingGateway(() => _ = Interlocked.Increment(ref probes)));
        _ = registry.EligibilityFor(OwnedGroup).TryMarkCatchingUp(1, default);

        var verify = committer.VerifyReplicasAsync(cancellationToken);
        await recovery.Requested.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        await committer.DisposeAsync();
        recovery.Release();

        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(verify.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

        _ = await Assert.That(Volatile.Read(ref probes)).IsEqualTo(0);
    }

    /// <summary>A write parked on recovery fails closed when the committer is disposed meanwhile, and appends nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommitParkedOnRecoveryFailsAfterDispose(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.Fail);
        local.Recover();
        var recovery = RecoveryLifecycle.Recovering();
        await using var registry = await OpenRegistryAsync(cancellationToken);
        var committer = CreateCommitter(registry, local, recovery);

        var write = committer.CommitTryAddAsync(Guid.NewGuid().ToString("N"), "cache", "k1", Entry(), cancellationToken);
        await recovery.Requested.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        await committer.DisposeAsync();
        recovery.Release();

        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(write.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

        _ = await Assert.That(local.EntryReads).IsEqualTo(0);
        _ = await Assert.That(await LastLogIndexAsync(registry, cancellationToken)).IsEqualTo(0UL);
    }

    private static ReplicaGroupCommitter CreateCommitter(ReplicaGroupRegistry registry, ScriptedApplyCache local, RecoveryLifecycle recovery, IReplicaRpcGateway? gateway = null) =>
        new(registry, new TwoNodeLocator(), gateway ?? new AcceptingGateway(), local, OwnedGroup, new ReplicaTopologyStamp(Fingerprint, 1), NullLogger<ReplicaGroupCommitter>.Instance)
        {
            Recovery = recovery,
            ShutdownBudget = StallTimeout,
        };

    private static NodeCacheEntry<object?> Entry() => new() { Value = "v", Version = 1 };

    private static async Task<ulong> LastLogIndexAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken)
    {
        var log = registry.TryGetLog(OwnedGroup, out var found) ? found : throw new InvalidOperationException("The owned group log is not open.");
        return (await log.GetStatusAsync(cancellationToken)).LastLogIndex;
    }

    private async Task<ReplicaGroupRegistry> OpenRegistryAsync(CancellationToken cancellationToken)
    {
        var registry = new ReplicaGroupRegistry(Dir, [OwnedGroup], 2, Fingerprint, 1, NullLoggerFactory.Instance);
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
}
