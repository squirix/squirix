using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
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

    private static ReplicaGroupCommitter CreateCommitter(ReplicaGroupRegistry registry, ScriptedApplyCache local, RecoveryLifecycle recovery) =>
        new(registry, new TwoNodeLocator(), new AcceptingGateway(), local, OwnedGroup, new ReplicaTopologyStamp(Fingerprint, 1), NullLogger<ReplicaGroupCommitter>.Instance)
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
