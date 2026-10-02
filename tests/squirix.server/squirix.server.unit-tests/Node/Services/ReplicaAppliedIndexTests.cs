using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Rocks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// An RF=3 group owner tracks the index it applied to memory, densely, persists it only behind the cache journal durability barrier,
/// and after a restart applies again exactly the committed entries above the persisted index.
/// </summary>
public sealed class ReplicaAppliedIndexTests : ServerUnitTestBase
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    /// <summary>An entry that is not the next one is refused before it reaches memory, and an applied entry is never applied again.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ApplyRefusesEntryOutOfOrder(CancellationToken cancellationToken)
    {
        var cache = new StubCache();
        var applier = new ReplicaLeaderApplier(cache, NullLogger.Instance);
        var factory = new ReplicaMutationFactory(cache, "n1", 1UL, TimeProvider.System, NullLogger.Instance);
        var first = factory.PrepareSet(NewOperationId(), "cache", "k1", Entry("k1"), 1UL);
        var second = factory.PrepareSet(NewOperationId(), "cache", "k2", Entry("k2"), 2UL);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(applier.ApplyAsync(2UL, second.CanonicalPayload, cancellationToken));
        _ = await Assert.That(cache.Applied.IsEmpty).IsTrue();

        await applier.ApplyAsync(1UL, first.CanonicalPayload, cancellationToken);
        await applier.ApplyAsync(2UL, second.CanonicalPayload, cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(applier.ApplyAsync(2UL, second.CanonicalPayload, cancellationToken));

        _ = await Assert.That(applier.AppliedIndex).IsEqualTo(2UL);
        await SequenceAssert.EqualAsync(["k1", "k2"], cache.Applied.ToArray(), StringComparer.Ordinal);
    }

    /// <summary>A committed entry whose memory apply has not returned yet is not counted as applied, so the flush does not persist it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppliedIndexWaitsForApply(CancellationToken cancellationToken)
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cache = new ILogicalNamespacedCacheCreateExpectations<object?>();
        _ = cache.Setups.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<object?>>(), Arg.Any<CancellationToken>())
                 .Callback(async (_, _, _, _, token) =>
                 {
                     _ = entered.TrySetResult();
                     await release.Task.WaitAsync(Bound, TimeProvider.System, token).ConfigureAwait(false);
                 });
        using var dir = new TempDirectory("squirix-owner-applied-wait");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway(), cache.Instance());

        var write = committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry("k1"), cancellationToken);
        await entered.Task.WaitAsync(Bound, TimeProvider.System, cancellationToken);
        await committer.FlushAppliedAsync(Durable(), cancellationToken);
        var applying = await StatusAsync(registry, cancellationToken);

        release.SetResult();
        await write.WaitAsync(Bound, TimeProvider.System, cancellationToken);
        await committer.FlushAppliedAsync(Durable(), cancellationToken);
        var applied = await StatusAsync(registry, cancellationToken);

        _ = await Assert.That((applying.CommitIndex, applying.LastAppliedIndex)).IsEqualTo((1UL, 0UL));
        _ = await Assert.That((applied.CommitIndex, applied.LastAppliedIndex)).IsEqualTo((1UL, 1UL));
    }

    /// <summary>
    /// An entry whose apply fails after its majority is not counted as applied; once the apply recovers, the coordinator applies it
    /// ahead of the next write, and the applied index advances through both in index order.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PendingApplyAdvancesInOrder(CancellationToken cancellationToken)
    {
        var applied = new ConcurrentQueue<string>();
        var failing = 1;
        var cache = new ILogicalNamespacedCacheCreateExpectations<object?>();
        _ = cache.Setups.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<object?>>(), Arg.Any<CancellationToken>())
                 .Callback((_, _, key, _, _) =>
                 {
                     if (Volatile.Read(ref failing) != 0)
                         return ValueTask.FromException(new IOException("memory apply failed"));

                     applied.Enqueue(key);
                     return ValueTask.CompletedTask;
                 });
        using var dir = new TempDirectory("squirix-owner-applied-pending");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway(), cache.Instance());

        var unknown = await NodeAsyncAssert.ThrowsAsync<SquirixException>(committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry("k1"), cancellationToken));
        await committer.FlushAppliedAsync(Durable(), cancellationToken);
        var failed = await StatusAsync(registry, cancellationToken);
        Volatile.Write(ref failing, 0);
        await committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry("k2"), cancellationToken);
        await committer.FlushAppliedAsync(Durable(), cancellationToken);
        var recovered = await StatusAsync(registry, cancellationToken);

        _ = await Assert.That(unknown.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        _ = await Assert.That((failed.CommitIndex, failed.LastAppliedIndex)).IsEqualTo((1UL, 0UL));
        _ = await Assert.That((recovered.CommitIndex, recovered.LastAppliedIndex)).IsEqualTo((2UL, 2UL));
        await SequenceAssert.EqualAsync(["k1", "k2"], applied.ToArray(), StringComparer.Ordinal);
    }

    /// <summary>The flush persists the applied index only after the cache journal durability barrier returned, and releases the payloads.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FlushAwaitsBarrierFirst(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-applied-barrier");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway());
        await committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry("k1"), cancellationToken);
        ulong? atBarrier = null;
        var durability = new IJournalDurabilityCoordinatorCreateExpectations();
        _ = durability.Setups.AwaitDurabilityCommitAsync(Arg.Any<CancellationToken>())
                      .Callback(async token => atBarrier = (await StatusAsync(registry, token)).LastAppliedIndex);

        await committer.FlushAppliedAsync(durability.Instance(), cancellationToken);

        _ = await Assert.That(atBarrier).IsEqualTo(0UL).Because("The applied index must not be persisted before the barrier returns.");
        _ = await Assert.That((await StatusAsync(registry, cancellationToken)).LastAppliedIndex).IsEqualTo(1UL);
        _ = await Assert.That((await OwnedLog(registry).GetCommittedEntriesAsync(cancellationToken)).Count).IsEqualTo(0);
    }

    /// <summary>A failed durability barrier leaves the durable applied index and the retained payloads unchanged.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedBarrierKeepsApplied(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-applied-barrier-fail");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway());
        await committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry("k1"), cancellationToken);
        var durability = new IJournalDurabilityCoordinatorCreateExpectations();
        _ = durability.Setups.AwaitDurabilityCommitAsync(Arg.Any<CancellationToken>())
                      .Callback(static _ => ValueTask.FromException(new IOException("journal flush failed")));

        _ = await NodeAsyncAssert.ThrowsAsync<IOException>(committer.FlushAppliedAsync(durability.Instance(), cancellationToken));

        _ = await Assert.That((await StatusAsync(registry, cancellationToken)).LastAppliedIndex).IsEqualTo(0UL);
        _ = await Assert.That((await OwnedLog(registry).GetCommittedEntriesAsync(cancellationToken)).Count).IsEqualTo(1);
    }

    /// <summary>
    /// A restarted owner applies again, in log order, exactly the committed entries above the persisted applied index, before the first new
    /// write; nothing at or below the persisted index is applied again, and a recovered tail entry applies the effect its record carries.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartReappliesAboveApplied(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-applied-restart");
        await using (var registry = await OpenRegistryAsync(dir, cancellationToken))
        {
            await using var committer = CreateCommitter(registry, new ScriptedGateway());
            await committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry("k1"), cancellationToken);
            await committer.FlushAppliedAsync(Durable(), cancellationToken);
            await committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry("k2"), cancellationToken);
            await committer.CommitSetAsync(NewOperationId(), "cache", "k3", Entry("k3"), cancellationToken);
        }

        // The uncommitted tail adds k3 again, decided while the leader held k3: it carries the outcome false and changes nothing,
        // however memory looks when it is applied.
        var leaderMemory = new StubCache();
        await leaderMemory.SetEntryAsync(NewOperationId(), "cache", "k3", Entry("k3"), cancellationToken);
        await SeedTailAsync(dir, 1, leaderMemory, cancellationToken, "k3");
        var cache = new StubCache();
        await using var restarted = await OpenRegistryAsync(dir, cancellationToken);
        await using var owner = CreateCommitter(restarted, new ScriptedGateway(), cache);

        await owner.CommitSetAsync(NewOperationId(), "cache", "k4", Entry("k4"), cancellationToken);
        var added = await owner.CommitTryAddAsync(TailOperationId("k3"), "cache", "k3", Entry("k3"), cancellationToken);

        await SequenceAssert.EqualAsync(["k2", "k3", "k4"], cache.Applied.ToArray(), StringComparer.Ordinal);
        _ = await Assert.That(added).IsFalse().Because("The recovered tail reports the outcome its record carries.");
        var status = await StatusAsync(restarted, cancellationToken);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex)).IsEqualTo((5UL, 5UL));
    }

    /// <summary>A resync after a write refused before its append keeps the applied index, so it applies no entry of this process again.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ResyncKeepsAppliedIndex(CancellationToken cancellationToken)
    {
        var cache = new StubCache();
        using var dir = new TempDirectory("squirix-owner-applied-resync");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway(), cache);
        await committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry("k1"), cancellationToken);
        await committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry("k2"), cancellationToken);

        // Demoting both followers leaves no verified majority: the next write is refused before its append and drops the started state,
        // so the write after it resyncs while the durable applied index is still behind the commit index.
        var eligibility = registry.EligibilityFor("n1");
        _ = eligibility.TryMarkCatchingUp(1, default);
        _ = eligibility.TryMarkCatchingUp(2, default);
        var refusal = await NodeAsyncAssert.ThrowsAsync<RpcException>(committer.CommitSetAsync(NewOperationId(), "cache", "refused", Entry("refused"), cancellationToken));
        _ = await Assert.That(refusal.StatusCode).IsEqualTo(StatusCode.Unavailable);
        await committer.CommitSetAsync(NewOperationId(), "cache", "k3", Entry("k3"), cancellationToken);

        await SequenceAssert.EqualAsync(["k1", "k2", "k3"], cache.Applied.ToArray(), StringComparer.Ordinal);
        var status = await StatusAsync(registry, cancellationToken);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((3UL, 3UL, 0UL));
    }

    private static IJournalDurabilityCoordinator Durable()
    {
        var durability = new IJournalDurabilityCoordinatorCreateExpectations();
        _ = durability.Setups.AwaitDurabilityCommitAsync(Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        return durability.Instance();
    }

    private static IFollowerLog OwnedLog(ReplicaGroupRegistry registry) =>
        registry.TryGetLog("n1", out var log) ? log : throw new InvalidOperationException("The owned group log is not open.");
}
