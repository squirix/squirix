using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
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
/// The gated compaction step of an RF=3 group owner: it compacts only a committed, applied log that every follower durably holds, and
/// a write arriving meanwhile appends after the compacted log.
/// </summary>
public sealed class OwnedLogCompactionTests : ServerUnitTestBase
{
    private static readonly ReplicaLogCompactionPolicy AnyEntry = new(long.MaxValue, 1);
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    /// <summary>A committed, applied log every follower holds compacts to its header, and keeps its last term from the snapshot baseline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactsToHeaderOnly(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owned-compaction");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway());
        await WriteAsync(committer, 3, cancellationToken);

        _ = await Assert.That(await committer.CompactOwnedLogAsync(AnyEntry, Durable(), cancellationToken)).IsEqualTo(ReplicaLogCompactionOutcome.Compacted);

        var log = OwnedLog(registry);
        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That(await log.GetRetentionAsync(cancellationToken)).IsEqualTo(new FollowerLogRetention(HeaderLength(), 0, 0, 3UL));
        _ = await Assert.That((status.LastLogIndex, status.LastLogTerm, status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((3UL, 1UL, 3UL, 3UL));
        _ = await Assert.That(await log.GetTermAtAsync(3UL, cancellationToken)).IsEqualTo(1UL);
    }

    /// <summary>A log below both thresholds is left alone without taking the commit gate.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BelowThresholdKeepsLog(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owned-compaction-threshold");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway());
        await WriteAsync(committer, 2, cancellationToken);

        var outcome = await committer.CompactOwnedLogAsync(new ReplicaLogCompactionPolicy(long.MaxValue, 3), Durable(), cancellationToken);

        _ = await Assert.That(outcome).IsEqualTo(ReplicaLogCompactionOutcome.BelowThreshold);
        _ = await Assert.That((await OwnedLog(registry).GetRetentionAsync(cancellationToken)).RetainedEntries).IsEqualTo(2);
    }

    /// <summary>A restarted owner whose commit coordinator has not started yet does not compact.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NotStartedIsNotReady(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owned-compaction-not-started");
        await SeedAsync(dir, cancellationToken);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway());

        _ = await Assert.That(await committer.CompactOwnedLogAsync(AnyEntry, Durable(), cancellationToken)).IsEqualTo(ReplicaLogCompactionOutcome.NotReady);
        await AssertNotCompactedAsync(registry, cancellationToken);
    }

    /// <summary>A committed entry whose memory apply failed keeps the log.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PendingApplyKeepsLog(CancellationToken cancellationToken)
    {
        var cache = new ILogicalNamespacedCacheCreateExpectations<object?>();
        _ = cache.Setups.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<object?>>(), Arg.Any<CancellationToken>())
                 .Callback(static (_, _, _, _, _) => ValueTask.FromException(new IOException("memory apply failed")));
        using var dir = new TempDirectory("squirix-owned-compaction-pending");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway(), cache.Instance());
        var unknown = await NodeAsyncAssert.ThrowsAsync<SquirixException>(committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry("k1"), cancellationToken));

        var outcome = await committer.CompactOwnedLogAsync(AnyEntry, Durable(), cancellationToken);

        _ = await Assert.That(unknown.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        _ = await Assert.That(outcome).IsEqualTo(ReplicaLogCompactionOutcome.PendingApply);
        await AssertNotCompactedAsync(registry, cancellationToken);
    }

    /// <summary>
    /// An entry appended locally but not committed keeps the log, even with every follower verified at the commit index: the followers may
    /// still need it.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UncommittedTailKeepsLog(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owned-compaction-tail");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway());
        await WriteAsync(committer, 1, cancellationToken);
        await AppendTailAsync(registry, 1, new StubCache(), cancellationToken, "t1");
        var status = await OwnedLog(registry).GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex)).IsEqualTo((2UL, 1UL));
        _ = await Assert.That(registry.EligibilityFor("n1").AllCanCountInWriteQuorum()).IsTrue();

        _ = await Assert.That(await committer.CompactOwnedLogAsync(AnyEntry, Durable(), cancellationToken)).IsEqualTo(ReplicaLogCompactionOutcome.UncommittedTail);
        await AssertNotCompactedAsync(registry, cancellationToken);
    }

    /// <summary>A follower slot that is not verified ready keeps the log: the leader cannot tell which entries that follower still needs.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FollowerNotReadyKeepsLog(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owned-compaction-not-ready");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway());
        await WriteAsync(committer, 2, cancellationToken);
        _ = registry.EligibilityFor("n1").TryMarkCatchingUp(2, default);

        _ = await Assert.That(await committer.CompactOwnedLogAsync(AnyEntry, Durable(), cancellationToken)).IsEqualTo(ReplicaLogCompactionOutcome.FollowerNotReady);
        await AssertNotCompactedAsync(registry, cancellationToken);
    }

    /// <summary>A ready follower that has not acknowledged every committed entry keeps the log.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FollowerBehindKeepsLog(CancellationToken cancellationToken)
    {
        var gateway = new ScriptedGateway();
        using var dir = new TempDirectory("squirix-owned-compaction-behind");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);
        await WriteAsync(committer, 1, cancellationToken);
        gateway.Set("n3", FollowerMode.Silent);
        await WriteAsync(committer, 1, cancellationToken);

        _ = await Assert.That(await committer.CompactOwnedLogAsync(AnyEntry, Durable(), cancellationToken)).IsEqualTo(ReplicaLogCompactionOutcome.FollowerBehind);
        await AssertNotCompactedAsync(registry, cancellationToken);
    }

    /// <summary>A follower whose append failed is taken out of the quorum, and its slot keeps the log as not ready.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DownFollowerKeepsLog(CancellationToken cancellationToken)
    {
        var gateway = new ScriptedGateway();
        using var dir = new TempDirectory("squirix-owned-compaction-down");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);
        await WriteAsync(committer, 1, cancellationToken);
        gateway.Set("n3", FollowerMode.Down);
        await WriteAsync(committer, 1, cancellationToken);
        _ = await committer.Probe.Repairs.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(await committer.CompactOwnedLogAsync(AnyEntry, Durable(), cancellationToken)).IsEqualTo(ReplicaLogCompactionOutcome.FollowerNotReady);
        await AssertNotCompactedAsync(registry, cancellationToken);
    }

    /// <summary>A committed entry whose idempotency outcome is still unresolved keeps the log, and readiness stays untouched.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnresolvedOutcomeKeepsLog(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owned-compaction-unresolved");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway());
        await WriteAsync(committer, 2, cancellationToken);
        _ = OwnedLog(registry).Idempotency.Reserve("client", "in-flight", [1], GroupRecordKind.UserMutation, 1UL, 1UL);

        _ = await Assert.That(await committer.CompactOwnedLogAsync(AnyEntry, Durable(), cancellationToken)).IsEqualTo(ReplicaLogCompactionOutcome.UnresolvedOutcome);
        await AssertNotCompactedAsync(registry, cancellationToken);
    }

    /// <summary>A snapshot past the maximum snapshot size keeps the log, and readiness stays untouched.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OversizedSnapshotKeepsLog(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owned-compaction-oversized");
        await using var registry = await OpenRegistryAsync(dir, new FollowerLogOptions { MaxSnapshotBytes = 64 }, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway());
        await WriteAsync(committer, 2, cancellationToken);

        _ = await Assert.That(await committer.CompactOwnedLogAsync(AnyEntry, Durable(), cancellationToken)).IsEqualTo(ReplicaLogCompactionOutcome.SnapshotTooLarge);
        await AssertNotCompactedAsync(registry, cancellationToken);
    }

    /// <summary>
    /// A write arriving while the step holds the commit gate waits for it, then appends right after the compacted log, with the
    /// snapshot baseline as its predecessor.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WriteWaitsForCompaction(CancellationToken cancellationToken)
    {
        var gateway = new ScriptedGateway();
        using var dir = new TempDirectory("squirix-owned-compaction-concurrent");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);
        await WriteAsync(committer, 2, cancellationToken);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var durability = new IJournalDurabilityCoordinatorCreateExpectations();
        _ = durability.Setups.AwaitDurabilityCommitAsync(Arg.Any<CancellationToken>())
                      .Callback(async token =>
                      {
                          _ = entered.TrySetResult();
                          await release.Task.WaitAsync(Bound, TimeProvider.System, token).ConfigureAwait(false);
                      });

        var compaction = committer.CompactOwnedLogAsync(AnyEntry, durability.Instance(), cancellationToken);
        await entered.Task.WaitAsync(Bound, TimeProvider.System, cancellationToken);
        var write = committer.CommitSetAsync(NewOperationId(), "cache", "k3", Entry("k3"), cancellationToken);
        var duringStep = await OwnedLog(registry).GetStatusAsync(cancellationToken);
        _ = await Assert.That(write.IsCompleted).IsFalse().Because("The write must wait for the compaction step to release the commit gate.");

        release.SetResult();
        _ = await Assert.That(await compaction.WaitAsync(Bound, TimeProvider.System, cancellationToken)).IsEqualTo(ReplicaLogCompactionOutcome.Compacted);
        await write.WaitAsync(Bound, TimeProvider.System, cancellationToken);

        var log = OwnedLog(registry);
        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That(duringStep.LastLogIndex).IsEqualTo(2UL);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex)).IsEqualTo((3UL, 3UL));
        _ = await Assert.That((await log.GetRetentionAsync(cancellationToken)).SnapshotIndex).IsEqualTo(2UL);
        _ = await Assert.That(await log.GetTermAtAsync(2UL, cancellationToken)).IsEqualTo(1UL);
        _ = await Assert.That(gateway.Appends).Contains(("n2", 2UL, 1));
    }

    private static async Task AssertNotCompactedAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken)
    {
        var log = OwnedLog(registry);
        var retention = await log.GetRetentionAsync(cancellationToken);
        _ = await Assert.That(retention.SnapshotIndex).IsEqualTo(0UL);
        _ = await Assert.That(retention.RetainedEntries > 0).IsTrue();
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).Readiness).IsEqualTo(FollowerLogReadiness.Ready);
    }

    private static IJournalDurabilityCoordinator Durable()
    {
        var durability = new IJournalDurabilityCoordinatorCreateExpectations();
        _ = durability.Setups.AwaitDurabilityCommitAsync(Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        return durability.Instance();
    }

    private static long HeaderLength() => GroupLogCodec.LogFileHeader.Length;

    private static IFollowerLog OwnedLog(ReplicaGroupRegistry registry) =>
        registry.TryGetLog("n1", out var log) ? log : throw new InvalidOperationException("The owned group log is not open.");

    private static async Task WriteAsync(ReplicaGroupCommitter committer, int count, CancellationToken cancellationToken)
    {
        for (var i = 0; i < count; i++)
        {
            var key = NewOperationId();
            await committer.CommitSetAsync(NewOperationId(), "cache", key, Entry(key), cancellationToken);
        }
    }
}
