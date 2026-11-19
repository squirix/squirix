using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaApplyServiceTests;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Payload retention and append admission of a follower whose applied progress is delayed.</summary>
public sealed class ReplicaPayloadBacklogTests : ServerUnitTestBase
{
    private static readonly byte[] Fingerprint = [9, 8, 7];

    private static readonly string[] Groups = ["n1", "n2", "n3"];

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>A follower retains acknowledged payloads while the applied index waits for durability and releases only the durable prefix.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The follower group log is not open.</exception>
    [Test]
    public async Task RetainsPayloadsUntilDurableApply(CancellationToken cancellationToken)
    {
        const string groupId = "n2";
        const int entryCount = 4;
        const ulong lastIndex = entryCount;
        using var dir = new TempDirectory("squirix-follower-backlog");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        if (!registry.TryGetLog(groupId, out var log))
            throw new InvalidOperationException("The follower group log is not open.");

        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
        var applier = new ReplicaGroupApplier(new StubCache(), NullLogger.Instance);
        var firstBytes = await AppendFollowerPayloadAsync(follower, groupId, 1UL, cancellationToken);
        await FollowerLogTestKit.MakeCommitDurableAsync(log, cancellationToken);
        await applier.CatchUpAsync(log, 0UL, 1UL, cancellationToken);

        var enteredBarrier = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseBarrier = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var durability = new IJournalDurabilityCoordinatorCreateExpectations();
        _ = durability.Setups.AwaitDurabilityCommitAsync(Arg.Any<CancellationToken>()).Callback(token =>
        {
            _ = enteredBarrier.TrySetResult();
            return new ValueTask(releaseBarrier.Task.WaitAsync(HangGuard, TimeProvider.System, token));
        });
        var coordinator = durability.Instance();
        var flush = applier.FlushAsync(log, coordinator, cancellationToken);
        var expectedBytes = firstBytes;
        try
        {
            await enteredBarrier.Task.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            _ = await Assert.That(await ReadBacklogAsync(log, cancellationToken)).IsEqualTo((1, firstBytes));

            for (var index = 2UL; index <= lastIndex; index++)
                expectedBytes += await AppendFollowerPayloadAsync(follower, groupId, index, cancellationToken);

            await FollowerLogTestKit.MakeCommitDurableAsync(log, cancellationToken);
            await applier.CatchUpAsync(log, 0UL, lastIndex, cancellationToken);
            var stalled = await log.GetStatusAsync(cancellationToken);
            _ = await Assert.That((stalled.LastLogIndex, stalled.CommitIndex, stalled.LastAppliedIndex, applier.AppliedIndex))
                            .IsEqualTo((lastIndex, lastIndex, 0UL, lastIndex));
            _ = await Assert.That(await ReadBacklogAsync(log, cancellationToken)).IsEqualTo((entryCount, expectedBytes));
        }
        finally
        {
            releaseBarrier.SetResult();
            await flush.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }

        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastAppliedIndex).IsEqualTo(1UL);
        _ = await Assert.That(await ReadBacklogAsync(log, cancellationToken)).IsEqualTo((entryCount - 1, expectedBytes - firstBytes));

        await applier.FlushAsync(log, coordinator, cancellationToken);

        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastAppliedIndex).IsEqualTo(lastIndex);
        _ = await Assert.That(await ReadBacklogAsync(log, cancellationToken)).IsEqualTo((0, 0L));
        _ = await Assert.That((await log.GetRetentionAsync(cancellationToken)).RetainedEntries).IsEqualTo(entryCount);
    }

    /// <summary>A full follower refuses an append until applied progress frees room for the very same batch.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The follower group log is not open.</exception>
    [Test]
    public async Task ResumesAppendAfterDurableApply(CancellationToken cancellationToken)
    {
        const string groupId = "n2";
        var first = Batch(Prepare("a", 1UL), 0UL, 0UL);
        var next = Batch(Prepare("b", 2UL), 1UL, 1UL);
        long firstBytes = first.Entries[0].Payload.Length;
        long nextBytes = next.Entries[0].Payload.Length;
        var limit = Math.Max(first.Entries[0].Payload.Length, next.Entries[0].Payload.Length);
        using var dir = new TempDirectory("squirix-follower-backlog-resume");
        await using var registry = await OpenRegistryAsync(dir, Groups, PayloadLimit(limit), cancellationToken);
        if (!registry.TryGetLog(groupId, out var log))
            throw new InvalidOperationException("The follower group log is not open.");

        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
        _ = await Assert.That((await follower.AppendAsync(groupId, Fingerprint, 1, first, cancellationToken)).Success).IsTrue();
        var before = await log.GetRetentionAsync(cancellationToken);
        _ = await Assert.That((before.RetainedPayloads, before.RetainedPayloadBytes)).IsEqualTo((1, firstBytes));

        var blocked = await follower.AppendAsync(groupId, Fingerprint, 1, next, cancellationToken);
        _ = await Assert.That((blocked.Success, blocked.RefusalCode, blocked.LastLogIndex)).IsEqualTo((false, RefusalCodes.ApplyBacklog, 1UL));
        _ = await Assert.That(await log.GetRetentionAsync(cancellationToken)).IsEqualTo(before);
        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CommitHint, status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((1UL, 0UL, 0UL));
        _ = await Assert.That((await follower.AppendAsync(groupId, Fingerprint, 1, first, cancellationToken)).Success).IsTrue();

        await FollowerLogTestKit.MakeCommitDurableAsync(log, cancellationToken);
        var cache = new StubCache();
        var applier = new ReplicaGroupApplier(cache, NullLogger.Instance);
        await applier.CatchUpAsync(log, 0UL, 1UL, cancellationToken);
        var refused = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sender = CreateSender(follower, groupId, refused);
        var mutation = new ReplicaMutationFactory(cache, groupId, 1UL, TimeProvider.System, NullLogger.Instance).Rebuild(next.Entries[0]);
        var pending = sender.EnqueueAsync(mutation, 1UL, 1UL, 1UL);
        await refused.Task.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        _ = await Assert.That(pending.IsCompleted).IsFalse();

        var durable = new IJournalDurabilityCoordinatorCreateExpectations();
        _ = durable.Setups.AwaitDurabilityCommitAsync(Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        await applier.FlushAsync(log, durable.Instance(), cancellationToken);

        var acknowledged = await pending.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        _ = await Assert.That(acknowledged.LogIndex).IsEqualTo(2UL);
        var after = await log.GetRetentionAsync(cancellationToken);
        _ = await Assert.That((after.RetainedPayloads, after.RetainedPayloadBytes)).IsEqualTo((1, nextBytes));
    }

    /// <summary>Cache pressure drives a durable checkpoint on the apply loop while periodic maintenance is stopped.</summary>
    /// <param name="exactFit">Whether the first append fills the cache exactly and requests the checkpoint on acceptance.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The group is not open.</exception>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task CachePressureDrivesDurableCheckpoint(bool exactFit, CancellationToken cancellationToken)
    {
        const string group = "n2";
        var first = Batch(Prepare("a", 1), 0, exactFit ? 1UL : 0UL);
        var limit = first.Entries[0].Payload.Length + (exactFit ? 0 : 1);
        using var dir = new TempDirectory("squirix-pressure-checkpoint");
        await using var registry = await OpenRegistryAsync(dir, Groups, PayloadLimit(limit), cancellationToken);
        if (!registry.TryGetLog(group, out var log))
            throw new InvalidOperationException("The group is not open.");

        var barrier = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var durability = new IJournalDurabilityCoordinatorCreateExpectations();
        _ = durability.Setups.AwaitDurabilityCommitAsync(Arg.Any<CancellationToken>()).Callback(token =>
        {
            _ = barrier.TrySetResult();
            return new ValueTask(release.Task.WaitAsync(token));
        });
        using var meter = new Meter("test");
        var cache = new StubCache();
        var appliers = CreateAppliers(registry, cache, meter);
        using var service = new ReplicaApplyService(
            registry, appliers, LeadOwnGroup(registry, appliers), ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(), durability.Instance(), NullLogger<ReplicaApplyService>.Instance, new FakeTimeProvider());
        await service.StartAsync(cancellationToken);
        try
        {
            var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
            _ = await Assert.That((await follower.AppendAsync(group, Fingerprint, 1, first, cancellationToken)).Success).IsTrue();
            if (exactFit)
                await barrier.Task.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            var refused = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await using var sender = CreateSender(follower, group, refused);
            var mutation = new ReplicaMutationFactory(cache, group, 1, TimeProvider.System, NullLogger.Instance).PrepareSet(NewOperationId(), "cache", "b", Entry("b"), 2);
            var ack = sender.EnqueueAsync(mutation, 1, 1, 1);
            await Task.WhenAll(barrier.Task, refused.Task).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            var stalled = await log.GetStatusAsync(cancellationToken);
            _ = await Assert.That((stalled.LastAppliedIndex, stalled.CommitIndex, ack.IsCompleted)).IsEqualTo((0UL, 1UL, false));

            release.SetResult();
            var accepted = await ack.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            var resumed = await log.GetStatusAsync(cancellationToken);
            _ = await Assert.That((accepted.LogIndex, resumed.LastAppliedIndex, resumed.LastLogIndex)).IsEqualTo((2UL, 1UL, 2UL));
        }
        finally
        {
            _ = release.TrySetResult();
            await service.StopAsync(cancellationToken);
        }
    }

    /// <summary>A full uncommitted tail can still advance through disk-only records, which apply in order.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The follower group log is not open.</exception>
    [Test]
    public async Task FullUncommittedTailSpillsToDisk(CancellationToken cancellationToken)
    {
        const string groupId = "n2";
        var first = Batch(Prepare("larger-first", 1UL), 0UL, 0UL);
        var second = Batch(Prepare("b", 2UL), 1UL, 0UL);
        long limit = first.Entries[0].Payload.Length;
        using var dir = new TempDirectory("squirix-follower-backlog-disk");
        await using var registry = await OpenRegistryAsync(dir, Groups, PayloadLimit(limit), cancellationToken);
        if (!registry.TryGetLog(groupId, out var log))
            throw new InvalidOperationException("The follower group log is not open.");

        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
        _ = await Assert.That((await follower.AppendAsync(groupId, Fingerprint, 1, first, cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await follower.AppendAsync(groupId, Fingerprint, 1, second, cancellationToken)).Success).IsTrue();
        var beforeDuplicate = await log.GetRetentionAsync(cancellationToken);
        _ = await Assert.That((beforeDuplicate.RetainedEntries, beforeDuplicate.RetainedPayloads, beforeDuplicate.RetainedPayloadBytes))
                        .IsEqualTo((2, 1, limit));
        _ = await Assert.That((await follower.AppendAsync(groupId, Fingerprint, 1, second, cancellationToken)).Success).IsTrue();
        _ = await Assert.That(await log.GetRetentionAsync(cancellationToken)).IsEqualTo(beforeDuplicate);
        _ = await Assert.That((await log.GetUncommittedTailAsync(cancellationToken)).Count).IsEqualTo(2);

        // The empty append can publish a commit index referencing a disk-only frame.
        var heartbeat = second with { Entries = [], PrevLogIndex = 2UL, LeaderCommitIndex = 2UL };
        _ = await Assert.That((await follower.AppendAsync(groupId, Fingerprint, 1, heartbeat, cancellationToken)).Success).IsTrue();
        await FollowerLogTestKit.MakeCommitDurableAsync(log, cancellationToken);
        var cache = new StubCache();
        var applier = new ReplicaGroupApplier(cache, NullLogger.Instance);
        await applier.CatchUpAsync(log, 0UL, 2UL, cancellationToken);

        await SequenceAssert.EqualAsync(["larger-first", "b"], cache.Applied.ToArray(), StringComparer.Ordinal);
        _ = await Assert.That(applier.AppliedIndex).IsEqualTo(2UL);
    }

    /// <summary>A committed disk-only frame is never replaced by a conflicting duplicate or a corrupt replay.</summary>
    /// <param name="corruptFrame">Whether the stored frame is damaged before replay.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The follower group log is not open.</exception>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DiskOnlyReplayFailsClosed(bool corruptFrame, CancellationToken cancellationToken)
    {
        const string groupId = "n2";
        var first = Batch(Prepare("a", 1UL), 0UL, 1UL);
        var second = Batch(Prepare("longer-second", 2UL), 1UL, 2UL);
        using var dir = new TempDirectory("squirix-follower-disk-replay");
        await using var registry = await OpenRegistryAsync(dir, Groups, PayloadLimit(first.Entries[0].Payload.Length), cancellationToken);
        if (!registry.TryGetLog(groupId, out var log))
            throw new InvalidOperationException("The follower group log is not open.");

        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
        _ = await Assert.That((await follower.AppendAsync(groupId, Fingerprint, 1, first, cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await follower.AppendAsync(groupId, Fingerprint, 1, second, cancellationToken)).Success).IsTrue();
        await FollowerLogTestKit.MakeCommitDurableAsync(log, cancellationToken);
        var before = await log.GetRetentionAsync(cancellationToken);
        _ = await Assert.That(before.RetainedPayloads).IsEqualTo(1);
        _ = await Assert.That((await follower.AppendAsync(groupId, Fingerprint, 1, second, cancellationToken)).Success).IsTrue();

        if (corruptFrame)
        {
            var entry = second.Entries[0];
            CorruptFrame(log, in entry, before.LogBytes);
        }

        var replay = corruptFrame ? second with { LeaderTerm = 2UL } : Batch(Prepare("different", 2UL), 1UL, 2UL);
        var refused = await follower.AppendAsync(groupId, Fingerprint, 1, replay, cancellationToken);
        _ = await Assert.That((refused.Success, refused.RefusalCode, refused.CurrentTerm))
                        .IsEqualTo((false, corruptFrame ? RefusalCodes.ChecksumMismatch : RefusalCodes.LogMismatch, replay.LeaderTerm));
        _ = await Assert.That(await log.GetRetentionAsync(cancellationToken)).IsEqualTo(before);
        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.Readiness, status.CommitIndex, status.LastLogIndex)).IsEqualTo((FollowerLogReadiness.Failed, 2UL, 2UL));
    }

    /// <summary>Recovery keeps the payload cache bounded and replays a committed suffix that exceeds the bound.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecoveryHonorsPayloadLimit(CancellationToken cancellationToken)
    {
        const string groupId = "n2";
        using var dir = new TempDirectory("squirix-follower-backlog-recovery");
        var first = Batch(Prepare("a", 1UL), 0UL, 2UL).Entries[0];
        var second = Batch(Prepare("b", 2UL), 1UL, 2UL).Entries[0];
        long limit = first.Payload.Length;
        var composition = GroupComposition.Create(groupId);
        await using (var writer = new FollowerLog(dir, groupId, composition, NullLogger<FollowerLog>.Instance, TimeProvider.System))
        {
            await writer.OpenSeededAsync(cancellationToken);
            var append = await writer.AppendAsync(new FollowerLogAppendRequest(groupId, 1, 0UL, 0UL, 2UL, new[] { first, second }), cancellationToken);
            _ = await Assert.That(append.Success).IsTrue();
            await FollowerLogTestKit.MakeCommitDurableAsync(writer, cancellationToken);
        }

        await using var recovered = new FollowerLog(dir, groupId, composition, NullLogger<FollowerLog>.Instance, PayloadLimit(limit));
        await recovered.OpenAsync(cancellationToken);
        _ = await Assert.That(await ReadBacklogAsync(recovered, cancellationToken)).IsEqualTo((1, limit));
        var status = await recovered.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((2UL, 0UL));
        var cache = new StubCache();
        var applier = new ReplicaGroupApplier(cache, NullLogger.Instance);
        await applier.CatchUpAsync(recovered, status.LastAppliedIndex, status.CommitIndex, cancellationToken);
        await SequenceAssert.EqualAsync(["a", "b"], cache.Applied.ToArray(), StringComparer.Ordinal);
    }

    private static void CorruptFrame(IFollowerLog log, in FollowerLogEntry entry, long end)
    {
        if (log is not IFollowerLogContext context)
            throw new InvalidOperationException("The follower log does not have a writable WAL.");

        // Use the active writer because the WAL is opened with write sharing disabled.
        var frame = new byte[GroupLogCodec.ComputeFrameEncodedLength(entry.Payload.Length)];
        GroupLogCodec.EncodeFrame(frame, in entry);
        frame[^1] ^= 0xFF;
        context.Durability.Write(frame, end - frame.Length);
    }

    private static ReplicaFollowerSender CreateSender(ReplicaFollower follower, string groupId, TaskCompletionSource refused)
    {
        var gateway = new IReplicaRpcGatewayCreateExpectations();
        _ = gateway.Setups.AppendEntriesAsync(Arg.Any<string>(), Arg.Any<ReplicaRpcHeader>(), Arg.Any<FollowerBatch>(), Arg.Any<CancellationToken>())
                   .Callback(async (_, _, batch, token) =>
                   {
                       var result = await follower.AppendAsync(groupId, Fingerprint, 1, batch, token).ConfigureAwait(false);
                       if (string.Equals(result.RefusalCode, RefusalCodes.ApplyBacklog, StringComparison.Ordinal))
                           _ = refused.TrySetResult();

                       return result;
                   });
        var header = new ReplicaRpcHeader(groupId, Fingerprint, 1, 1, groupId, groupId);
        return new ReplicaFollowerSender(gateway.Instance(), "n1", in header, 1UL, 1UL, HangGuard)
        {
            TimeProvider = TimeProvider.System,
        };
    }

    private static FollowerLogOptions PayloadLimit(long bytes) => new()
    {
        TimeProvider = TimeProvider.System,
        ShutdownTimeProvider = TimeProvider.System,
        MaxRetainedPayloadBytes = bytes,
    };

    private static async Task<long> AppendFollowerPayloadAsync(
        ReplicaFollower follower,
        string groupId,
        ulong index,
        CancellationToken cancellationToken)
    {
        var batch = Batch(Prepare($"pending-{index}", index), index - 1UL, index);
        var appended = await follower.AppendAsync(groupId, Fingerprint, 1, batch, cancellationToken);
        _ = await Assert.That(appended.Success).IsTrue().Because($"entry {index} was refused with '{appended.RefusalCode}'");
        return batch.Entries[0].Payload.Length;
    }

    private static async Task<(int Payloads, long Bytes)> ReadBacklogAsync(IFollowerLog log, CancellationToken cancellationToken)
    {
        var retention = await log.GetRetentionAsync(cancellationToken);
        return (retention.RetainedPayloads, retention.RetainedPayloadBytes);
    }
}
