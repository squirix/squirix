using System;
using System.Diagnostics.Metrics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Rocks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.LedGroupsTestKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// A maintenance pass persists the applied index of every follower group behind the cache journal durability barrier and compacts the
/// group log through it.
/// </summary>
public sealed class ReplicaLogCompactionServiceTests : ServerUnitTestBase
{
    /// <summary>After the owned log, the pass persists each follower group's applied index and releases the applied payloads from memory.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FollowerFlushReleasesPayloads(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-maintenance-follower-flush");
        await using var registry = await OpenRegistryAsync(dir, ["n1", "n2", "n3"], null, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway());
        using var meter = new Meter("test");
        var metrics = new ReplicationMetrics(meter);
        var appliers = new ReplicaGroupAppliers(registry, new StubCache(), "n1", NullLogger<ReplicaGroupAppliers>.Instance, metrics);
        var log = await SeedAsync(registry, "n2", 3, 3UL, cancellationToken);
        _ = await ReplicaOutcomeRecovery.RestoreAsync(log, TimeProvider.System, cancellationToken);
        await appliers.For("n2").CatchUpAsync(log, 0UL, 3UL, cancellationToken);
        var barriers = new int[1];
        var durability = new IJournalDurabilityCoordinatorCreateExpectations();
        _ = durability.Setups.AwaitDurabilityCommitAsync(Arg.Any<CancellationToken>())
                      .Callback(token =>
                      {
                          _ = Interlocked.Increment(ref barriers[0]);
                          return token.IsCancellationRequested ? ValueTask.FromCanceled(token) : ValueTask.CompletedTask;
                      });
        using var service = new ReplicaLogCompactionService(
            LeadOwn(committer),
            durability.Instance(),
            new ReplicaLogCompactionOptions(),
            new ReplicaLogCompactionPolicy(long.MaxValue, int.MaxValue),
            metrics,
            appliers,
            registry,
            NullLogger<ReplicaLogCompactionService>.Instance,
            TimeProvider.System);

        await service.RunOnceAsync(cancellationToken);

        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((3UL, 3UL));
        _ = await Assert.That((await log.GetCommittedEntriesAsync(0UL, 10, cancellationToken)).Count).IsEqualTo(0);
        _ = await Assert.That(Volatile.Read(ref barriers[0])).IsEqualTo(1).Because("Only the follower group with applied entries awaits the barrier.");
    }

    /// <summary>
    /// Once a follower group log reaches the threshold, the pass compacts it through the applied index it just persisted and keeps the
    /// entry the group has not committed yet.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FollowerPassCompactsGroupLog(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-maintenance-follower-compact");
        await using var registry = await OpenRegistryAsync(dir, ["n1", "n2", "n3"], null, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway());
        using var meter = new Meter("test");
        var metrics = new ReplicationMetrics(meter);
        var appliers = new ReplicaGroupAppliers(registry, new StubCache(), "n1", NullLogger<ReplicaGroupAppliers>.Instance, metrics);
        var log = await SeedAsync(registry, "n2", 4, 3UL, cancellationToken);
        _ = await ReplicaOutcomeRecovery.RestoreAsync(log, TimeProvider.System, cancellationToken);
        await appliers.For("n2").CatchUpAsync(log, 0UL, 3UL, cancellationToken);
        var durability = new IJournalDurabilityCoordinatorCreateExpectations();
        _ = durability.Setups.AwaitDurabilityCommitAsync(Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        using var service = new ReplicaLogCompactionService(
            LeadOwn(committer),
            durability.Instance(),
            new ReplicaLogCompactionOptions(),
            new ReplicaLogCompactionPolicy(long.MaxValue, 2),
            metrics,
            appliers,
            registry,
            NullLogger<ReplicaLogCompactionService>.Instance,
            TimeProvider.System);

        await service.RunOnceAsync(cancellationToken);

        var status = await log.GetStatusAsync(cancellationToken);
        var retention = await log.GetRetentionAsync(cancellationToken);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((4UL, 3UL, 3UL));
        _ = await Assert.That((retention.SnapshotIndex, retention.RetainedEntries)).IsEqualTo((3UL, 1));
    }

    /// <summary>A group whose applier lease is held by a committer leading it is skipped by the follower pass: its log is neither flushed nor compacted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FollowerPassSkipsLeasedGroup(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-maintenance-follower-leased");
        await using var registry = await OpenRegistryAsync(dir, ["n1", "n2", "n3"], null, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway());
        using var meter = new Meter("test");
        var metrics = new ReplicationMetrics(meter);
        var appliers = new ReplicaGroupAppliers(registry, new StubCache(), "n1", NullLogger<ReplicaGroupAppliers>.Instance, metrics);
        var log = await SeedAsync(registry, "n2", 4, 3UL, cancellationToken);
        _ = await ReplicaOutcomeRecovery.RestoreAsync(log, TimeProvider.System, cancellationToken);
        await appliers.For("n2").CatchUpAsync(log, 0UL, 3UL, cancellationToken);
        var durability = new IJournalDurabilityCoordinatorCreateExpectations();
        _ = durability.Setups.AwaitDurabilityCommitAsync(Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        using var service = new ReplicaLogCompactionService(
            LeadOwn(committer),
            durability.Instance(),
            new ReplicaLogCompactionOptions(),
            new ReplicaLogCompactionPolicy(long.MaxValue, 2),
            metrics,
            appliers,
            registry,
            NullLogger<ReplicaLogCompactionService>.Instance,
            TimeProvider.System);
        _ = appliers.For("n2").DriverLease.TryLock(out var lease);

        await service.RunOnceAsync(cancellationToken);
        var whileHeld = (await log.GetStatusAsync(cancellationToken)).LastAppliedIndex;
        lease.Dispose();
        await service.RunOnceAsync(cancellationToken);

        var status = await log.GetStatusAsync(cancellationToken);
        var retention = await log.GetRetentionAsync(cancellationToken);
        _ = await Assert.That((whileHeld, status.LastAppliedIndex, retention.SnapshotIndex)).IsEqualTo((0UL, 3UL, 3UL));
    }

    /// <summary>
    /// A follower group log is not compacted before its applier rebuilt the outcomes of the applied entries, which the snapshot would
    /// otherwise lose with their frames; after the rebuild it is.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FollowerCompactionWaitsForRebuild(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-maintenance-follower-rebuild");
        await using var registry = await OpenRegistryAsync(dir, ["n1", "n2", "n3"], null, cancellationToken);
        var log = await SeedAsync(registry, "n2", 4, 3UL, cancellationToken);
        _ = await log.AdvanceAppliedAsync(3UL, cancellationToken);
        var policy = new ReplicaLogCompactionPolicy(long.MaxValue, 2);

        var waiting = await ReplicaLogCompactionStep.RunFollowerAsync(log, policy, cancellationToken);
        var before = await log.GetRetentionAsync(cancellationToken);
        _ = await ReplicaOutcomeRecovery.RestoreAsync(log, TimeProvider.System, cancellationToken);
        var compacted = await ReplicaLogCompactionStep.RunFollowerAsync(log, policy, cancellationToken);
        var after = await log.GetRetentionAsync(cancellationToken);

        _ = await Assert.That((waiting, before.SnapshotIndex)).IsEqualTo((ReplicaLogCompactionOutcome.PendingApply, 0UL));
        _ = await Assert.That((compacted, after.SnapshotIndex, after.RetainedEntries)).IsEqualTo((ReplicaLogCompactionOutcome.Compacted, 3UL, 1));
    }

    /// <summary>A follower group whose maintenance keeps failing the same way is logged once, and again after a pass succeeded in between.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RepeatedFollowerFaultLogsOnce(CancellationToken cancellationToken)
    {
        const int retryEventId = 4011;
        using var dir = new TempDirectory("squirix-maintenance-follower-fault");
        await using var registry = await OpenRegistryAsync(dir, ["n1", "n2", "n3"], null, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway());
        using var meter = new Meter("test");
        var metrics = new ReplicationMetrics(meter);
        var appliers = new ReplicaGroupAppliers(registry, new StubCache(), "n1", NullLogger<ReplicaGroupAppliers>.Instance, metrics);
        var log = await SeedAsync(registry, "n2", 3, 3UL, cancellationToken);
        _ = await ReplicaOutcomeRecovery.RestoreAsync(log, TimeProvider.System, cancellationToken);
        var failing = new[] { true };
        var durability = new IJournalDurabilityCoordinatorCreateExpectations();
        _ = durability.Setups.AwaitDurabilityCommitAsync(Arg.Any<CancellationToken>())
                      .Callback(_ => Volatile.Read(ref failing[0]) ? ValueTask.FromException(new IOException("journal unavailable")) : ValueTask.CompletedTask);
        var events = new EventRecordingLogger();
        using var service = new ReplicaLogCompactionService(
            LeadOwn(committer),
            durability.Instance(),
            new ReplicaLogCompactionOptions(),
            new ReplicaLogCompactionPolicy(long.MaxValue, int.MaxValue),
            metrics,
            appliers,
            registry,
            events,
            TimeProvider.System);

        await appliers.For("n2").CatchUpAsync(log, 0UL, 2UL, cancellationToken);
        await service.RunOnceAsync(cancellationToken);
        await service.RunOnceAsync(cancellationToken);
        var repeated = events.Count(retryEventId);
        Volatile.Write(ref failing[0], false);
        await service.RunOnceAsync(cancellationToken);
        Volatile.Write(ref failing[0], true);
        await appliers.For("n2").CatchUpAsync(log, 2UL, 3UL, cancellationToken);
        await service.RunOnceAsync(cancellationToken);

        _ = await Assert.That((repeated, events.Count(retryEventId))).IsEqualTo((1, 2));
    }

    private static async Task<IFollowerLog> SeedAsync(ReplicaGroupRegistry registry, string groupId, int count, ulong commit, CancellationToken cancellationToken)
    {
        if (!registry.TryGetLog(groupId, out var log))
            throw new InvalidOperationException($"The group log {groupId} is not open.");

        var factory = new ReplicaMutationFactory(new StubCache(), "n1", 1UL, TimeProvider.System, NullLogger.Instance);
        var entries = new FollowerLogEntry[count];
        for (var i = 0; i < count; i++)
        {
            var logIndex = ulong.CreateChecked(i + 1);
            var key = $"k{i + 1}";
            entries[i] = new FollowerLogEntry(logIndex, 1UL, factory.PrepareSet(NewOperationId(), "cache", key, Entry(key), logIndex).CanonicalPayload);
        }

        var appended = await log.AppendAsync(new FollowerLogAppendRequest(groupId, 1UL, 0UL, 0UL, commit, entries), cancellationToken);
        return appended.Success ? log : throw new InvalidOperationException($"The group log {groupId} refused the entries: {appended.RefusalCode}.");
    }
}
