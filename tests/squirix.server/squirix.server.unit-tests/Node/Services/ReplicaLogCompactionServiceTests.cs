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
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>A maintenance pass persists the applied index of every follower group behind the cache journal durability barrier.</summary>
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
        var appliers = new ReplicaFollowerAppliers(registry, new StubCache(), "n1", NullLogger<ReplicaFollowerAppliers>.Instance, metrics);
        var log = await SeedAsync(registry, "n2", 3, cancellationToken);
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
            committer,
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
        var appliers = new ReplicaFollowerAppliers(registry, new StubCache(), "n1", NullLogger<ReplicaFollowerAppliers>.Instance, metrics);
        var log = await SeedAsync(registry, "n2", 3, cancellationToken);
        var failing = new[] { true };
        var durability = new IJournalDurabilityCoordinatorCreateExpectations();
        _ = durability.Setups.AwaitDurabilityCommitAsync(Arg.Any<CancellationToken>())
                      .Callback(_ => Volatile.Read(ref failing[0]) ? ValueTask.FromException(new IOException("journal unavailable")) : ValueTask.CompletedTask);
        var events = new EventRecordingLogger();
        using var service = new ReplicaLogCompactionService(
            committer,
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

    private static async Task<IFollowerLog> SeedAsync(ReplicaGroupRegistry registry, string groupId, int count, CancellationToken cancellationToken)
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

        var appended = await log.AppendAsync(new FollowerLogAppendRequest(groupId, 1UL, 0UL, 0UL, ulong.CreateChecked(count), entries), cancellationToken);
        return appended.Success ? log : throw new InvalidOperationException($"The group log {groupId} refused the entries: {appended.RefusalCode}.");
    }
}
