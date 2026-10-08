using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.LedGroupsTestKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// Node n1 leads two replica groups at once, n1 from slot 0 and n2 from slot 2: each group commits, stalls, keeps its outcomes, regains
/// its write quorum, and compacts on its own.
/// </summary>
public sealed class LedGroupsTests : ServerUnitTestBase
{
    private const string CacheName = "cache";

    /// <summary>The event id of a group whose every replica slot is verified.</summary>
    private const int VerificationCompleteEventId = 4002;

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Each group appends its write at index 1 of its own log and sends it to its own followers, n2 and n3, as leader n1; nothing is sent to
    /// node n1 itself.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TwoLedGroupsCommitIndependently(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-led-groups-independent");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var ofN1 = new ScriptedGateway();
        var ofN2 = new ScriptedGateway();
        await using var committers = LeadTwo(registry, (ofN1, ofN2), new StubCache(), TimeProvider.System);

        await committers.ForKey(CacheName, "a").CommitSetAsync(NewOperationId(), CacheName, "a", Entry("a"), cancellationToken);
        await committers.ForKey(CacheName, "b").CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken);

        // A commit returns once one follower holds the entry: wait until the slower follower of each group got it as well.
        await Task.WhenAll(ofN1.SentAsync("n2"), ofN1.SentAsync("n3"), ofN2.SentAsync("n2"), ofN2.SentAsync("n3")).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        _ = await Assert.That(await PositionAsync(registry, "n1", cancellationToken)).IsEqualTo((1UL, 1UL));
        _ = await Assert.That(await PositionAsync(registry, "n2", cancellationToken)).IsEqualTo((1UL, 1UL));
        await AssertSentAsync(ofN1, "n1");
        await AssertSentAsync(ofN2, "n2");
    }

    /// <summary>A group whose followers stopped answering holds its own write only: the other group commits meanwhile.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StalledGroupDoesNotBlockOtherGroup(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-led-groups-stalled");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var cache = new StubCache();
        var stalledGateway = new ScriptedGateway();
        stalledGateway.Set("n2", FollowerMode.Silent);
        stalledGateway.Set("n3", FollowerMode.Silent);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        stalledGateway.OnAppend = () => _ = entered.TrySetResult();
        var budgetClock = new FakeTimeProvider();
        var led = new ReplicaGroupCommitter[2];
        led[0] = CreateGroupCommitter(registry, "n1", new ScriptedGateway(), cache, TimeProvider.System);
        led[1] = CreateGroupCommitter(registry, "n2", stalledGateway, cache, TimeProvider.System, seams: (budgetClock, null));
        await using var committers = new ReplicaGroupCommitters(led, "n1", Owners(), TimeProvider.System);

        // The budget of the stalled write runs on a clock nobody moves until the other group has committed.
        var stalled = committers.For("n2").CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken);
        await entered.Task.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        await committers.For("n1").CommitSetAsync(NewOperationId(), CacheName, "a", Entry("a"), cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        var heldMeanwhile = stalled.IsCompleted;
        budgetClock.Advance(TimeSpan.FromSeconds(5));
        var error = await NodeAsyncAssert.ThrowsAsync<SquirixException>(stalled.WaitAsync(HangGuard, TimeProvider.System, cancellationToken));

        _ = await Assert.That(heldMeanwhile).IsFalse();
        _ = await Assert.That(error.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        _ = await Assert.That(await PositionAsync(registry, "n1", cancellationToken)).IsEqualTo((1UL, 1UL));
        _ = await Assert.That(await PositionAsync(registry, "n2", cancellationToken)).IsEqualTo((1UL, 0UL));
    }

    /// <summary>
    /// The same client operation identifier commits once in each group, as the groups keep their outcomes apart; after a restart a retry
    /// replays the outcome of its own group without appending anything.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OutcomeLookupIsPerGroup(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-led-groups-outcomes");
        var operationId = NewOperationId();
        await using (var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken))
        {
            await using var committers = LeadTwo(registry, (new ScriptedGateway(), new ScriptedGateway()), new StubCache(), TimeProvider.System);
            _ = await Assert.That(await committers.For("n1").CommitTryAddAsync(operationId, CacheName, "a", Entry("a"), cancellationToken)).IsTrue();
            _ = await Assert.That(await committers.For("n2").CommitTryAddAsync(operationId, CacheName, "b", Entry("b"), cancellationToken)).IsTrue();
        }

        await using var reopened = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await using var restarted = LeadTwo(reopened, (new ScriptedGateway(), new ScriptedGateway()), new StubCache(), TimeProvider.System);

        // Memory regains both keys from the logs, so an add that ran again would decide false and append a second entry.
        _ = await Assert.That(await restarted.For("n1").CommitTryAddAsync(operationId, CacheName, "a", Entry("a"), cancellationToken)).IsTrue();
        _ = await Assert.That(await restarted.For("n2").CommitTryAddAsync(operationId, CacheName, "b", Entry("b"), cancellationToken)).IsTrue();
        _ = await Assert.That(await PositionAsync(reopened, "n1", cancellationToken)).IsEqualTo((1UL, 1UL));
        _ = await Assert.That(await PositionAsync(reopened, "n2", cancellationToken)).IsEqualTo((1UL, 1UL));
    }

    /// <summary>After a restart the readiness service verifies the replica slots of every led group, so each regains its write quorum.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadinessVerifiesEveryLedGroup(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-led-groups-readiness");
        await using (var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken))
        {
            await using var committers = LeadTwo(registry, (new ScriptedGateway(), new ScriptedGateway()), new StubCache(), TimeProvider.System);
            await committers.For("n1").CommitSetAsync(NewOperationId(), CacheName, "a", Entry("a"), cancellationToken);
            await committers.For("n2").CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken);
        }

        await using var reopened = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var recovering = (reopened.EligibilityFor("n1").AllCanCountInWriteQuorum(), reopened.EligibilityFor("n2").AllCanCountInWriteQuorum());
        await using var restarted = LeadTwo(reopened, (new ScriptedGateway(), new ScriptedGateway()), new StubCache(), TimeProvider.System);
        var log = new EventRecordingLogger();
        using var service = new ReplicaGroupReadinessService(restarted, log, TimeProvider.System);

        await service.StartAsync(cancellationToken);
        try
        {
            await log.WhenLoggedAsync(VerificationCompleteEventId, "group n1 ").WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            await log.WhenLoggedAsync(VerificationCompleteEventId, "group n2 ").WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That(service.ExecuteTask?.IsCompletedSuccessfully).IsTrue().Because("A host stop ends every group loop normally.");
        _ = await Assert.That(recovering).IsEqualTo((false, false));
        _ = await Assert.That(reopened.EligibilityFor("n1").AllCanCountInWriteQuorum()).IsTrue();
        _ = await Assert.That(reopened.EligibilityFor("n2").AllCanCountInWriteQuorum()).IsTrue();
    }

    /// <summary>A loop that fails outside the retried faults stops the loop of the other group, and the service ends with its fault.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LoopFaultStopsReadiness(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-led-groups-readiness-fault");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var cache = new StubCache();
        var failed = ReplicaCommitterDoubles.RecoveryLifecycle.Failed(new NotSupportedException("unexpected recovery failure"));
        var led = new ReplicaGroupCommitter[2];
        led[0] = CreateGroupCommitter(registry, "n1", new ScriptedGateway(), cache, TimeProvider.System, seams: (null, failed));
        led[1] = CreateGroupCommitter(registry, "n2", new ScriptedGateway(), cache, TimeProvider.System);
        await using var committers = new ReplicaGroupCommitters(led, "n1", Owners(), TimeProvider.System);
        using var service = new ReplicaGroupReadinessService(committers, new EventRecordingLogger(), TimeProvider.System);

        await service.StartAsync(cancellationToken);
        try
        {
            // The service ends only once the loop of n2, which never fails on its own, has ended too.
            _ = await NodeAsyncAssert.ThrowsAsync<NotSupportedException>(service.ExecuteTask!.WaitAsync(HangGuard, TimeProvider.System, cancellationToken));
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }
    }

    /// <summary>One maintenance pass compacts the log of every led group through its commit index, and leaves the followed group alone.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactionCompactsEveryLedGroup(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-led-groups-compaction");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var cache = new StubCache();
        using var meter = new Meter("test");
        var metrics = new ReplicationMetrics(meter);
        var appliers = new ReplicaGroupAppliers(registry, cache, "n1", NullLogger<ReplicaGroupAppliers>.Instance, metrics);
        await using var committers = LeadTwo(registry, (new ScriptedGateway(), new ScriptedGateway()), cache, TimeProvider.System, appliers);
        await committers.For("n1").CommitSetAsync(NewOperationId(), CacheName, "a", Entry("a"), cancellationToken);
        await committers.For("n1").CommitSetAsync(NewOperationId(), CacheName, "a", Entry("a"), cancellationToken);
        await committers.For("n2").CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken);
        await committers.For("n2").CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken);
        var durability = new IJournalDurabilityCoordinatorCreateExpectations();
        _ = durability.Setups.AwaitDurabilityCommitAsync(Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        using var service = new ReplicaLogCompactionService(
            committers,
            durability.Instance(),
            (new ReplicaLogCompactionOptions(), new ReplicaLogCompactionPolicy(long.MaxValue, 1)),
            metrics,
            (appliers, registry),
            NullLogger<ReplicaLogCompactionService>.Instance,
            TimeProvider.System);

        await service.RunOnceAsync(cancellationToken);

        _ = await Assert.That(await RetainedAsync(registry, "n1", cancellationToken)).IsEqualTo((2UL, 0));
        _ = await Assert.That(await RetainedAsync(registry, "n2", cancellationToken)).IsEqualTo((2UL, 0));
        _ = await Assert.That(await RetainedAsync(registry, "n3", cancellationToken)).IsEqualTo((0UL, 0));
    }

    /// <summary>Asserts that every batch with entries a group sent went to its followers n2 and n3, once each, from leader n1.</summary>
    /// <param name="gateway">The follower transport of the group.</param>
    /// <param name="groupId">The group.</param>
    /// <returns>An asynchronous operation.</returns>
    private static async Task AssertSentAsync(ScriptedGateway gateway, string groupId)
    {
        var sent = (N2: 0, N3: 0);
        foreach (var (node, header) in gateway.AppendHeaders)
        {
            _ = await Assert.That((header.GroupId, header.LeaderNodeId, header.SenderNodeId)).IsEqualTo((groupId, "n1", "n1"));
            _ = await Assert.That(node).IsNotEqualTo("n1");
            sent = string.Equals(node, "n2", StringComparison.Ordinal) ? (sent.N2 + 1, sent.N3) : (sent.N2, sent.N3 + 1);
        }

        _ = await Assert.That(sent).IsEqualTo((1, 1));
    }

    private static IFollowerLog LogOf(ReplicaGroupRegistry registry, string groupId) =>
        registry.TryGetLog(groupId, out var log) ? log : throw new InvalidOperationException($"The group log {groupId} is not open.");

    /// <summary>Reads the last index and the commit index of a group log.</summary>
    /// <param name="registry">The registry serving the group.</param>
    /// <param name="groupId">The group.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The last log index and the commit index.</returns>
    private static async Task<(ulong Last, ulong Commit)> PositionAsync(ReplicaGroupRegistry registry, string groupId, CancellationToken cancellationToken)
    {
        var status = await LogOf(registry, groupId).GetStatusAsync(cancellationToken);
        return (status.LastLogIndex, status.CommitIndex);
    }

    /// <summary>Reads the snapshot index and the number of retained entries of a group log.</summary>
    /// <param name="registry">The registry serving the group.</param>
    /// <param name="groupId">The group.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The snapshot index and the retained entry count.</returns>
    private static async Task<(ulong SnapshotIndex, int Retained)> RetainedAsync(ReplicaGroupRegistry registry, string groupId, CancellationToken cancellationToken)
    {
        var retention = await LogOf(registry, groupId).GetRetentionAsync(cancellationToken);
        return (retention.SnapshotIndex, retention.RetainedEntries);
    }
}
