using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.LedGroupsTestKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// The apply service applies the committed entries of every follower group to memory, wakes on the follower path's signal, and stops
/// only the group whose committed record cannot be applied. The fallback clock never advances, so only a signal can wake a loop.
/// </summary>
public sealed class ReplicaApplyServiceTests : ServerUnitTestBase
{
    private const int StoppedEventId = 4028;

    private const int RetryEventId = 4029;

    /// <summary>The event id of the passes a leadership skips.</summary>
    private const int PassSkippedEventId = 4047;

    private static readonly byte[] Fingerprint = [9, 8, 7];

    private static readonly string[] Groups = ["n1", "n2", "n3"];

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>How long a stop is given to end a service early while a group loop still applies an entry.</summary>
    private static readonly TimeSpan StopGrace = TimeSpan.FromMilliseconds(200);

    /// <summary>Every follower group applies its committed entries and nothing above its commit index.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppliesCommittedFollowerEntries(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-service-committed");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await SeedAsync(registry, "n2", 2UL, cancellationToken, "k1", "k2", "k3");
        await SeedAsync(registry, "n3", 1UL, cancellationToken, "m1");
        var cache = new StubCache();
        using var meter = new Meter("test");
        var appliers = CreateAppliers(registry, cache, meter);
        using var service = new ReplicaApplyService(
            registry,
            appliers,
            LeadOwnGroup(registry, appliers),
            ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(),
            NullLogger<ReplicaApplyService>.Instance,
            new FakeTimeProvider());
        var applied = WhenAppliedAsync(cache, 3);

        await service.StartAsync(cancellationToken);
        try
        {
            await applied.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That((appliers.For("n2").AppliedIndex, appliers.For("n3").AppliedIndex)).IsEqualTo((2UL, 1UL));
        var keys = cache.Applied.ToArray();
        Array.Sort(keys, StringComparer.Ordinal);
        await SequenceAssert.EqualAsync(["k1", "k2", "m1"], keys, StringComparer.Ordinal);
    }

    /// <summary>
    /// A group this node leads gets no apply loop: its committer is the one driver of its applier, so a committed entry of it stays for
    /// the committer while the other groups apply theirs.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ApplyServiceSkipsLedGroups(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-service-led");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await SeedAsync(registry, "n1", 1UL, cancellationToken, "o1");
        await SeedAsync(registry, "n3", 1UL, cancellationToken, "m1");
        var cache = new StubCache();
        using var meter = new Meter("test");
        var appliers = CreateAppliers(registry, cache, meter);
        var fallback = new FakeTimeProvider();
        using var service = new ReplicaApplyService(
            registry,
            appliers,
            LeadOwnGroup(registry, appliers),
            ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(),
            NullLogger<ReplicaApplyService>.Instance,
            fallback);
        var first = WhenAppliedAsync(cache, 1);

        await service.StartAsync(cancellationToken);
        try
        {
            await first.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            // A loop of the led group would wake on its signal or on the fallback pass; the next entry of n3 passes after both.
            registry.ApplySignalFor("n1").Notify();
            fallback.Advance(TimeSpan.FromSeconds(2));
            var second = WhenAppliedAsync(cache, 2);
            _ = await new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers()).AppendAsync("n3", Fingerprint, 1, Batch(Prepare("m2", 2UL), 1UL, 2UL), cancellationToken);
            await second.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        await SequenceAssert.EqualAsync(["m1", "m2"], cache.Applied.ToArray(), StringComparer.Ordinal);
        _ = await Assert.That((appliers.For("n1").AppliedIndex, appliers.For("n3").AppliedIndex)).IsEqualTo((0UL, 2UL));
    }

    /// <summary>
    /// While a leadership drives a group applier, the apply loop of the group skips its passes and says so once, so nothing is applied
    /// twice; once the leadership ends, the next pass applies the committed entries.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ApplyLoopSkipsPassWhileLeaseHeld(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-service-lease");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await SeedAsync(registry, "n2", 1UL, cancellationToken, "k1");
        await SeedAsync(registry, "n3", 1UL, cancellationToken, "m1");
        var cache = new StubCache();
        using var meter = new Meter("test");
        var appliers = CreateAppliers(registry, cache, meter);
        await appliers.For("n2").DriverLease.LeadAsync(cancellationToken);
        var log = new EventRecordingLogger();
        using var service = new ReplicaApplyService(
            registry,
            appliers,
            LeadOwnGroup(registry, appliers),
            ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(),
            log,
            new FakeTimeProvider());
        var other = WhenAppliedAsync(cache, 1);

        await service.StartAsync(cancellationToken);
        ulong whileHeld;
        try
        {
            await other.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            await log.WhenLoggedAsync(PassSkippedEventId, "group n2 ").WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            whileHeld = appliers.For("n2").AppliedIndex;
            var released = WhenAppliedAsync(cache, 2);
            appliers.For("n2").DriverLease.EndLeading();
            registry.ApplySignalFor("n2").Notify();
            await released.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That((whileHeld, appliers.For("n2").AppliedIndex)).IsEqualTo((0UL, 1UL));
        _ = await Assert.That(log.Count(PassSkippedEventId)).IsEqualTo(1);
        await SequenceAssert.EqualAsync(["m1", "k1"], cache.Applied.ToArray(), StringComparer.Ordinal);
    }

    /// <summary>An entry the follower path appends and commits wakes the apply loop of its group, with no fallback pass.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FollowerAppendWakesApplyLoop(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-service-signal");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await SeedAsync(registry, "n2", 1UL, cancellationToken, "k1");
        var cache = new StubCache();
        using var meter = new Meter("test");
        var appliers = CreateAppliers(registry, cache, meter);
        using var service = new ReplicaApplyService(
            registry,
            appliers,
            LeadOwnGroup(registry, appliers),
            ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(),
            NullLogger<ReplicaApplyService>.Instance,
            new FakeTimeProvider());
        var first = WhenAppliedAsync(cache, 1);

        await service.StartAsync(cancellationToken);
        try
        {
            await first.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            var second = WhenAppliedAsync(cache, 2);
            var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
            var appended = await follower.AppendAsync("n2", Fingerprint, 1, Batch(Prepare("k2", 2UL), 1UL, 2UL), cancellationToken);
            _ = await Assert.That(appended.Success).IsTrue().Because($"refused with '{appended.RefusalCode}'");
            await second.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        await SequenceAssert.EqualAsync(["k1", "k2"], cache.Applied.ToArray(), StringComparer.Ordinal);
        _ = await Assert.That(appliers.For("n2").AppliedIndex).IsEqualTo(2UL);
    }

    /// <summary>An inconsistent committed record stops only its group: it and every later entry stay unapplied, and the other group goes on.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InconsistentRecordStopsOnlyItsGroup(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-service-inconsistent");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await SeedAsync(registry, "n2", 2UL, [await InconsistentAsync(cancellationToken), Prepare("k2", 2UL)], cancellationToken);
        await SeedAsync(registry, "n3", 1UL, cancellationToken, "m1");
        var cache = new StubCache();
        var events = new EventRecordingLogger();
        using var meter = new Meter("test");
        var appliers = CreateAppliers(registry, cache, meter);
        using var service = new ReplicaApplyService(
            registry,
            appliers,
            LeadOwnGroup(registry, appliers),
            ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(),
            events,
            new FakeTimeProvider());
        var first = WhenAppliedAsync(cache, 1);

        await service.StartAsync(cancellationToken);
        try
        {
            await events.WhenLoggedAsync(StoppedEventId).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            await first.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            var second = WhenAppliedAsync(cache, 2);
            _ = await new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers()).AppendAsync("n3", Fingerprint, 1, Batch(Prepare("m2", 2UL), 1UL, 2UL), cancellationToken);
            await second.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            _ = await Assert.That(service.ExecuteTask?.IsCompleted).IsFalse().Because("A stopped group must not end the service.");
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        await SequenceAssert.EqualAsync(["m1", "m2"], cache.Applied.ToArray(), StringComparer.Ordinal);
        _ = await Assert.That(appliers.For("n2").AppliedIndex).IsEqualTo(0UL);
        _ = await Assert.That(events.Count(StoppedEventId)).IsEqualTo(1);
    }

    /// <summary>The loop rebuilds the outcomes of a follower group before its first catch-up and records the outcome of every entry it applies.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ApplyLoopKeepsFollowerOutcomes(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-service-outcomes");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var committed = Prepare("k1", 1UL);
        var appended = Prepare("k2", 2UL);
        await SeedAsync(registry, "n2", 1UL, [committed], cancellationToken);
        var cache = new StubCache();
        using var meter = new Meter("test");
        var appliers = CreateAppliers(registry, cache, meter);
        using var service = new ReplicaApplyService(
            registry,
            appliers,
            LeadOwnGroup(registry, appliers),
            ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(),
            NullLogger<ReplicaApplyService>.Instance,
            new FakeTimeProvider());
        var first = WhenAppliedAsync(cache, 1);

        await service.StartAsync(cancellationToken);
        try
        {
            await first.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            var second = WhenAppliedAsync(cache, 2);
            _ = await new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers()).AppendAsync("n2", Fingerprint, 1, Batch(in appended, 1UL, 2UL), cancellationToken);
            await second.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            // The stop waits for the pass in flight, whose applies are not canceled, so the outcome of the last entry is recorded.
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That(registry.TryGetLog("n2", out var log)).IsTrue();
        _ = await Assert.That(log!.Idempotency.OutcomesRebuilt).IsTrue();
        _ = await Assert.That(log.Idempotency.Lookup(committed.OperationScope, committed.OperationId, committed.OperationFingerprint.Span, out _))
                        .IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(log.Idempotency.Lookup(appended.OperationScope, appended.OperationId, appended.OperationFingerprint.Span, out _))
                        .IsEqualTo(GroupIdempotencyLookup.Found);
    }

    /// <summary>A fault outside the retried storage and journal faults ends the service with that fault.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnexpectedFaultFaultsTheService(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-service-fault");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await SeedAsync(registry, "n2", 1UL, cancellationToken, "k1");
        var cache = new ILogicalNamespacedCacheCreateExpectations<object?>();
        _ = cache.Setups.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<object?>>(), Arg.Any<CancellationToken>())
                 .Callback(static (_, _, _, _, _) => ValueTask.FromException(new NotSupportedException("unexpected cache failure")));
        using var meter = new Meter("test");
        var appliers = CreateAppliers(registry, cache.Instance(), meter);
        using var service = new ReplicaApplyService(
            registry,
            appliers,
            LeadOwnGroup(registry, appliers),
            ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(),
            NullLogger<ReplicaApplyService>.Instance,
            new FakeTimeProvider());

        await service.StartAsync(cancellationToken);
        try
        {
            _ = await NodeAsyncAssert.ThrowsAsync<NotSupportedException>(service.ExecuteTask!.WaitAsync(HangGuard, TimeProvider.System, cancellationToken));
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That(appliers.For("n2").AppliedIndex).IsEqualTo(0UL);
    }

    /// <summary>A full journal leaves the group pending instead of ending the service, and the next pass applies the entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FullJournalIsRetried(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-service-full-journal");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await SeedAsync(registry, "n2", 1UL, cancellationToken, "k1");
        var writes = 0;
        var applied = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cache = new ILogicalNamespacedCacheCreateExpectations<object?>();
        _ = cache.Setups.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<object?>>(), Arg.Any<CancellationToken>())
                 .Callback((_, _, _, _, _) =>
                  {
                      var write = Interlocked.Increment(ref writes);
                      if (write == 1)
                          return ValueTask.FromException(new JournalCapacityExceededException());

                      if (write == 3)
                          _ = applied.TrySetResult();

                      return ValueTask.CompletedTask;
                  });
        var events = new EventRecordingLogger();
        using var meter = new Meter("test");
        var appliers = CreateAppliers(registry, cache.Instance(), meter);
        using var service = new ReplicaApplyService(
            registry,
            appliers,
            LeadOwnGroup(registry, appliers),
            ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(),
            events,
            new FakeTimeProvider());

        await service.StartAsync(cancellationToken);
        try
        {
            await events.WhenLoggedAsync(RetryEventId).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            _ = await new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers()).AppendAsync("n2", Fingerprint, 1, Batch(Prepare("k2", 2UL), 1UL, 2UL), cancellationToken);
            await applied.Task.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            _ = await Assert.That(service.ExecuteTask?.IsCompleted).IsFalse().Because("A full journal must not end the service.");
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That(appliers.For("n2").AppliedIndex).IsEqualTo(2UL);
    }

    /// <summary>The stop waits for the loop of every group, including one still applying an entry after the others ended with the host.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StopWaitsForEveryGroupLoop(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-service-stop");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await SeedAsync(registry, "n3", 1UL, cancellationToken, "m1");
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cache = new ILogicalNamespacedCacheCreateExpectations<object?>();
        _ = cache.Setups.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<object?>>(), Arg.Any<CancellationToken>())
                 .Callback((_, _, _, _, _) =>
                  {
                      _ = entered.TrySetResult();
                      return new ValueTask(release.Task);
                  });
        using var meter = new Meter("test");
        var appliers = CreateAppliers(registry, cache.Instance(), meter);
        using var service = new ReplicaApplyService(
            registry,
            appliers,
            LeadOwnGroup(registry, appliers),
            ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(),
            NullLogger<ReplicaApplyService>.Instance,
            new FakeTimeProvider());

        await service.StartAsync(cancellationToken);
        Task? stopped = null;
        try
        {
            await entered.Task.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            // The apply of group n3 is not canceled, so the service cannot end before it is released, while the loop of group n2 ends at
            // once. The grace only gives a service that ended early the time to show it; a correct one never ends within it.
            stopped = service.StopAsync(cancellationToken);
            _ = await NodeAsyncAssert.ThrowsAsync<TimeoutException>(service.ExecuteTask!.WaitAsync(StopGrace, TimeProvider.System, cancellationToken));
        }
        finally
        {
            _ = release.TrySetResult();
            await (stopped ?? service.StopAsync(cancellationToken)).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }

        _ = await Assert.That(service.ExecuteTask.IsCompletedSuccessfully).IsTrue();
        _ = await Assert.That((appliers.For("n2").AppliedIndex, appliers.For("n3").AppliedIndex)).IsEqualTo((0UL, 1UL));
    }

    /// <summary>Leads the own group n1 with a committer driving the applier of that group.</summary>
    /// <param name="registry">The registry of node n1.</param>
    /// <param name="appliers">The appliers of the served groups.</param>
    /// <returns>The committers of the led groups.</returns>
    private static ReplicaGroupCommitters LeadOwnGroup(ReplicaGroupRegistry registry, ReplicaGroupAppliers appliers)
    {
        var led = new ReplicaGroupCommitter[1];
        led[0] = CreateGroupCommitter(registry, "n1", new ScriptedGateway(), new StubCache(), TimeProvider.System, appliers.For("n1"));
        return new ReplicaGroupCommitters(led, "n1", Owners(), TimeProvider.System);
    }

    private static ReplicaGroupAppliers CreateAppliers(ReplicaGroupRegistry registry, ILogicalNamespacedCache<object?> cache, Meter meter) => new(
        registry,
        cache,
        "n1",
        NullLogger<ReplicaGroupAppliers>.Instance,
        new ReplicationMetrics(meter));

    /// <summary>Returns a task that completes once the cache applied at least <paramref name="count" /> writes.</summary>
    /// <param name="cache">The local cache double.</param>
    /// <param name="count">The number of applied writes to wait for.</param>
    /// <returns>The task completing on the write that reaches the count.</returns>
    private static Task WhenAppliedAsync(StubCache cache, int count)
    {
        var reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        cache.OnApplied = () =>
        {
            if (cache.Applied.Count >= count)
                _ = reached.TrySetResult();
        };
        if (cache.Applied.Count >= count)
            _ = reached.TrySetResult();

        return reached.Task;
    }

    /// <summary>Prepares a record that sets <paramref name="key" /> at <paramref name="logIndex" />.</summary>
    /// <param name="key">The key the record writes.</param>
    /// <param name="logIndex">The log index of the record.</param>
    /// <returns>The decoded record.</returns>
    /// <exception cref="InvalidOperationException">The prepared record does not decode.</exception>
    private static ReplicaLogRecord Prepare(string key, ulong logIndex)
    {
        var factory = new ReplicaMutationFactory(new StubCache(), "n1", 1UL, TimeProvider.System, NullLogger.Instance);
        var prepared = factory.PrepareSet(NewOperationId(), "cache", key, Entry(key), logIndex);
        return ReplicaLogCodec.Decode(prepared.CanonicalPayload) ?? ThrowHelper.Throw<ReplicaLogRecord>(new InvalidOperationException("The prepared record does not decode."));
    }

    /// <summary>Prepares a record that writes an entry while its outcome says the add was rejected.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The inconsistent record at log index 1.</returns>
    /// <exception cref="InvalidOperationException">The prepared record does not decode.</exception>
    private static async Task<ReplicaLogRecord> InconsistentAsync(CancellationToken cancellationToken)
    {
        var factory = new ReplicaMutationFactory(new StubCache(), "n1", 1UL, TimeProvider.System, NullLogger.Instance);
        var prepared = await factory.PrepareTryAddAsync(NewOperationId(), "cache", "bad", Entry("bad"), 1UL, cancellationToken);
        var record = ReplicaLogCodec.Decode(prepared.CanonicalPayload) ??
                     ThrowHelper.Throw<ReplicaLogRecord>(new InvalidOperationException("The prepared record does not decode."));
        return record with { OutcomePayload = ReplicaOutcomeCodec.Encode(false, ReadOnlyMemory<byte>.Empty) };
    }

    private static FollowerBatch Batch(in ReplicaLogRecord record, ulong prevLogIndex, ulong leaderCommitIndex) => new(
        [record],
        "n2",
        1,
        prevLogIndex,
        prevLogIndex == 0 ? 0UL : 1UL,
        leaderCommitIndex);

    private static Task SeedAsync(ReplicaGroupRegistry registry, string groupId, ulong commitIndex, CancellationToken cancellationToken, params string[] keys)
    {
        var records = new ReplicaLogRecord[keys.Length];
        for (var i = 0; i < keys.Length; i++)
            records[i] = Prepare(keys[i], ulong.CreateChecked(i + 1));

        return SeedAsync(registry, groupId, commitIndex, records, cancellationToken);
    }

    /// <summary>Appends records to a follower group log from index 1 and commits them through <paramref name="commitIndex" />, without applying them.</summary>
    /// <param name="registry">The open registry.</param>
    /// <param name="groupId">The follower group.</param>
    /// <param name="commitIndex">The commit index after the append.</param>
    /// <param name="records">The records, in log order from index 1.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The group log is not open or refused the records.</exception>
    private static async Task SeedAsync(ReplicaGroupRegistry registry, string groupId, ulong commitIndex, ReplicaLogRecord[] records, CancellationToken cancellationToken)
    {
        if (!registry.TryGetLog(groupId, out var log))
            throw new InvalidOperationException($"The group log {groupId} is not open.");

        var entries = new FollowerLogEntry[records.Length];
        for (var i = 0; i < records.Length; i++)
        {
            var logIndex = ulong.CreateChecked(i + 1);
            var positioned = records[i] with { LogIndex = logIndex, Term = 1 };
            entries[i] = new FollowerLogEntry(logIndex, 1, ReplicaLogCodec.Encode(in positioned));
        }

        var appended = await log.AppendAsync(new FollowerLogAppendRequest(groupId, 1, 0, 0, commitIndex, entries), cancellationToken);
        if (!appended.Success)
            throw new InvalidOperationException($"The group log {groupId} refused the records: {appended.RefusalCode}.");
    }
}
