using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The stream of a state transfer: the messages it sends, how it batches the entries, how it ends on a refusal, a fault, a timeout and a cancellation.</summary>
[Immutable]
public sealed class ReplicaTransferStreamTests : ServerUnitTestBase
{
    private const string CacheName = "cache";
    private const string GroupId = TransferFixtures.GroupId;
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>A transfer is a begin, the snapshot, wipe steps while the receiver asks for more, the entries, and an end with the count and the checksum of what was sent.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MessagesFollowTheProtocol(CancellationToken cancellationToken)
    {
        var (_, walk, clock) = await LeaderAsync(3, cancellationToken);
        var gateway = new ScriptedTransferGateway();
        gateway.SetWipeSteps(2);
        var stream = StreamTo(gateway, clock);

        var result = await stream.RunAsync(Snapshot(), walk, cancellationToken);

        _ = await Assert.That(result).IsEqualTo(new ReplicaTransferStream.Result(ReplicaTransferOutcome.Transferred, string.Empty));
        _ = await Assert.That(string.Join(',', gateway.Calls)).IsEqualTo("begin,snapshot,wipe,wipe,wipe,entries,end");
        _ = await Assert.That(string.Join(',', gateway.Sequences)).IsEqualTo("1,2,3,4,5,6");
        _ = await Assert.That(gateway.TransferIdCount).IsEqualTo(1);
        var totals = ReplicaStateHarness.TotalsOf([.. AllEntries(gateway)]);
        _ = await Assert.That(gateway.End).IsEqualTo((totals.EntryCount, totals.Crc));
        _ = await Assert.That((stream.Entries, totals.EntryCount)).IsEqualTo((3UL, 3UL));
    }

    /// <summary>A batch carries at most 512 entries however small they are.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BatchesStopAtTheEntryCap(CancellationToken cancellationToken)
    {
        var (_, walk, clock) = await LeaderAsync(513, cancellationToken);
        var gateway = new ScriptedTransferGateway();

        _ = await StreamTo(gateway, clock).RunAsync(Snapshot(), walk, cancellationToken);

        _ = await Assert.That(string.Join(',', gateway.Batches)).IsEqualTo("512,1");
    }

    /// <summary>An entry larger than the message budget travels alone, and no other batch passes the budget.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OversizedEntryTravelsAlone(CancellationToken cancellationToken)
    {
        var (cache, walk, clock) = await LeaderAsync(2, cancellationToken);
        await cache.SetAsync(new CacheKey(CacheName, "big"), new NodeCacheEntry<object?>(new string('x', ReplicaTransferOptions.MessageBudgetBytes + 1024)), cancellationToken);
        var gateway = new ScriptedTransferGateway();

        var result = await StreamTo(gateway, clock).RunAsync(Snapshot(), walk, cancellationToken);

        _ = await Assert.That(result.Outcome).IsEqualTo(ReplicaTransferOutcome.Transferred);
        var entries = 0;
        var alone = 0;
        var othersWithinBudget = true;
        foreach (var batch in CollectionsMarshal.AsSpan(gateway.Sent))
        {
            long bytes = 0;
            foreach (var entry in batch)
                bytes += ReplicaTransferBatch.SizeOf(entry.CacheName, System.Text.Encoding.UTF8.GetString(entry.Key.Span), entry.Payload.Length);

            entries += batch.Length;
            if (batch.Length == 1 && bytes > ReplicaTransferOptions.MessageBudgetBytes)
                alone++;
            else
                othersWithinBudget &= bytes <= ReplicaTransferOptions.MessageBudgetBytes;
        }

        _ = await Assert.That((entries, alone, othersWithinBudget)).IsEqualTo((3, 1, true));
    }

    /// <summary>The buffers of a message stay unchanged until the call returns, and a deadline travels as absolute ticks.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BuffersStayUnchangedUntilTheCallReturns(CancellationToken cancellationToken)
    {
        var (cache, walk, clock) = await LeaderAsync(0, cancellationToken);
        var deadline = clock.GetUtcNow().UtcDateTime.AddHours(1);
        await cache.SetAsync(new CacheKey(CacheName, "key-1"), new NodeCacheEntry<object?>("v1", 1, deadline), cancellationToken);
        var gateway = new ScriptedTransferGateway { HoldAt = "entries" };
        byte[]? before = null;
        gateway.OnEntries = entries => before = [.. entries.Span[0].Key.Span, .. entries.Span[0].Payload.Span];
        var run = StreamTo(gateway, clock).RunAsync(Snapshot(), walk, cancellationToken);
        await gateway.Held.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        var held = gateway.Sent[0];
        byte[] during = [.. held[0].Key.Span, .. held[0].Payload.Span];
        gateway.Release();
        _ = await run;

        _ = await Assert.That(before).IsNotNull();
        _ = await Assert.That(Convert.ToHexString(during)).IsEqualTo(Convert.ToHexString(before!));
        _ = await Assert.That(held[0].ExpiresUtcTicks).IsEqualTo(deadline.Ticks);
    }

    /// <summary>Both ends real: the stream feeds the receiver, which counts and checksums the same entries, wipes the old keys and completes the copy at the snapshot index.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RealReceiverCompletesTheCopy(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-stream-receiver");
        await using var harness = await ReplicaStateHarness.OpenAsync(dir, cancellationToken, new ReplicaTransferOptions { WipeStepKeys = 1 });
        await harness.SeedAsync(cancellationToken, "stale-1", "stale-2", "other-1");
        var (_, walk, _) = await LeaderAsync(3, cancellationToken);
        var gateway = new ReceiverGateway(harness.Receiver);
        var stream = new ReplicaTransferStream(gateway, new ReplicaTransferOptions(), Header(), "n1", harness.Clock);

        var result = await stream.RunAsync(Snapshot(), walk, cancellationToken);

        _ = await Assert.That(result).IsEqualTo(new ReplicaTransferStream.Result(ReplicaTransferOutcome.Transferred, string.Empty));
        _ = harness.Registry.TryGetLog(GroupId, out var log);
        var status = await log!.GetStatusAsync(cancellationToken);
        _ = await Assert.That((log.IsComplete, status.LastAppliedIndex)).IsEqualTo((true, TransferFixtures.SnapshotIndex));
        _ = await Assert.That(await harness.Cache.GetEntryAsync(new CacheKey(CacheName, "stale-1"), cancellationToken)).IsNull();
        _ = await Assert.That(await harness.Cache.GetEntryAsync(new CacheKey(CacheName, "other-1"), cancellationToken)).IsNotNull();
        for (var i = 0; i < 3; i++)
            _ = await Assert.That(await harness.Cache.GetEntryAsync(new CacheKey(CacheName, $"key-{i}"), cancellationToken)).IsNotNull();
    }

    /// <summary>Any refusal ends the stream with its code and sends no abort; a reply of a higher term ends it as superseded.</summary>
    /// <param name="code">The refusal code.</param>
    /// <param name="term">The term of the reply.</param>
    /// <param name="expected">The name of the outcome.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments("transfer-busy", 1UL, "follower_refused")]
    [Arguments("journal-full", 1UL, "follower_refused")]
    [Arguments("stale-term", 5UL, "superseded")]
    public async Task RefusalEndsWithoutAbort(string code, ulong term, string expected, CancellationToken cancellationToken)
    {
        var (_, walk, clock) = await LeaderAsync(1, cancellationToken);
        var gateway = new ScriptedTransferGateway { RefuseAt = "begin", RefusalCode = code, ReplyTerm = term };
        List<ReplicaTransferReply> seen = [];
        var stream = new ReplicaTransferStream(gateway, new ReplicaTransferOptions(), Header(), "n1", clock) { Replies = seen.Add };

        var result = await stream.RunAsync(Snapshot(), walk, cancellationToken);

        var reason = string.Equals(expected, "follower_refused", StringComparison.Ordinal) ? code : string.Empty;
        _ = await Assert.That((ReplicaTransferOutcomeNames.Of(result.Outcome), result.Reason)).IsEqualTo((expected, reason));
        _ = await Assert.That(string.Join(',', gateway.Calls)).IsEqualTo("begin");
        _ = await Assert.That((seen.Count, seen[0].Term)).IsEqualTo((1, term));
    }

    /// <summary>A transport fault or a step that outlives its timeout ends the stream as unreachable and sends exactly one abort.</summary>
    /// <param name="fault">Whether the step fails in transport; otherwise it is held until the step timeout.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task FaultOrTimeoutSendsOneAbort(bool fault, CancellationToken cancellationToken)
    {
        var (_, walk, clock) = await LeaderAsync(1, cancellationToken);
        var gateway = fault ? new ScriptedTransferGateway { FailAt = "entries" } : new ScriptedTransferGateway { HoldAt = "entries" };
        var options = new ReplicaTransferOptions { StepTimeout = TimeSpan.FromSeconds(5) };
        var run = new ReplicaTransferStream(gateway, options, Header(), "n1", clock).RunAsync(Snapshot(), walk, cancellationToken);
        if (!fault)
        {
            await gateway.Held.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            clock.Advance(options.StepTimeout);
        }

        var result = await run;

        _ = await Assert.That(result.Outcome).IsEqualTo(ReplicaTransferOutcome.Unreachable);
        _ = await Assert.That(gateway.Aborts).IsEqualTo(1);
        _ = await Assert.That(string.Join(',', gateway.Calls)).EndsWith(",abort");
    }

    /// <summary>A sender that starts draining ends the stream as aborted, and a canceled pass ends it by throwing, without an abort.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DrainAbortsAndCancellationThrows(CancellationToken cancellationToken)
    {
        var (_, walk, clock) = await LeaderAsync(1, cancellationToken);
        using var draining = new CancellationTokenSource();
        var drained = new ScriptedTransferGateway { HoldAt = "wipe" };
        var drain = new ReplicaTransferStream(drained, new ReplicaTransferOptions(), Header(), "n1", clock) { Draining = draining.Token }.RunAsync(Snapshot(), walk, cancellationToken);
        await drained.Held.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        await draining.CancelAsync();
        var aborted = await drain;

        using var pass = new CancellationTokenSource();
        var canceled = new ScriptedTransferGateway { HoldAt = "wipe" };
        var cancel = new ReplicaTransferStream(canceled, new ReplicaTransferOptions(), Header(), "n1", clock).RunAsync(Snapshot(), walk, pass.Token);
        await canceled.Held.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        await pass.CancelAsync();
        var thrown = false;
        try
        {
            _ = await cancel;
        }
        catch (OperationCanceledException) when (pass.IsCancellationRequested)
        {
            thrown = true;
        }

        _ = await Assert.That(aborted.Outcome).IsEqualTo(ReplicaTransferOutcome.Aborted);
        _ = await Assert.That(string.Join(',', drained.Calls)).EndsWith(",abort");
        _ = await Assert.That(thrown).IsTrue();
        _ = await Assert.That(canceled.Aborts).IsEqualTo(0);
    }

    /// <summary>A call canceled by the drain of the sender surfaces as a transport fault, and the stream still ends as aborted, not as unreachable.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DrainFaultInCallIsAborted(CancellationToken cancellationToken)
    {
        var (_, walk, clock) = await LeaderAsync(1, cancellationToken);
        using var draining = new CancellationTokenSource();
        var gateway = new ScriptedTransferGateway { HoldAt = "wipe", CancelFailsInTransport = true };
        var run = new ReplicaTransferStream(gateway, new ReplicaTransferOptions(), Header(), "n1", clock) { Draining = draining.Token }.RunAsync(Snapshot(), walk, cancellationToken);
        await gateway.Held.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        await draining.CancelAsync();
        var result = await run;

        _ = await Assert.That(result.Outcome).IsEqualTo(ReplicaTransferOutcome.Aborted);
        _ = await Assert.That(gateway.Aborts).IsEqualTo(1);
    }

    /// <summary>A step the follower answers as busy is sent again unchanged after a delay that doubles to two seconds; the pause is announced once per stall.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BusyStepIsResentAfterGrowingDelays(CancellationToken cancellationToken)
    {
        var (_, walk, _) = await LeaderAsync(3, cancellationToken);
        var clock = new TimerSpyClock();
        var gateway = new ScriptedTransferGateway();
        gateway.BusyFor("entries", 5);
        var paused = 0;
        var run = new ReplicaTransferStream(gateway, new ReplicaTransferOptions(), Header(), "n1", clock) { Paused = () => Interlocked.Increment(ref paused) }.RunAsync(Snapshot(), walk, cancellationToken);

        var delays = await DriveBackoffsAsync(clock, run, cancellationToken);
        var result = await run;

        _ = await Assert.That(result.Outcome).IsEqualTo(ReplicaTransferOutcome.Transferred);
        _ = await Assert.That(string.Join(',', delays.ConvertAll(static delay => delay.TotalMilliseconds.ToString(CultureInfo.InvariantCulture)))).IsEqualTo("250,500,1000,2000,2000");
        _ = await Assert.That(string.Join(',', gateway.Calls)).IsEqualTo("begin,snapshot,wipe,entries,entries,entries,entries,entries,entries,end");
        _ = await Assert.That(string.Join(',', gateway.Sequences)).IsEqualTo("1,2,3,3,3,3,3,3,4");
        _ = await Assert.That(string.Join(',', gateway.Batches)).IsEqualTo("3,3,3,3,3,3");
        _ = await Assert.That(paused).IsEqualTo(1);
        var totals = ReplicaStateHarness.TotalsOf(gateway.Sent[0]);
        _ = await Assert.That(gateway.End).IsEqualTo((totals.EntryCount, totals.Crc));
    }

    /// <summary>The stall budget restarts with every accepted reply, so two pauses that each stay under it end nothing although together they pass it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AcceptedReplyRestartsStallBudget(CancellationToken cancellationToken)
    {
        var (_, walk, _) = await LeaderAsync(3, cancellationToken);
        var clock = new TimerSpyClock();
        var gateway = new ScriptedTransferGateway();
        gateway.BusyFor("begin", 4);
        gateway.BusyFor("entries", 4);
        var options = new ReplicaTransferOptions { JournalStallBudget = TimeSpan.FromSeconds(3) };
        var run = new ReplicaTransferStream(gateway, options, Header(), "n1", clock).RunAsync(Snapshot(), walk, cancellationToken);

        var delays = await DriveBackoffsAsync(clock, run, cancellationToken);
        var result = await run;

        _ = await Assert.That(result.Outcome).IsEqualTo(ReplicaTransferOutcome.Transferred);
        var total = TimeSpan.Zero;
        foreach (var delay in CollectionsMarshal.AsSpan(delays))
            total += delay;

        _ = await Assert.That(total).IsGreaterThan(options.JournalStallBudget);
    }

    /// <summary>A follower that stays busy for the stall budget gets an abort, and the stream ends as refused as journal-full.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StallBudgetEndsAsJournalFull(CancellationToken cancellationToken)
    {
        var (_, walk, _) = await LeaderAsync(3, cancellationToken);
        var clock = new TimerSpyClock();
        var gateway = new ScriptedTransferGateway();
        gateway.BusyFor("wipe", int.MaxValue);
        var options = new ReplicaTransferOptions { JournalStallBudget = TimeSpan.FromSeconds(30) };
        var run = new ReplicaTransferStream(gateway, options, Header(), "n1", clock).RunAsync(Snapshot(), walk, cancellationToken);

        _ = await DriveBackoffsAsync(clock, run, cancellationToken);
        var result = await run;

        _ = await Assert.That(result).IsEqualTo(new ReplicaTransferStream.Result(ReplicaTransferOutcome.FollowerRefused, RefusalCodes.JournalFull));
        _ = await Assert.That(gateway.Aborts).IsEqualTo(1);
        _ = await Assert.That(string.Join(',', gateway.Calls)).EndsWith(",abort");
    }

    /// <summary>A sender that starts draining while the stream waits to resend a busy step ends it as aborted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DrainDuringBusyWaitAborts(CancellationToken cancellationToken)
    {
        var (_, walk, _) = await LeaderAsync(3, cancellationToken);
        var clock = new TimerSpyClock();
        var gateway = new ScriptedTransferGateway();
        gateway.BusyFor("wipe", int.MaxValue);
        using var draining = new CancellationTokenSource();
        var run = new ReplicaTransferStream(gateway, new ReplicaTransferOptions(), Header(), "n1", clock) { Draining = draining.Token }.RunAsync(Snapshot(), walk, cancellationToken);
        while (clock.BackoffDelays.Count == 0)
            await Task.Yield();

        await draining.CancelAsync();
        var result = await run;

        _ = await Assert.That(result.Outcome).IsEqualTo(ReplicaTransferOutcome.Aborted);
        _ = await Assert.That(gateway.Aborts).IsEqualTo(1);
    }

    /// <summary>Fires each backoff timer of the stream once it exists, until the run ends.</summary>
    /// <param name="clock">The clock of the stream.</param>
    /// <param name="run">The running stream.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The delays the stream waited, in order.</returns>
    private static async Task<List<TimeSpan>> DriveBackoffsAsync(TimerSpyClock clock, Task run, CancellationToken cancellationToken)
    {
        List<TimeSpan> waited = [];
        while (!run.IsCompleted)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var created = clock.BackoffDelays;
            if (waited.Count < created.Count)
            {
                var delay = created[waited.Count];
                waited.Add(delay);
                clock.Advance(delay);
            }
            else
            {
                await Task.Yield();
            }
        }

        return waited;
    }

    private static List<ReplicaStateEntry> AllEntries(ScriptedTransferGateway gateway)
    {
        List<ReplicaStateEntry> all = [];
        foreach (var batch in CollectionsMarshal.AsSpan(gateway.Sent))
            all.AddRange(batch);

        return all;
    }

    private static ReplicaRpcHeader Header() => new(GroupId, TransferFixtures.Fingerprint, 1UL, 1UL, "n2", "n2");

    private static GroupSnapshot Snapshot() => new(
        GroupId,
        TransferFixtures.Fingerprint,
        1UL,
        TransferFixtures.SnapshotTerm,
        TransferFixtures.SnapshotIndex,
        TransferFixtures.SnapshotIndex,
        [],
        new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc));

    private static ReplicaTransferStream StreamTo(ScriptedTransferGateway gateway, FakeTimeProvider clock) =>
        new(gateway, new ReplicaTransferOptions(), Header(), "n1", clock);

    private static async Task<(PhysicalCache<object?> Cache, ReplicaGroupEntries Walk, FakeTimeProvider Clock)> LeaderAsync(int keys, CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var cache = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
        for (var i = 0; i < keys; i++)
            await cache.SetAsync(new CacheKey(CacheName, $"key-{i}"), new NodeCacheEntry<object?>($"v{i}"), cancellationToken);

        var locator = new INodeLocatorCreateExpectations();
        _ = locator.Setups.GetOwner(Arg.Any<string>(), Arg.Any<string>()).ReturnValue(GroupId);
        return (cache, new ReplicaGroupEntries(cache, locator.Instance()), clock);
    }

    /// <summary>Routes the messages of a stream to a receiver, as the replication service does.</summary>
    private sealed class ReceiverGateway : IReplicaTransferGateway
    {
        private readonly ReplicaTransferReceiver _receiver;

        internal ReceiverGateway(ReplicaTransferReceiver receiver)
        {
            _receiver = receiver;
        }

        public Task<ReplicaTransferReply> BeginAsync(string nodeId, ReplicaRpcHeader header, string transferId, ReplicaTransferBaseline baseline, CancellationToken cancellationToken) =>
            _receiver.BeginAsync(SenderOf(in header, transferId), baseline, cancellationToken);

        public Task<ReplicaTransferReply> SendAsync(string nodeId, ReplicaRpcHeader header, ReplicaTransferMessage message, CancellationToken cancellationToken) => message.Step switch
        {
            ReplicaTransferStep.Snapshot => _receiver.SnapshotAsync(SenderOf(in header, message.TransferId), message.Sequence, message.Offset, message.Chunk, cancellationToken),
            ReplicaTransferStep.Wipe => _receiver.WipeAsync(SenderOf(in header, message.TransferId), message.Sequence, cancellationToken),
            ReplicaTransferStep.Entries => _receiver.EntriesAsync(SenderOf(in header, message.TransferId), message.Sequence, message.Entries, cancellationToken),
            ReplicaTransferStep.End => _receiver.EndAsync(SenderOf(in header, message.TransferId), message.Sequence, message.EntryCount, message.EntriesCrc, cancellationToken),
            _ => throw new ArgumentOutOfRangeException(nameof(message), message.Step, "Unsupported transfer step."),
        };

        public Task<ReplicaTransferReply> AbortAsync(string nodeId, ReplicaRpcHeader header, string transferId, CancellationToken cancellationToken) =>
            _receiver.AbortAsync(SenderOf(in header, transferId), cancellationToken);

        private static ReplicaTransferHeader SenderOf(in ReplicaRpcHeader header, string transferId) => new(header.GroupId, transferId, header.LeaderNodeId, header.Term);
    }

    /// <summary>A fake clock that records the timers made for the short waits of the stream, so a test fires each one once it exists.</summary>
    [ThreadSafe]
    private sealed class TimerSpyClock : FakeTimeProvider
    {
        private static readonly TimeSpan LongestBackoff = TimeSpan.FromSeconds(2);
        private readonly List<TimeSpan> _backoffs = [];
        private readonly Lock _sync = new();

        internal TimerSpyClock()
            : base(new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero))
        {
        }

        /// <summary>Gets the delays of the backoff timers made so far, in order; the longer step timeouts are not counted.</summary>
        internal List<TimeSpan> BackoffDelays
        {
            get
            {
                lock (_sync)
                    return [.. _backoffs];
            }
        }

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            if (dueTime <= LongestBackoff)
            {
                lock (_sync)
                    _backoffs.Add(dueTime);
            }

            return base.CreateTimer(callback, state, dueTime, period);
        }
    }
}
