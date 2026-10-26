using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Cluster.Replication.ReplicaSenderTestKit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Heartbeats of a <see cref="ReplicaFollowerSender" />: only from an idle slot, never ahead of an entry, every reply observed.</summary>
[Immutable]
public sealed class ReplicaFollowerSenderHeartbeatTests : ServerUnitTestBase
{
    /// <summary>An idle slot sends an empty append at its last entry with the leader commit, and the reply reaches the observer with the slot.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task IdleSlotHeartbeatsAtLastEntry(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var observer = new ReplyLog(1);
        var sender = new ReplicaFollowerSender(gateway, "n2", in Header, 5, 1, HangGuard) { TimeProvider = TimeProvider.System, ReplicaIndex = 2, ReplyObserver = (slot, reply) => observer.Record(slot, reply.Success) };
        try
        {
            var sent = sender.TryEnqueueHeartbeat(4UL);
            var heartbeat = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            heartbeat.Accept();
            var replies = await BoundedAsync(observer.Completed, cancellationToken);

            _ = await Assert.That(sent).IsTrue();
            _ = await Assert.That((heartbeat.Count, heartbeat.PrevLogIndex, heartbeat.CommitIndex)).IsEqualTo((0, 5UL, 4UL));
            _ = await Assert.That(replies).IsEqualTo("2:True");
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>
    /// A slot with a request in flight takes no heartbeat; an entry enqueued behind a heartbeat goes out after it, and both replies are
    /// observed.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BusySlotSkipsHeartbeat(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var observer = new ReplyLog(2);
        var sender = new ReplicaFollowerSender(gateway, "n2", in Header, 0, 0, HangGuard) { TimeProvider = TimeProvider.System, ReplicaIndex = 1, ReplyObserver = (slot, reply) => observer.Record(slot, reply.Success) };
        try
        {
            var idle = sender.TryEnqueueHeartbeat(0UL);
            var heartbeat = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            var entry = EnqueueAsync(sender, 1);
            var busy = sender.TryEnqueueHeartbeat(0UL);
            heartbeat.Accept();
            var append = await BoundedAsync(gateway.CallAsync(1), cancellationToken);
            append.Accept();
            _ = await BoundedAsync(entry, cancellationToken);
            var replies = await BoundedAsync(observer.Completed, cancellationToken);

            _ = await Assert.That((idle, busy)).IsEqualTo((true, false));
            _ = await Assert.That((heartbeat.Count, append.FirstIndex)).IsEqualTo((0, 1UL));
            _ = await Assert.That(replies).IsEqualTo("1:True,1:True");
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>An idle slot sends one empty append with the commit index, and the same or a lower index, once sent, sends nothing more.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task IdleSlotSendsEachCommitIndexOnce(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            var first = sender.TryEnqueueCommitIndex(3UL);
            var heartbeat = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            heartbeat.Accept();
            var repeated = sender.TryEnqueueCommitIndex(3UL);
            var lower = sender.TryEnqueueCommitIndex(2UL);
            _ = sender.TryEnqueueCommitIndex(4UL);
            var second = await BoundedAsync(gateway.CallAsync(1), cancellationToken);
            second.Accept();

            _ = await Assert.That((first, repeated, lower)).IsEqualTo((true, false, false));
            _ = await Assert.That((heartbeat.Count, heartbeat.CommitIndex, second.Count, second.CommitIndex)).IsEqualTo((0, 3UL, 0, 4UL));
            _ = await Assert.That(gateway.CallCount).IsEqualTo(2);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>A busy slot defers one heartbeat however often the index is offered, and sends it once the slot runs out of entries.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BusySlotDefersOneCommitIndexHeartbeat(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            var entry = EnqueueAsync(sender, 1);
            var append = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            var offers = new[] { sender.TryEnqueueCommitIndex(1UL), sender.TryEnqueueCommitIndex(1UL), sender.TryEnqueueCommitIndex(1UL) };
            append.Accept();
            _ = await BoundedAsync(entry, cancellationToken);
            var heartbeat = await BoundedAsync(gateway.CallAsync(1), cancellationToken);
            heartbeat.Accept();
            var after = sender.TryEnqueueCommitIndex(1UL);

            _ = await Assert.That((offers[0], offers[1], offers[2])).IsEqualTo((false, false, false));
            _ = await Assert.That((heartbeat.Count, heartbeat.PrevLogIndex, heartbeat.CommitIndex)).IsEqualTo((0, 1UL, 1UL));
            _ = await Assert.That((after, gateway.CallCount)).IsEqualTo((false, 2));
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>A busy slot that sends another request carrying the index sends no heartbeat for it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NextBatchCarriesCommitIndex(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            var first = EnqueueAsync(sender, 1);
            var firstCall = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            var second = EnqueueAsync(sender, 2);
            var deferred = sender.TryEnqueueCommitIndex(1UL);
            firstCall.Accept();
            _ = await BoundedAsync(first, cancellationToken);
            var secondCall = await BoundedAsync(gateway.CallAsync(1), cancellationToken);
            secondCall.Accept();
            _ = await BoundedAsync(second, cancellationToken);
            var after = sender.TryEnqueueCommitIndex(1UL);

            _ = await Assert.That((deferred, after)).IsEqualTo((false, false));
            _ = await Assert.That((secondCall.Count, secondCall.CommitIndex)).IsEqualTo((1, 1UL));
            _ = await Assert.That(gateway.CallCount).IsEqualTo(2);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>A drained or closed sender takes no commit index heartbeat.</summary>
    [Test]
    public async Task NoCommitIndexHeartbeatWhenClosed()
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);

        await sender.DrainAsync(HangGuard);
        var draining = sender.TryEnqueueCommitIndex(1UL);
        await sender.DisposeAsync();
        var closed = sender.TryEnqueueCommitIndex(2UL);

        _ = await Assert.That((draining, closed)).IsEqualTo((false, false));
        _ = await Assert.That(gateway.CallCount).IsEqualTo(0);
    }

    /// <summary>The debounce sends one empty append with the commit index only once the delay passed, and nothing more for an index it sent.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DebounceSendsOneHeartbeatAfterDelay(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var time = new FakeTimeProvider();
        var sender = CreateTimedSender(gateway, time);
        try
        {
            sender.ScheduleCommitIndex(3UL);
            time.Advance(ReplicaFollowerSender.CommitIndexDebounce - TimeSpan.FromMilliseconds(1));
            var early = gateway.CallCount;
            time.Advance(TimeSpan.FromMilliseconds(1));
            var heartbeat = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            heartbeat.Accept();
            sender.ScheduleCommitIndex(3UL);
            time.Advance(ReplicaFollowerSender.CommitIndexDebounce);

            _ = await Assert.That((early, heartbeat.Count, heartbeat.CommitIndex, gateway.CallCount)).IsEqualTo((0, 0, 3UL, 1));
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>A writer that commits faster than the debounce sends no empty append while it writes; the last index follows after the delay.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FastSerialWriterSendsNoHeartbeat(CancellationToken cancellationToken)
    {
        const ulong writes = 10;
        var gateway = new ParkingFollowerGateway();
        var time = new FakeTimeProvider();
        var sender = CreateTimedSender(gateway, time);
        try
        {
            for (var i = 1UL; i <= writes; i++)
            {
                var entry = EnqueueAsync(sender, i);
                (await BoundedAsync(gateway.CallAsync(int.CreateChecked(i) - 1), cancellationToken)).Accept();
                _ = await BoundedAsync(entry, cancellationToken);
                sender.ScheduleCommitIndex(i);
                time.Advance(ReplicaFollowerSender.CommitIndexDebounce / 5);
            }

            var during = gateway.CallCount;
            time.Advance(ReplicaFollowerSender.CommitIndexDebounce);
            var tail = await BoundedAsync(gateway.CallAsync(int.CreateChecked(writes)), cancellationToken);
            tail.Accept();

            _ = await Assert.That((during, tail.Count, tail.CommitIndex, gateway.CallCount)).IsEqualTo((int.CreateChecked(writes), 0, writes, int.CreateChecked(writes) + 1));
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>An index that the next batch of a busy slot carries is not sent again when the debounce fires.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BatchCarryingIndexCancelsDebounce(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var time = new FakeTimeProvider();
        var sender = CreateTimedSender(gateway, time);
        try
        {
            var first = EnqueueAsync(sender, 1);
            var firstCall = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            var second = EnqueueAsync(sender, 2);
            sender.ScheduleCommitIndex(1UL);
            firstCall.Accept();
            _ = await BoundedAsync(first, cancellationToken);
            var secondCall = await BoundedAsync(gateway.CallAsync(1), cancellationToken);
            secondCall.Accept();
            _ = await BoundedAsync(second, cancellationToken);
            time.Advance(ReplicaFollowerSender.CommitIndexDebounce * 2);

            _ = await Assert.That((secondCall.CommitIndex, gateway.CallCount)).IsEqualTo((1UL, 2));
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>A sender that was closed or drained after the debounce was armed sends nothing when it passes.</summary>
    [Test]
    public async Task ClosedOrDrainedSenderSendsNoDebounce()
    {
        var gateway = new ParkingFollowerGateway();
        var time = new FakeTimeProvider();
        var drained = CreateTimedSender(gateway, time);
        var closed = CreateTimedSender(gateway, time);
        drained.ScheduleCommitIndex(1UL);
        closed.ScheduleCommitIndex(1UL);
        await drained.DrainAsync(HangGuard);
        await closed.DisposeAsync();
        time.Advance(ReplicaFollowerSender.CommitIndexDebounce * 2);
        drained.ScheduleCommitIndex(2UL);
        closed.ScheduleCommitIndex(2UL);
        time.Advance(ReplicaFollowerSender.CommitIndexDebounce * 2);
        await drained.DisposeAsync();

        _ = await Assert.That(gateway.CallCount).IsEqualTo(0);
    }

    /// <summary>A catch-up lease, a drain, and a close each keep the heartbeat from going out.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NoHeartbeatWhileLeasedDrainedOrClosed(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        bool leased;
        using (await sender.BeginCatchUpAsync(cancellationToken))
            leased = sender.TryEnqueueHeartbeat(0UL);

        await sender.DrainAsync(HangGuard);
        var draining = sender.TryEnqueueHeartbeat(0UL);
        await sender.DisposeAsync();
        var closed = sender.TryEnqueueHeartbeat(0UL);

        _ = await Assert.That((leased, draining, closed)).IsEqualTo((false, false, false));
        _ = await Assert.That(gateway.CallCount).IsEqualTo(0);
    }

    /// <summary>A send that fails in transport reports no reply, and an observer that throws never fails an entry the follower accepted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ObserverSeesOnlyAnswers(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var observed = 0;
        var sender = new ReplicaFollowerSender(gateway, "n2", in Header, 0, 0, HangGuard)
        {
            TimeProvider = TimeProvider.System,
            ReplyObserver = (_, _) =>
            {
                _ = Interlocked.Increment(ref observed);
                throw new InvalidOperationException("observer fault");
            },
        };
        try
        {
            _ = sender.TryEnqueueHeartbeat(0UL);
            (await BoundedAsync(gateway.CallAsync(0), cancellationToken)).Fail(new IOException("heartbeat lost"));
            var failed = EnqueueAsync(sender, 1);
            (await BoundedAsync(gateway.CallAsync(1), cancellationToken)).Fail(new IOException("append lost"));
            _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(BoundedAsync(failed, cancellationToken));
            var silent = Volatile.Read(ref observed);
            var accepted = EnqueueAsync(sender, 2);
            (await BoundedAsync(gateway.CallAsync(2), cancellationToken)).Accept();

            var acknowledgement = await BoundedAsync(accepted, cancellationToken);

            _ = await Assert.That(silent).IsEqualTo(0);
            _ = await Assert.That(acknowledgement.LogIndex).IsEqualTo(2UL);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>Collects the observed replies and completes once the expected number arrived.</summary>
    [ThreadSafe]
    private sealed class ReplyLog
    {
        private readonly TaskCompletionSource<string> _completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly int _expected;
        private readonly List<(int Slot, bool Success)> _replies = [];
        private readonly Lock _sync = new();

        internal ReplyLog(int expected)
        {
            _expected = expected;
        }

        /// <summary>Gets the replies once the expected number arrived, comma-separated as slot:success.</summary>
        internal Task<string> Completed => _completed.Task;

        internal void Record(int slot, bool success)
        {
            lock (_sync)
            {
                _replies.Add((slot, success));
                if (_replies.Count == _expected)
                    _ = _completed.TrySetResult(string.Join(',', _replies.ConvertAll(static reply => $"{reply.Slot}:{reply.Success}")));
            }
        }
    }
}
