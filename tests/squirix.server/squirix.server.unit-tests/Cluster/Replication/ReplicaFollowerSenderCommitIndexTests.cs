using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Cluster.Replication.ReplicaSenderTestKit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>The commit index a <see cref="ReplicaFollowerSender" /> delivers to its follower: carried by an append, or sent alone after a quiet period.</summary>
public sealed class ReplicaFollowerSenderCommitIndexTests : ServerUnitTestBase
{
    private const int MaxClockSteps = 100_000;

    private static readonly TimeSpan ClockStep = TimeSpan.FromMilliseconds(5);

    /// <summary>A batch of a busy slot that carries the index makes the empty append needless.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BatchCarryingIndexNeedsNoHeartbeat(CancellationToken cancellationToken)
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

    /// <summary>An accepted catch-up request ends the backoff of refused live batches, so the tail still goes out within the quiet period.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CatchUpEndsBackoff(CancellationToken cancellationToken)
    {
        const ulong refusals = 3;
        var gateway = new ParkingFollowerGateway();
        var time = new FakeTimeProvider();
        var sender = CreateTimedSender(gateway, time);
        try
        {
            for (var i = 1UL; i <= refusals; i++)
            {
                var entry = EnqueueAsync(sender, i);
                (await BoundedAsync(gateway.CallAsync(int.CreateChecked(i) - 1), cancellationToken)).Refuse("log mismatch");
                _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(entry);
                await AwaitRetryStateAsync(sender, (int.CreateChecked(i), false));
            }

            sender.ScheduleCommitIndex(7UL);
            using (var lease = await sender.BeginCatchUpAsync(cancellationToken))
            {
                var batch = new FollowerBatch([], Header.LeaderNodeId, Header.Term, 0, 0, 2);
                var catchUp = lease.SendAsync(in batch, cancellationToken);
                (await BoundedAsync(gateway.CallAsync(int.CreateChecked(refusals)), cancellationToken)).Accept();
                _ = await BoundedAsync(catchUp, cancellationToken);
            }

            time.Advance(ReplicaFollowerSender.CommitIndexDebounce);
            var tail = await BoundedAsync(gateway.CallAsync(int.CreateChecked(refusals) + 1), cancellationToken);
            tail.Accept();

            _ = await Assert.That((tail.Count, tail.CommitIndex)).IsEqualTo((0, 7UL));
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>A closed sender, or one drained with nothing undelivered, sends nothing when the quiet period passes.</summary>
    [Test]
    public async Task ClosedOrCurrentSenderSendsNothing()
    {
        var gateway = new ParkingFollowerGateway();
        var time = new FakeTimeProvider();
        var drained = CreateTimedSender(gateway, time);
        var closed = CreateTimedSender(gateway, time);
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

    /// <summary>A drain tries its flush once: a refused or lost one ends the drain at once instead of repeating until the budget.</summary>
    /// <param name="refuse">Whether the follower refuses the flush; otherwise the transport fails.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task DrainFlushesOnce(bool refuse, CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var time = new FakeTimeProvider();
        var sender = CreateTimedSender(gateway, time);
        try
        {
            sender.ScheduleCommitIndex(3UL);
            var drain = sender.DrainAsync(HangGuard).AsTask();
            var flush = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            if (refuse)
                flush.Refuse("log mismatch");
            else
                flush.Fail(new IOException("peer down"));

            await drain.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            time.Advance(ReplicaFollowerSender.CommitIndexRetryCap * 2);

            _ = await Assert.That((flush.CommitIndex, gateway.CallCount)).IsEqualTo((3UL, 1));
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>A drain inside the quiet period sends the index at once and waits for it, so the sender that replaces this one loses nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DrainFlushesUndeliveredIndex(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var time = new FakeTimeProvider();
        var sender = CreateTimedSender(gateway, time);
        try
        {
            sender.ScheduleCommitIndex(3UL);
            var drain = sender.DrainAsync(HangGuard).AsTask();
            var heartbeat = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            var early = drain.IsCompleted;
            heartbeat.Accept();
            await drain.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            time.Advance(ReplicaFollowerSender.CommitIndexDebounce * 4);

            _ = await Assert.That((early, heartbeat.Count, heartbeat.CommitIndex, gateway.CallCount)).IsEqualTo((false, 0, 3UL, 1));
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>A heartbeat the follower did not answer delivered nothing, so it is sent again after the quiet period.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedHeartbeatIsSentAgain(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var time = new FakeTimeProvider();
        var sender = CreateTimedSender(gateway, time);
        try
        {
            sender.ScheduleCommitIndex(3UL);
            time.Advance(ReplicaFollowerSender.CommitIndexDebounce);
            var failedAt = time.GetTimestamp();
            (await BoundedAsync(gateway.CallAsync(0), cancellationToken)).Fail(new IOException("heartbeat lost"));
            var again = await AdvanceUntilCallAsync(time, gateway, 1, cancellationToken);
            var waited = time.GetElapsedTime(failedAt);
            again.Refuse("log mismatch");
            var third = await AdvanceUntilCallAsync(time, gateway, 2, cancellationToken);
            third.Accept();
            time.Advance(ReplicaFollowerSender.CommitIndexDebounce * 4);

            _ = await Assert.That(waited >= ReplicaFollowerSender.CommitIndexDebounce).IsTrue();
            _ = await Assert.That((again.CommitIndex, third.CommitIndex, gateway.CallCount)).IsEqualTo((3UL, 3UL, 3));
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>A writer that commits faster than the quiet period sends no empty append while it writes; the last index follows after the quiet period.</summary>
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

    /// <summary>The empty append goes out once the quiet period passed, and nothing more for an index the follower accepted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HeartbeatGoesOutAfterQuietPeriod(CancellationToken cancellationToken)
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
            time.Advance(ReplicaFollowerSender.CommitIndexDebounce * 2);

            _ = await Assert.That((early, heartbeat.Count, heartbeat.CommitIndex, gateway.CallCount)).IsEqualTo((0, 0, 3UL, 1));
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>A quiet period that passes under a catch-up lease does not lose the index: the end of the lease sends it even with nothing left to send.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeaseEndSendsUndeliveredIndex(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var time = new FakeTimeProvider();
        var sender = CreateTimedSender(gateway, time);
        try
        {
            Task<ReplicaDurableAcknowledgement> held;
            using (var lease = await sender.BeginCatchUpAsync(cancellationToken))
            {
                held = EnqueueAsync(sender, 1);
                sender.ScheduleCommitIndex(1UL);
                time.Advance(ReplicaFollowerSender.CommitIndexDebounce);
                lease.MarkHeld(1UL);
            }

            _ = await BoundedAsync(held, cancellationToken);
            var heartbeat = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            heartbeat.Accept();

            _ = await Assert.That((heartbeat.Count, heartbeat.PrevLogIndex, heartbeat.CommitIndex, gateway.CallCount)).IsEqualTo((0, 1UL, 1UL, 1));
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>A batch that carries a lower commit index than the target does not count as delivering the target.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LowerBatchLeavesTargetUndelivered(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var time = new FakeTimeProvider();
        var sender = CreateTimedSender(gateway, time);
        try
        {
            var first = EnqueueAsync(sender, 1);
            var firstCall = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            var second = EnqueueAsync(sender, 2);
            sender.ScheduleCommitIndex(5UL);
            time.Advance(ReplicaFollowerSender.CommitIndexDebounce);
            firstCall.Accept();
            _ = await BoundedAsync(first, cancellationToken);
            var secondCall = await BoundedAsync(gateway.CallAsync(1), cancellationToken);
            secondCall.Accept();
            _ = await BoundedAsync(second, cancellationToken);
            var heartbeat = await BoundedAsync(gateway.CallAsync(2), cancellationToken);
            heartbeat.Accept();

            _ = await Assert.That((secondCall.CommitIndex, heartbeat.Count, heartbeat.CommitIndex, gateway.CallCount)).IsEqualTo((1UL, 0, 5UL, 3));
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>Retries after refusals wait 50, 100, 200 ms and so on up to the cap, and one accepted request starts over at the quiet period.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusedHeartbeatBacksOff(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var time = new FakeTimeProvider();
        var sender = CreateTimedSender(gateway, time);
        try
        {
            sender.ScheduleCommitIndex(3UL);
            time.Advance(ReplicaFollowerSender.CommitIndexDebounce);
            var spacings = new TimeSpan[8];
            var call = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            for (var i = 0; i < spacings.Length; i++)
            {
                var refusedAt = time.GetTimestamp();
                call.Refuse("log mismatch");
                await AwaitRetryStateAsync(sender, (i + 1, true));
                call = await AdvanceUntilCallAsync(time, gateway, i + 1, cancellationToken);
                spacings[i] = time.GetElapsedTime(refusedAt);
            }

            call.Accept();
            await AwaitRetryStateAsync(sender, (0, false));
            sender.ScheduleCommitIndex(4UL);
            time.Advance(ReplicaFollowerSender.CommitIndexDebounce);
            var restarted = await BoundedAsync(gateway.CallAsync(spacings.Length + 1), cancellationToken);

            var expected = ReplicaFollowerSender.CommitIndexDebounce;
            for (var i = 0; i < spacings.Length; i++)
            {
                _ = await Assert.That(spacings[i] >= expected && spacings[i] < expected + ClockStep).IsTrue().Because($"retry {i} waited {spacings[i]}, expected {expected}");
                expected = TimeSpan.FromTicks(Math.Min(expected.Ticks * 2, ReplicaFollowerSender.CommitIndexRetryCap.Ticks));
            }

            _ = await Assert.That(restarted.CommitIndex).IsEqualTo(4UL);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>Moves the clock in small steps until the call arrives; the reply of the previous call is handled on another thread, so the step count is not fixed.</summary>
    /// <param name="time">The clock of the sender.</param>
    /// <param name="gateway">The follower transport double.</param>
    /// <param name="index">The zero-based arrival number of the call.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The call, once it arrived.</returns>
    private static async Task<ParkingFollowerGateway.Call> AdvanceUntilCallAsync(FakeTimeProvider time, ParkingFollowerGateway gateway, int index, CancellationToken cancellationToken)
    {
        var arrival = gateway.CallAsync(index);
        for (var step = 0; step < MaxClockSteps && !arrival.IsCompleted; step++)
        {
            time.Advance(ClockStep);
            await Task.Yield();
        }

        return await BoundedAsync(arrival, cancellationToken);
    }

    /// <summary>Waits until the sender has processed the reply of its last request.</summary>
    /// <param name="sender">The sender.</param>
    /// <param name="expected">The failure count and armed state to wait for.</param>
    /// <returns>An asynchronous operation.</returns>
    private static async Task AwaitRetryStateAsync(ReplicaFollowerSender sender, (int Failures, bool Armed) expected)
    {
        for (var attempt = 0; attempt < MaxClockSteps && sender.CommitRetryState != expected; attempt++)
            await Task.Yield();

        _ = await Assert.That(sender.CommitRetryState).IsEqualTo(expected);
    }
}
