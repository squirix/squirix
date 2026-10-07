using System;
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

/// <summary>Pausing one <see cref="ReplicaFollowerSender" /> under a catch-up lease and resuming it when the lease ends.</summary>
[Immutable]
public sealed class ReplicaFollowerSenderCatchUpTests : ServerUnitTestBase
{
    /// <summary>A lease is granted only once the request in flight is answered, and that request's entry is still acknowledged.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BeginCatchUpWaitsForRequestInFlight(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            var first = EnqueueAsync(sender, 1);
            var call = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            var begin = BeginAsync(sender, cancellationToken);

            _ = await Assert.That(begin.IsCompleted).IsFalse();
            call.Accept();

            using var lease = await BoundedAsync(begin, cancellationToken);
            _ = await Assert.That((await BoundedAsync(first, cancellationToken)).LogIndex).IsEqualTo(1UL);
            _ = await Assert.That(lease.NodeId).IsEqualTo("n2");
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>An entry enqueued while a lease is held waits; the lease's own request goes out, and the entry follows once the lease ends.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LiveEntriesParkWhilePaused(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            var lease = await sender.BeginCatchUpAsync(cancellationToken);
            var live = EnqueueAsync(sender, 1);

            _ = await Assert.That(gateway.CallCount).IsEqualTo(0);
            var send = lease.SendAsync(Probe(0), cancellationToken);
            var probe = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            _ = await Assert.That(probe.Count).IsEqualTo(0);
            _ = await Assert.That(live.IsCompleted).IsFalse();
            probe.Accept();
            _ = await BoundedAsync(send, cancellationToken);
            lease.Dispose();

            var resumed = await BoundedAsync(gateway.CallAsync(1), cancellationToken);
            resumed.Accept();
            _ = await Assert.That(resumed.FirstIndex).IsEqualTo(1UL);
            _ = await Assert.That((await BoundedAsync(live, cancellationToken)).LogIndex).IsEqualTo(1UL);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>Only one lease may be active on a sender.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SecondLeaseIsRefused(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            using var lease = await sender.BeginCatchUpAsync(cancellationToken);

            _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReplicaFollowerCatchUp>(sender.BeginCatchUpAsync(cancellationToken));
        }
        finally
        {
            await sender.DisposeAsync();
        }
    }

    /// <summary>A lease keeps one request in flight; a second send before the answer fails without reaching the follower.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CatchUpSendIsOneAtATime(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            using var lease = await sender.BeginCatchUpAsync(cancellationToken);
            var first = lease.SendAsync(Probe(0), cancellationToken);
            var call = await BoundedAsync(gateway.CallAsync(0), cancellationToken);

            _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(lease.SendAsync(Probe(0), cancellationToken));

            call.Accept();
            _ = await Assert.That((await BoundedAsync(first, cancellationToken)).Success).IsTrue();
            _ = await Assert.That(gateway.CallCount).IsEqualTo(1);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>
    /// Ending a lease acknowledges the waiting entries the catch-up delivered, with the acknowledgement the live path would build, and
    /// sends the rest naming the delivered entry as their predecessor.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ResumeAcknowledgesHeldEntries(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            var lease = await sender.BeginCatchUpAsync(cancellationToken);
            var first = EnqueueAsync(sender, 1);
            var second = EnqueueAsync(sender, 2);
            var third = EnqueueAsync(sender, 3);
            lease.MarkHeld(2);
            lease.Dispose();

            var held = await BoundedAsync(first, cancellationToken);
            _ = await Assert.That((await BoundedAsync(second, cancellationToken)).LogIndex).IsEqualTo(2UL);
            var resumed = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            resumed.Accept();
            var live = await BoundedAsync(third, cancellationToken);

            _ = await Assert.That(resumed.FirstIndex).IsEqualTo(3UL);
            _ = await Assert.That(resumed.PrevLogIndex).IsEqualTo(2UL);
            _ = await Assert.That(resumed.Count).IsEqualTo(1);
            _ = await Assert.That((held.GroupId, held.Term, held.LogIndex, held.PayloadChecksum, held.IsDurable, held.IsReady))
                                  .IsEqualTo((live.GroupId, live.Term, 1UL, live.PayloadChecksum, live.IsDurable, live.IsReady));
            _ = await Assert.That(held.OperationFingerprint.Span.SequenceEqual(live.OperationFingerprint.Span)).IsTrue();
            _ = await Assert.That(gateway.CallCount).IsEqualTo(1);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>Disposing the sender cancels the lease's request in flight; the lease can still be disposed afterwards.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeCancelsCatchUpRequest(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway { ObservesCancellation = true };
        var sender = CreateSender(gateway);
        var lease = await sender.BeginCatchUpAsync(cancellationToken);
        try
        {
            var send = lease.SendAsync(Probe(0), cancellationToken);
            var call = await BoundedAsync(gateway.CallAsync(0), cancellationToken);

            await sender.DisposeAsync().AsTask().WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            _ = await Assert.That(call.Token.IsCancellationRequested).IsTrue();
            _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(BoundedAsync(send, cancellationToken));
            _ = await Assert.That(lease.IsClosed).IsTrue();
            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(lease.SendAsync(Probe(0), cancellationToken));
        }
        finally
        {
            lease.Dispose();
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>A lease's request is bounded by the sender's append timeout on the sender's clock.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CatchUpSendTimesOutOnSenderClock(CancellationToken cancellationToken)
    {
        var time = new FakeTimeProvider();
        var gateway = new ParkingFollowerGateway { ObservesCancellation = true };
        var sender = new ReplicaFollowerSender(gateway, "n2", in Header, 0, 0, TimeSpan.FromSeconds(5)) { TimeProvider = time };
        try
        {
            using var lease = await sender.BeginCatchUpAsync(cancellationToken);
            var send = lease.SendAsync(Probe(0), cancellationToken);
            _ = await BoundedAsync(gateway.CallAsync(0), cancellationToken);

            time.Advance(TimeSpan.FromSeconds(5));

            _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(BoundedAsync(send, cancellationToken));
            _ = await Assert.That(lease.IsClosed).IsFalse();
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>A drain waits for the active lease to end before it waits for the loop that sends the rest.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DrainWaitsForLease(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            var lease = await sender.BeginCatchUpAsync(cancellationToken);
            var live = EnqueueAsync(sender, 1);
            var drain = sender.DrainAsync(HangGuard).AsTask();

            _ = await Assert.That(drain.IsCompleted).IsFalse();
            lease.Dispose();

            var resumed = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            _ = await Assert.That(drain.IsCompleted).IsFalse();
            resumed.Accept();
            await drain.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            _ = await Assert.That((await BoundedAsync(live, cancellationToken)).LogIndex).IsEqualTo(1UL);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>Once a drain started, the lease's next send is refused so the catch-up ends instead of spending the drain budget.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SendAfterDrainIsRefused(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            var lease = await sender.BeginCatchUpAsync(cancellationToken);
            var inFlight = lease.SendAsync(Probe(0), cancellationToken);
            var call = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            var drain = sender.DrainAsync(HangGuard).AsTask();

            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(lease.SendAsync(Probe(0), cancellationToken));

            call.Accept();
            _ = await Assert.That((await BoundedAsync(inFlight, cancellationToken)).Success).IsTrue();
            lease.Dispose();
            await drain.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            _ = await Assert.That(gateway.CallCount).IsEqualTo(1);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>Entries parked under a lease still count against the backlog cap.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BacklogFullWhilePaused(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = new ReplicaFollowerSender(gateway, "n2", in Header, 0, 0, HangGuard) { MaxPendingEntries = 1 };
        try
        {
            using var lease = await sender.BeginCatchUpAsync(cancellationToken);
            var parked = EnqueueAsync(sender, 1);

            var rejected = EnqueueAsync(sender, 2);

            _ = await Assert.That(parked.IsCompleted).IsFalse();
            var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(rejected);
            _ = await Assert.That(error.Message).IsEqualTo("follower append backlog full");
            _ = await Assert.That(gateway.CallCount).IsEqualTo(0);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>A canceled wait for the request in flight resumes the live sends, and a later lease is still granted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancelledBeginCatchUpResumesSender(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            _ = EnqueueAsync(sender, 1);
            var first = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            using var canceled = new CancellationTokenSource();
            var begin = BeginAsync(sender, canceled.Token);
            await canceled.CancelAsync();

            _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(BoundedAsync(begin, cancellationToken));

            var second = EnqueueAsync(sender, 2);
            first.Accept();
            var resumed = await BoundedAsync(gateway.CallAsync(1), cancellationToken);
            resumed.Accept();
            _ = await Assert.That(resumed.FirstIndex).IsEqualTo(2UL);
            _ = await Assert.That((await BoundedAsync(second, cancellationToken)).LogIndex).IsEqualTo(2UL);

            using var lease = await BoundedAsync(BeginAsync(sender, cancellationToken), cancellationToken);
            _ = await Assert.That(lease.IsClosed).IsFalse();
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>A closed sender grants no lease.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ClosedSenderRefusesLease(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        await sender.DisposeAsync();

        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException, ReplicaFollowerCatchUp>(sender.BeginCatchUpAsync(cancellationToken));
    }

    /// <summary>A drain cancels the drain token at once, before it waits for the lease, so a lease holder waiting on the drain's gate can give up.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DrainCancelsDrainStarted(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            var lease = await sender.BeginCatchUpAsync(cancellationToken);
            var token = sender.DrainStarted;
            _ = await Assert.That(token.IsCancellationRequested).IsFalse();

            var drain = sender.DrainAsync(HangGuard).AsTask();

            _ = await Assert.That(token.IsCancellationRequested).IsTrue();
            _ = await Assert.That(drain.IsCompleted).IsFalse();
            lease.Dispose();
            await drain.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await sender.DisposeAsync();
        }
    }

    /// <summary>A closed sender hands out an already canceled drain token.</summary>
    [Test]
    public async Task ClosedSenderDrainStartedIsCanceled()
    {
        var sender = CreateSender(new ParkingFollowerGateway());
        await sender.DisposeAsync();

        _ = await Assert.That(sender.DrainStarted.IsCancellationRequested).IsTrue();
    }

    /// <summary>Starts taking a lease as a task, so the test can observe whether it completed.</summary>
    /// <param name="sender">The sender.</param>
    /// <param name="cancellationToken">The token of the wait.</param>
    /// <returns>The task of the lease.</returns>
    private static Task<ReplicaFollowerCatchUp> BeginAsync(ReplicaFollowerSender sender, CancellationToken cancellationToken) =>
        sender.BeginCatchUpAsync(cancellationToken).AsTask();

    /// <summary>Builds the empty append that names <paramref name="prevLogIndex" /> as its predecessor.</summary>
    /// <param name="prevLogIndex">The predecessor index.</param>
    /// <returns>The empty batch.</returns>
    private static FollowerBatch Probe(ulong prevLogIndex) => new([], Header.LeaderNodeId, Header.Term, prevLogIndex, 0, prevLogIndex);
}
