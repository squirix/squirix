using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Cluster.Replication.ReplicaSenderTestKit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Failures, timeouts, backlog limits, and disposal of one <see cref="ReplicaFollowerSender" />.</summary>
[Immutable]
public sealed class ReplicaFollowerSenderFailureTests : ServerUnitTestBase
{
    /// <summary>A request that waits for the follower longer than the append timeout is canceled and failed, and the next entry is then sent.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TimedOutAppendUnblocksNext(CancellationToken cancellationToken)
    {
        var time = new FakeTimeProvider();
        var gateway = new ParkingFollowerGateway { ObservesCancellation = true };
        var sender = new ReplicaFollowerSender(gateway, "n2", in Header, 0, 0, TimeSpan.FromSeconds(5)) { TimeProvider = time };
        try
        {
            var first = EnqueueAsync(sender, 1);
            var second = EnqueueAsync(sender, 2);
            _ = await BoundedAsync(gateway.CallAsync(0), cancellationToken);

            time.Advance(TimeSpan.FromSeconds(5));
            _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(BoundedAsync(first, cancellationToken));
            var next = await BoundedAsync(gateway.CallAsync(1), cancellationToken);
            next.Accept();

            _ = await Assert.That((await BoundedAsync(second, cancellationToken)).LogIndex).IsEqualTo(2UL);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>An entry beyond the backlog cap fails at once, without waiting for the follower.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BacklogBeyondCapFailsAtOnce(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = new ReplicaFollowerSender(gateway, "n2", in Header, 0, 0, HangGuard) { MaxPendingEntries = 2 };
        try
        {
            _ = EnqueueAsync(sender, 1);
            _ = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            var second = EnqueueAsync(sender, 2);
            var third = EnqueueAsync(sender, 3);

            var rejected = EnqueueAsync(sender, 4);

            _ = await Assert.That(rejected.IsFaulted).IsTrue();
            var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(rejected);
            _ = await Assert.That(error.Message).IsEqualTo("follower append backlog full");
            _ = await Assert.That(second.IsCompleted).IsFalse();
            _ = await Assert.That(third.IsCompleted).IsFalse();
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>A byte backlog beyond its cap fails the entry, while a single entry above the cap is still accepted when nothing waits.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ByteBacklogCapFailsAtOnce(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = new ReplicaFollowerSender(gateway, "n2", in Header, 0, 0, HangGuard) { MaxPendingBytes = 10 };
        try
        {
            _ = EnqueueAsync(sender, 1, payloadBytes: 20);
            _ = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            var second = EnqueueAsync(sender, 2, payloadBytes: 20);
            var rejected = EnqueueAsync(sender, 3);

            _ = await Assert.That(second.IsCompleted).IsFalse();
            _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(rejected);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>Disposing a sender with a request in flight cancels it and fails the entries still waiting, and a later enqueue fails too.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeFailsPendingAndCancels(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway { ObservesCancellation = true };
        var sender = CreateSender(gateway);
        try
        {
            var inFlight = EnqueueAsync(sender, 1);
            var waiting = EnqueueAsync(sender, 2);
            var call = await BoundedAsync(gateway.CallAsync(0), cancellationToken);

            await sender.DisposeAsync().AsTask().WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            _ = await Assert.That(call.Token.IsCancellationRequested).IsTrue();
            _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(BoundedAsync(inFlight, cancellationToken));
            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(BoundedAsync(waiting, cancellationToken));
            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(EnqueueAsync(sender, 3));
            _ = await Assert.That(gateway.CallCount).IsEqualTo(1);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>A gateway that ignores cancellation does not hold dispose past the shutdown budget, dispose does not throw, and the leak is reported.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeBoundedWhenCancelIgnored(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var reported = TimeSpan.Zero;
        var sender = new ReplicaFollowerSender(gateway, "n2", in Header, 0, 0, HangGuard)
        {
            ShutdownBudget = TimeSpan.FromMilliseconds(100),
            ShutdownLeakReporter = budget => reported = budget,
        };
        try
        {
            var inFlight = EnqueueAsync(sender, 1);
            _ = await BoundedAsync(gateway.CallAsync(0), cancellationToken);

            await sender.DisposeAsync().AsTask().WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            _ = await Assert.That(inFlight.IsCompleted).IsFalse();
            _ = await Assert.That(reported).IsEqualTo(TimeSpan.FromMilliseconds(100));
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>A follower that fails the transport gets each entry once: the failure fails its request and nothing is sent again.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailingFollowerIsNotRetried(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            var first = EnqueueAsync(sender, 1);
            var second = EnqueueAsync(sender, 2);
            var third = EnqueueAsync(sender, 3);
            (await BoundedAsync(gateway.CallAsync(0), cancellationToken)).Fail(new IOException("follower is down"));
            (await BoundedAsync(gateway.CallAsync(1), cancellationToken)).Fail(new IOException("follower is down"));

            _ = await NodeAsyncAssert.ThrowsAsync<IOException>(BoundedAsync(first, cancellationToken));
            _ = await NodeAsyncAssert.ThrowsAsync<IOException>(BoundedAsync(second, cancellationToken));
            _ = await NodeAsyncAssert.ThrowsAsync<IOException>(BoundedAsync(third, cancellationToken));
            _ = await Assert.That(gateway.CallCount).IsEqualTo(2);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>A sender that was given no entry never calls the follower.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task IdleSenderSendsNothing(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);

        await sender.DisposeAsync().AsTask().WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        _ = await Assert.That(gateway.CallCount).IsEqualTo(0);
    }

    /// <summary>An entry that does not follow the entries enqueued before it is refused instead of being sent out of order.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OutOfOrderEnqueueFails(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            _ = EnqueueAsync(sender, 2);
            _ = await BoundedAsync(gateway.CallAsync(0), cancellationToken);

            var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(EnqueueAsync(sender, 2));

            _ = await Assert.That(error.Message).IsEqualTo("Follower 'n2' append out of order: index 2 after 2.");
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>A refused request fails the entries it carried with the refusal, and the entries behind it are still sent, once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusalFailsItsBatchOnly(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            var first = EnqueueAsync(sender, 1);
            var second = EnqueueAsync(sender, 2);
            var third = EnqueueAsync(sender, 3);
            (await BoundedAsync(gateway.CallAsync(0), cancellationToken)).Refuse(FollowerLogRefusal.LogMismatch);

            var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(BoundedAsync(first, cancellationToken));
            var later = await BoundedAsync(gateway.CallAsync(1), cancellationToken);
            later.Accept();

            _ = await Assert.That(error.Message).IsEqualTo($"Follower 'n2' refused append: {FollowerLogRefusal.LogMismatch}.");
            _ = await Assert.That(later.Count).IsEqualTo(2);
            _ = await Assert.That((await BoundedAsync(second, cancellationToken)).LogIndex).IsEqualTo(2UL);
            _ = await Assert.That((await BoundedAsync(third, cancellationToken)).LogIndex).IsEqualTo(3UL);
            _ = await Assert.That(gateway.CallCount).IsEqualTo(2);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }
}
