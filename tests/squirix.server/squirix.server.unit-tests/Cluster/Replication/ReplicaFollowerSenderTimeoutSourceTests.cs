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

/// <summary>The timeout source a <see cref="ReplicaFollowerSender" /> reuses for its live appends.</summary>
[Immutable]
public sealed class ReplicaFollowerSenderTimeoutSourceTests : ServerUnitTestBase
{
    private static readonly TimeSpan AppendTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Consecutive appends that did not time out share one source.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <remarks>Runs on the system clock: a source whose timer runs on a fake clock cannot be reset, so only the system clock exercises the reuse.</remarks>
    [Test]
    public async Task ConsecutiveAppendsReuseTheSource(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway { ObservesCancellation = true };
        var sender = CreateSender(gateway, HangGuard);
        try
        {
            var first = EnqueueAsync(sender, 1);
            var firstCall = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            firstCall.Accept();
            _ = await BoundedAsync(first, cancellationToken);

            var second = EnqueueAsync(sender, 2);
            var secondCall = await BoundedAsync(gateway.CallAsync(1), cancellationToken);

            _ = await Assert.That(secondCall.Token.Equals(firstCall.Token)).IsTrue();
            _ = await Assert.That(sender.AppendSourcesCreated).IsEqualTo(1);
            _ = await Assert.That(secondCall.Token.IsCancellationRequested).IsFalse();

            secondCall.Accept();
            _ = await BoundedAsync(second, cancellationToken);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>An append that timed out leaves a canceled source behind, so the next append gets a fresh one that is not canceled.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TimedOutAppendGetsAFreshSourceNext(CancellationToken cancellationToken)
    {
        var time = new FakeTimeProvider();
        var gateway = new ParkingFollowerGateway { ObservesCancellation = true };
        var sender = CreateTimedSender(gateway, time, AppendTimeout);
        try
        {
            var first = EnqueueAsync(sender, 1);
            var second = EnqueueAsync(sender, 2);
            var firstCall = await BoundedAsync(gateway.CallAsync(0), cancellationToken);

            time.Advance(AppendTimeout);
            _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(BoundedAsync(first, cancellationToken));
            var secondCall = await BoundedAsync(gateway.CallAsync(1), cancellationToken);

            _ = await Assert.That(secondCall.Token.Equals(firstCall.Token)).IsFalse();
            _ = await Assert.That(secondCall.Token.IsCancellationRequested).IsFalse();
            _ = await Assert.That(sender.AppendSourcesCreated).IsEqualTo(2);

            secondCall.Accept();
            _ = await BoundedAsync(second, cancellationToken);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>Closing the sender cancels an append that runs on a reused source.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CloseCancelsAppendOnReusedSource(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway { ObservesCancellation = true };
        var sender = CreateSender(gateway, HangGuard);
        try
        {
            var first = EnqueueAsync(sender, 1);
            var firstCall = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            firstCall.Accept();
            _ = await BoundedAsync(first, cancellationToken);

            var second = EnqueueAsync(sender, 2);
            var secondCall = await BoundedAsync(gateway.CallAsync(1), cancellationToken);
            _ = await Assert.That(secondCall.Token.Equals(firstCall.Token)).IsTrue();

            await sender.DisposeAsync().AsTask().WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            _ = await Assert.That(secondCall.Token.IsCancellationRequested).IsTrue();
            _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(BoundedAsync(second, cancellationToken));
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }
}
