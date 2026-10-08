using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Time.Testing;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode.Failover;

/// <summary>The failover fixtures refuse a fake election clock before any node starts, and the bounded retry records every attempt.</summary>
public sealed class FixtureTests : EndToEndTestBase
{
    /// <summary>A failover cluster on a fake clock, shared or per node, fails to start before any node runs.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailoverRejectsFakeClock(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var shared = new MultiNodeStartOptions { ReplicaCount = 3, Failover = true, TimeProvider = clock };
        var perNode = new MultiNodeStartOptions
        {
            ReplicaCount = 3,
            Failover = true,
            NodeClock = nodeId => string.Equals(nodeId, "nodeC", StringComparison.Ordinal) ? clock : null,
        };

        var sharedFailure = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, HostedCluster>(HostedCluster.StartThreeNodeAsync(options: shared, cancellationToken: cancellationToken));
        var perNodeFailure = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, HostedCluster>(HostedCluster.StartThreeNodeAsync(options: perNode, cancellationToken: cancellationToken));

        _ = await Assert.That(sharedFailure.Message).StartsWith("Node nodeA runs automatic failover on a fake clock", StringComparison.Ordinal);
        _ = await Assert.That(perNodeFailure.Message).StartsWith("Node nodeC runs automatic failover on a fake clock", StringComparison.Ordinal);
    }

    /// <summary>The node clock wins over the shared clock, and a node without its own clock falls back to the shared one.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task NodeClockOverridesSharedClock()
    {
        var shared = new FakeTimeProvider();
        var skewed = new SkewedTimeProvider(TimeSpan.FromSeconds(-30));
        var options = new MultiNodeStartOptions
        {
            TimeProvider = shared,
            NodeClock = nodeId => string.Equals(nodeId, "nodeB", StringComparison.Ordinal) ? skewed : null,
        };

        _ = await Assert.That(options.ClockFor("nodeB")).IsSameReferenceAs(skewed);
        _ = await Assert.That(options.ClockFor("nodeA")).IsSameReferenceAs(shared);
    }

    /// <summary>Unavailable and deadline failures are retried until the call succeeds, and every attempt is recorded in order.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetriesUnavailableUntilSuccess(CancellationToken cancellationToken)
    {
        var calls = new Queue<StatusCode>([StatusCode.Unavailable, StatusCode.DeadlineExceeded]);
        var attempts = new List<EventualAttempt>();

        var result = await Eventually.SucceedsAsync(
            calls,
            static (pending, _) => pending.TryDequeue(out var status) ? Task.FromException<int>(new RpcException(new Status(status, "fault"))) : Task.FromResult(7),
            TimeSpan.FromSeconds(30),
            attempts,
            cancellationToken);

        _ = await Assert.That(result).IsEqualTo(7);
        _ = await Assert.That(string.Join(", ", attempts.ConvertAll(static a => a.Outcome))).IsEqualTo("RpcException Unavailable, RpcException DeadlineExceeded, ok");
    }

    /// <summary>Any other failure ends the retry at once and is rethrown after it is recorded.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OtherFailureIsNotRetried(CancellationToken cancellationToken)
    {
        var attempts = new List<EventualAttempt>();

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            Eventually.SucceedsAsync(
                0,
                static (_, _) => Task.FromException(new RpcException(new Status(StatusCode.FailedPrecondition, "wrong"))),
                TimeSpan.FromSeconds(30),
                attempts,
                cancellationToken));

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.FailedPrecondition);
        _ = await Assert.That(attempts).HasSingleItem();
        _ = await Assert.That(attempts[0].Outcome).IsEqualTo("RpcException FailedPrecondition");
    }

    /// <summary>A call that stays unavailable ends with a timeout that lists every attempt once the bound is spent.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SpentBoundListsEveryAttempt(CancellationToken cancellationToken)
    {
        var attempts = new List<EventualAttempt>();

        var failure = await NodeAsyncAssert.ThrowsAsync<TimeoutException>(
            Eventually.SucceedsAsync(
                0,
                static (_, _) => Task.FromException(new RpcException(new Status(StatusCode.Unavailable, "down"))),
                TimeSpan.FromMilliseconds(500),
                attempts,
                cancellationToken));

        _ = await Assert.That(attempts.Count).IsGreaterThanOrEqualTo(2);
        _ = await Assert.That(failure.Message).Contains($"attempt {attempts.Count}:", StringComparison.Ordinal);
    }
}
