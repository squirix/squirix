using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Testing;
using Squirix.Transport;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Mocks;
using TUnit.Mocks.Arguments;
using TUnit.Mocks.Generated;

namespace Squirix.E2ETests.Cache.MultiNode.Failover;

/// <summary>The failover fixtures refuse a fake election clock before any node starts, and the bounded retry records every attempt.</summary>
public sealed class FixtureTests
{
    private const string PendingReplicaApplyMessage = "Server is overloaded (replica_apply_pending).";
    private const bool RetryPendingReplicaApply = true;

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

        var sharedFailure = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, HostedCluster>(HostedCluster.StartThreeNodeAsync(options: shared, cancellationToken: cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        var perNodeFailure = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, HostedCluster>(HostedCluster.StartThreeNodeAsync(options: perNode, cancellationToken: cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(sharedFailure.Message).StartsWith("Node nodeA runs automatic failover on a fake clock", StringComparison.Ordinal);
        _ = await Assert.That(perNodeFailure.Message).StartsWith("Node nodeC runs automatic failover on a fake clock", StringComparison.Ordinal);
    }

    /// <summary>A repeated attempt of a step names registers of its own, so it never reads a value an earlier attempt left in the cache.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task AttemptsWriteDistinctRegisters()
    {
        var first = FailoverSteps.AttemptPrefix("stop10", 1);
        var second = FailoverSteps.AttemptPrefix("stop10", 2);
        var third = FailoverSteps.AttemptPrefix("stop10", 3);

        _ = await Assert.That(first).IsEqualTo("stop10");
        _ = await Assert.That([first, second, third]).HasDistinctItems();
        _ = await Assert.That(FailoverSteps.KeysOf("c", "nodeA", first, 1)[0]).IsNotEqualTo(FailoverSteps.KeysOf("c", "nodeA", second, 1)[0]);
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
        var calls = new Queue<WireStatus>([WireStatus.Unavailable, WireStatus.DeadlineExceeded]);
        var attempts = new List<EventualAttempt>();

        var result = await Eventually.SucceedsAsync(
            calls,
            static (pending, _) => pending.TryDequeue(out var status) ? Task.FromException<int>(new WireCallException(status, "fault")) : Task.FromResult(7),
            TimeSpan.FromSeconds(30),
            attempts,
            cancellationToken);

        _ = await Assert.That(result).IsEqualTo(7);
        _ = await Assert.That(string.Join(", ", attempts.ConvertAll(static a => a.Outcome))).IsEqualTo("WireCallException Unavailable, WireCallException DeadlineExceeded, ok");
    }

    /// <summary>A recovery probe can explicitly retry a write refused while earlier entries are being applied.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetriesPendingReplicaApplyOnProbeWrite(CancellationToken cancellationToken)
    {
        var error = new SquirixTransportException(new CallStatus(CacheTransportStatusCode.ResourceExhausted, PendingReplicaApplyMessage));
        var attempts = new List<EventualAttempt>();
        var cache = ICache<long>.Mock(MockBehavior.Strict);
        var writes = 0;
        _ = cache.SetAsync("probe", 7L, Arg.Any<CacheEntryOptions?>(), Arg.Any<CancellationToken>())
                 .ReturnsAsync((_, _, _, _) => Interlocked.Increment(ref writes) == 1 ? Task.FromException(error) : Task.CompletedTask);
        _ = cache.GetValueAsync("probe", Arg.Any<CancellationToken>()).ReturnsAsync(Task.FromResult(new CacheValueResult<long>(true, 8L)));

        var (read, _) = await FailoverSteps.RecoverAsync(cache.Object, "probe", 7L, Stopwatch.GetTimestamp(), attempts, cancellationToken, RetryPendingReplicaApply);

        _ = await Assert.That(writes).IsEqualTo(2);
        _ = await Assert.That(read).IsEqualTo(new CacheValueResult<long>(true, 8L));
    }

    /// <summary>The probe propagates a write refusal unless that refusal is eligible for a retry.</summary>
    /// <param name="message">The SDK error detail returned for the write refusal.</param>
    /// <param name="payloadTooLarge">Whether the SDK marked the refusal as a payload size error.</param>
    /// <param name="enablePendingReplicaApplyRetry">Whether the probe explicitly enables the pending replica apply retry.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(PendingReplicaApplyMessage, false, false)]
    [Arguments("Payload size limit is 128 bytes.", true, true)]
    public async Task ProbeWriteRefusalEndsOnFirstAttempt(string message, bool payloadTooLarge, bool enablePendingReplicaApplyRetry, CancellationToken cancellationToken)
    {
        var error = new SquirixTransportException(new CallStatus(CacheTransportStatusCode.ResourceExhausted, message, PayloadTooLarge: payloadTooLarge));
        var attempts = new List<EventualAttempt>();
        var cache = ICache<long>.Mock(MockBehavior.Strict);
        _ = cache.SetAsync("probe", 7L, Arg.Any<CacheEntryOptions?>(), Arg.Any<CancellationToken>())
                 .ReturnsAsync((_, _, _, _) => Task.FromException(error));

        var started = Stopwatch.GetTimestamp();
        var recovery = enablePendingReplicaApplyRetry
            ? FailoverSteps.RecoverAsync(cache.Object, "probe", 7L, started, attempts, cancellationToken, RetryPendingReplicaApply)
            : FailoverSteps.RecoverAsync(cache.Object, "probe", 7L, started, attempts, cancellationToken);
        var failure = await NodeAsyncAssert.ThrowsAsync<SquirixTransportException>(recovery, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(ReferenceEquals(error, failure)).IsTrue();
        _ = await Assert.That(attempts).HasSingleItem();
    }

    /// <summary>The write retry opt-in leaves a refusal of the subsequent read visible.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PendingReplicaApplyEscapesProbeRead(CancellationToken cancellationToken)
    {
        var error = new SquirixTransportException(new CallStatus(CacheTransportStatusCode.ResourceExhausted, PendingReplicaApplyMessage));
        var attempts = new List<EventualAttempt>();
        var cache = ICache<long>.Mock(MockBehavior.Strict);
        _ = cache.SetAsync("probe", 7L, Arg.Any<CacheEntryOptions?>(), Arg.Any<CancellationToken>()).ReturnsAsync(static (_, _, _, _) => Task.CompletedTask);
        _ = cache.GetValueAsync("probe", Arg.Any<CancellationToken>()).ReturnsAsync(Task.FromException<CacheValueResult<long>>(error));

        var failure = await NodeAsyncAssert.ThrowsAsync<SquirixTransportException>(
            FailoverSteps.RecoverAsync(cache.Object, "probe", 7L, Stopwatch.GetTimestamp(), attempts, cancellationToken, RetryPendingReplicaApply),
            TimeSpan.FromSeconds(30),
            TimeProvider.System,
            cancellationToken);

        _ = await Assert.That(ReferenceEquals(error, failure)).IsTrue();
        _ = await Assert.That(attempts).HasSingleItem();
    }

    /// <summary>Any other failure ends the retry at once and is rethrown after it is recorded.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OtherFailureIsNotRetried(CancellationToken cancellationToken)
    {
        var attempts = new List<EventualAttempt>();

        var failure = await NodeAsyncAssert.ThrowsAsync<WireCallException>(
            Eventually.SucceedsAsync(
                0,
                static (_, _) => Task.FromException(new WireCallException(WireStatus.FailedPrecondition, "wrong")),
                TimeSpan.FromSeconds(30),
                attempts,
                cancellationToken),
            TimeSpan.FromSeconds(30),
            TimeProvider.System,
            cancellationToken);

        _ = await Assert.That(failure.StatusCode).IsEqualTo(WireStatus.FailedPrecondition);
        _ = await Assert.That(attempts).HasSingleItem();
        _ = await Assert.That(attempts[0].Outcome).IsEqualTo("WireCallException FailedPrecondition");
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
                static (_, _) => Task.FromException(new WireCallException(WireStatus.Unavailable, "down")),
                TimeSpan.FromSeconds(2),
                attempts,
                cancellationToken),
            TimeSpan.FromSeconds(30),
            TimeProvider.System,
            cancellationToken);

        _ = await Assert.That(attempts.Count).IsGreaterThanOrEqualTo(1);
        for (var i = 1; i <= attempts.Count; i++)
            _ = await Assert.That(failure.Message).Contains($"attempt {i}:", StringComparison.Ordinal);

        _ = await Assert.That(failure.Message).DoesNotContain($"attempt {attempts.Count + 1}:", StringComparison.Ordinal);
    }
}
