using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Unit tests for the admission queue and slowdown behavior of node-level backpressure.</summary>
[Immutable]
public sealed class BackpressureAdmissionQueueTests : DisposableServerUnitTestBase
{
    private readonly Meter _testMeter = new("test");

    /// <summary>Verifies the queue fills to MaxQueue, rejects the next arrival as queue full and admits waiters in arrival order.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QueueFillsToMaxQueueThenRejects(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 1,
                MaxQueue = 3,
                SlowdownThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMinutes(1),
            },
            new BackpressureMetrics(meter));
        var held = (await gate.AcquireAsync("rest", "get", "rest:client-0", cancellationToken)).Lease;
        var queued1 = StartAcquireAsync(gate, "rest:client-1", cancellationToken);
        var queued2 = StartAcquireAsync(gate, "rest:client-2", cancellationToken);
        var queued3 = StartAcquireAsync(gate, "rest:client-3", cancellationToken);

        _ = await Assert.That(gate.QueueDepth).IsEqualTo(3);

        var (overflow, overflowLease) = await gate.AcquireAsync("rest", "get", "rest:client-9", cancellationToken);
        overflowLease.Dispose();
        _ = await Assert.That(overflow.RejectReason).IsEqualTo("queue_full");
        _ = await Assert.That(sink.HasEvent("squirix_backpressure_reject_total", ("transport", "rest"), ("op", "get"), ("reason", "queue_full"))).IsTrue();

        held.Dispose();
        var (decision1, lease1) = await queued1.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        _ = await Assert.That(decision1.IsAccepted).IsTrue();
        _ = await Assert.That(queued2.IsCompleted || queued3.IsCompleted).IsFalse();

        lease1.Dispose();
        var (decision2, lease2) = await queued2.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        _ = await Assert.That(decision2.IsAccepted).IsTrue();
        _ = await Assert.That(queued3.IsCompleted).IsFalse();

        lease2.Dispose();
        var (decision3, lease3) = await queued3.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        lease3.Dispose();
        _ = await Assert.That(decision3.IsAccepted).IsTrue();
    }

    /// <summary>Verifies two requests waking from slowdown together at a one-place queue yield exactly one queue-full rejection.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentWakeRejectsExactlyOneQueueFull(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 1,
                MaxQueue = 1,
                SlowdownThreshold = 1,
                MaxSlowdownDelay = TimeSpan.FromSeconds(1),
                MaxQueueWait = TimeSpan.FromMinutes(1),
            },
            new BackpressureMetrics(_testMeter),
            clock);
        var held = (await gate.AcquireAsync("rest", "get", "rest:client-0", cancellationToken)).Lease;
        using var firstCts = new CancellationTokenSource();
        using var secondCts = new CancellationTokenSource();
        var first = StartAcquireAsync(gate, "rest:client-a", firstCts.Token);
        var second = StartAcquireAsync(gate, "rest:client-b", secondCts.Token);
        _ = await Assert.That(first.IsCompleted || second.IsCompleted).IsFalse();

        clock.Advance(TimeSpan.FromSeconds(1));
        var tasks = new[] { first, second };
        await tasks.WaitUntilAsync(static t => t[0].IsCompleted || t[1].IsCompleted, cancellationToken);
        var rejected = first.IsCompleted ? first : second;
        var queued = first.IsCompleted ? second : first;
        var (decision, rejectedLease) = await rejected;
        rejectedLease.Dispose();
        _ = await Assert.That(decision.RejectReason).IsEqualTo("queue_full");

        await firstCts.CancelAsync();
        await secondCts.CancelAsync();
        await queued.WaitUntilAsync(static t => t.IsCompleted, cancellationToken);
        _ = await Assert.That(queued.IsCanceled).IsTrue();
        held.Dispose();
    }

    /// <summary>Verifies the slowdown delay ramps over the range from the slowdown threshold up to MaxInFlight.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SlowdownDelayScalesToMaxInFlight(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        var clock = new FakeTimeProvider();
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 4,
                MaxQueue = 1,
                SlowdownThreshold = 2,
                MaxSlowdownDelay = TimeSpan.FromSeconds(4),
            },
            new BackpressureMetrics(meter),
            clock);
        var first = (await gate.AcquireAsync("rest", "get", "rest:client-1", cancellationToken)).Lease;
        var second = (await gate.AcquireAsync("rest", "get", "rest:client-2", cancellationToken)).Lease;

        var third = StartAcquireAsync(gate, "rest:client-3", cancellationToken);
        clock.Advance(TimeSpan.FromMilliseconds(1999));
        _ = await Assert.That(third.IsCompleted).IsFalse();
        clock.Advance(TimeSpan.FromMilliseconds(1));
        var (thirdDecision, thirdLease) = await third;
        _ = await Assert.That(thirdDecision.IsAccepted).IsTrue();

        var fourth = StartAcquireAsync(gate, "rest:client-4", cancellationToken);
        clock.Advance(TimeSpan.FromMilliseconds(3999));
        _ = await Assert.That(fourth.IsCompleted).IsFalse();
        clock.Advance(TimeSpan.FromMilliseconds(1));
        var (fourthDecision, fourthLease) = await fourth;
        _ = await Assert.That(fourthDecision.IsAccepted).IsTrue();
        _ = await Assert.That(sink.HasEvent("squirix_backpressure_slowdown_total", ("transport", "rest"), ("op", "get"))).IsTrue();

        first.Dispose();
        second.Dispose();
        thirdLease.Dispose();
        fourthLease.Dispose();
    }

    /// <summary>Verifies a request that arrives with every slot taken and a free queue is slowed down and then queued, not rejected.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SaturatedRequestIsSlowedThenQueued(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 2,
                MaxQueue = 2,
                SlowdownThreshold = 2,
                MaxSlowdownDelay = TimeSpan.FromSeconds(1),
                MaxQueueWait = TimeSpan.FromMinutes(1),
            },
            new BackpressureMetrics(_testMeter),
            clock);
        var first = (await gate.AcquireAsync("rest", "get", "rest:client-1", cancellationToken)).Lease;
        var second = (await gate.AcquireAsync("rest", "get", "rest:client-2", cancellationToken)).Lease;

        var saturated = StartAcquireAsync(gate, "rest:client-3", cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(1));
        await gate.WaitUntilAsync(static g => g.QueueDepth == 1, cancellationToken);
        _ = await Assert.That(saturated.IsCompleted).IsFalse();

        first.Dispose();
        var (decision, lease) = await saturated.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        lease.Dispose();
        second.Dispose();

        _ = await Assert.That(decision.IsAccepted).IsTrue();
    }

    /// <summary>Verifies a queued request counts against the per-client limit, so the client cannot exceed it by queueing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QueuedRequestCountsAgainstClientLimit(CancellationToken cancellationToken)
    {
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 2,
                MaxQueue = 4,
                SlowdownThreshold = 2,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMinutes(1),
                PerClientMaxInFlight = 1,
            },
            new BackpressureMetrics(_testMeter));
        var heldB = (await gate.AcquireAsync("rest", "get", "rest:client-b", cancellationToken)).Lease;
        var heldC = (await gate.AcquireAsync("rest", "get", "rest:client-c", cancellationToken)).Lease;
        var queuedA = StartAcquireAsync(gate, "rest:client-a", cancellationToken);
        _ = await Assert.That(queuedA.IsCompleted).IsFalse();

        var (overLimit, overLimitLease) = await gate.AcquireAsync("rest", "get", "rest:client-a", cancellationToken);
        overLimitLease.Dispose();
        _ = await Assert.That(overLimit.RejectReason).IsEqualTo("client_concurrency_limit");

        heldB.Dispose();
        var (admitted, admittedLease) = await queuedA;
        _ = await Assert.That(admitted.IsAccepted).IsTrue();

        var (stillLimited, stillLimitedLease) = await gate.AcquireAsync("rest", "get", "rest:client-a", cancellationToken);
        stillLimitedLease.Dispose();
        _ = await Assert.That(stillLimited.RejectReason).IsEqualTo("client_concurrency_limit");

        admittedLease.Dispose();
        heldC.Dispose();
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static Task<(Decision Decision, Lease Lease)> StartAcquireAsync(AdmissionGate gate, string clientId, CancellationToken cancellationToken) =>
        gate.AcquireAsync("rest", "get", clientId, cancellationToken).AsTask();
}
