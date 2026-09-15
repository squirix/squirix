using System;
using System.Diagnostics.Metrics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Unit tests for node-level backpressure admission control.</summary>
[Immutable]
public sealed class BackpressureGateTests : DisposableServerUnitTestBase
{
    private readonly Meter _testMeter = new("test");

    /// <summary>Verifies disabled backpressure returns an accepted empty lease and emits bypass metrics.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AcquireBypassesDisabledBackpressure(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                Enabled = false,
                MaxInFlight = 1,
                MaxQueue = 0,
                SlowdownThreshold = 1,
                RejectThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(200),
            },
            new BackpressureMetrics(meter));

        var (decision, lease) = await gate.AcquireAsync("rest", "insert", "rest:client-a", cancellationToken);
        lease.Dispose();

        _ = await Assert.That(decision.IsAccepted).IsTrue();
        _ = await Assert.That(sink.HasEvent("squirix_backpressure_bypass_total", ("transport", "rest"), ("op", "insert"))).IsTrue();
    }

    /// <summary>Verifies admission succeeds immediately while slots are available.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AcquireSucceedsWithFreeCapacity(CancellationToken cancellationToken)
    {
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 2,
                MaxQueue = 1,
                SlowdownThreshold = 2,
                RejectThreshold = 2,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(200),
            },
            new BackpressureMetrics(_testMeter));

        var (decision, lease) = await gate.AcquireAsync("grpc", "get", "grpc:client-a", cancellationToken);
        using (lease)
        {
            _ = await Assert.That(decision.IsAccepted).IsTrue();
            _ = await Assert.That(decision.RejectReason).IsNull();
        }
    }

    /// <summary>Verifies concurrent acquire and release does not exceed configured in-flight capacity.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrencyCannotExceedConfiguredCap(CancellationToken cancellationToken)
    {
        const int maxInFlight = 3;
        var backpressureOptions = new AdmissionOptions
        {
            MaxInFlight = maxInFlight,
            MaxQueue = 64,
            SlowdownThreshold = maxInFlight,
            RejectThreshold = maxInFlight,
            MaxSlowdownDelay = TimeSpan.Zero,
            MaxQueueWait = TimeSpan.FromSeconds(2),
        };
        using var gate = new AdmissionGate(backpressureOptions, new BackpressureMetrics(_testMeter));
        IBackpressureGate gateForClients = gate;
        var current = new int[1];
        var observedMax = new int[1];
        var clients = new Task[24];
        for (var i = 0; i < clients.Length; i++)
            clients[i] = RunClientAsync(gateForClients, i, current, observedMax, cancellationToken);

        var runClients = Task.WhenAll(clients);

        await runClients;

        _ = await Assert.That(observedMax[0] <= maxInFlight).IsTrue();
    }

    /// <summary>Verifies disposing the same lease twice releases its slot exactly once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeaseDoubleDisposeReleasesOnce(CancellationToken cancellationToken)
    {
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 1,
                MaxQueue = 1,
                SlowdownThreshold = 1,
                RejectThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(200),
            },
            new BackpressureMetrics(_testMeter));

        var lease = (await gate.AcquireAsync("grpc", "get", "grpc:client-a", cancellationToken)).Lease;
        lease.Dispose();
        lease.Dispose();

        var (decision, secondLease) = await gate.AcquireAsync("grpc", "get", "grpc:client-a", cancellationToken);
        using (secondLease)
            _ = await Assert.That(decision.IsAccepted).IsTrue();
    }

    /// <summary>Verifies requests are rejected once the hard threshold is reached while another request is queued.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QueueFullRejectsImmediately(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 1,
                MaxQueue = 1,
                SlowdownThreshold = 1,
                RejectThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(200),
            },
            new BackpressureMetrics(meter));

        var first = (await gate.AcquireAsync("grpc", "insert", "grpc:client-a", cancellationToken)).Lease;
        using var secondCts = new CancellationTokenSource();

        // With MaxSlowdownDelay = 0 the queued acquire runs synchronously up to the queue-slot
        // await, so client-b is already counted in the queue depth (incremented before that
        // await suspends) by the time the task is created - no wall-clock delay is needed.
        var secondAcquire = gate.AcquireAsync("grpc", "insert", "grpc:client-b", secondCts.Token).AsTask();

        var (decision, rejectedLease) = await gate.AcquireAsync("grpc", "insert", "grpc:client-c", cancellationToken);
        rejectedLease.Dispose();

        // Check the reason first so a recurring flake reports the observed rejection instead of
        // a bare boolean failure.
        _ = await Assert.That(decision.RejectReason).IsEqualTo("hard_threshold");
        _ = await Assert.That(decision.IsAccepted).IsFalse();
        _ = await Assert.That(sink.HasEvent("squirix_backpressure_reject_total", ("transport", "grpc"), ("op", "insert"), ("reason", "hard_threshold"))).IsTrue();

        await secondCts.CancelAsync();
        await secondAcquire.WaitUntilAsync(static t => t.IsCompleted, cancellationToken);
        _ = await Assert.That(secondAcquire.IsCanceled).IsTrue();
        first.Dispose();
    }

    /// <summary>Verifies a queued request is rejected after exceeding the configured queue wait budget.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QueueTimeoutRejectsAndEmitsMetrics(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 1,
                MaxQueue = 1,
                SlowdownThreshold = 1,
                RejectThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(40),
            },
            new BackpressureMetrics(meter));

        using var lease = (await gate.AcquireAsync("rest", "get", "rest:client-a", cancellationToken)).Lease;

        var (decision, queuedLease) = await gate.AcquireAsync("rest", "get", "rest:client-b", cancellationToken);
        queuedLease.Dispose();

        _ = await Assert.That(decision.IsAccepted).IsFalse();
        _ = await Assert.That(decision.RejectReason).IsEqualTo("queue_wait_timeout");
        _ = await Assert.That(sink.HasEvent("squirix_backpressure_reject_total", ("transport", "rest"), ("op", "get"), ("reason", "queue_wait_timeout"))).IsTrue();
        _ = await Assert.That(sink.HasEvent("squirix_backpressure_queue_timeouts_total", ("transport", "rest"), ("op", "get"))).IsTrue();
    }

    /// <summary>Verifies a queued acquire completes after a held lease is released.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QueuedAcquireCompletesAfterLeaseRelease(CancellationToken cancellationToken)
    {
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 1,
                MaxQueue = 1,
                SlowdownThreshold = 1,
                RejectThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(500),
            },
            new BackpressureMetrics(_testMeter));

        var first = (await gate.AcquireAsync("grpc", "insert", "grpc:client-a", cancellationToken)).Lease;
        var queuedTask = gate.AcquireAsync("grpc", "insert", "grpc:client-b", cancellationToken).AsTask();

        _ = await Assert.That(queuedTask.IsCompleted).IsFalse();
        first.Dispose();

        var (decision, secondLease) = await queuedTask;
        using (secondLease)
            _ = await Assert.That(decision.IsAccepted).IsTrue();
    }

    /// <summary>Verifies queued admission observes caller cancellation and records queue cancellation metrics.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QueuedAcquireObservesCallerCancellation(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 1,
                MaxQueue = 1,
                SlowdownThreshold = 1,
                RejectThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromSeconds(2),
            },
            new BackpressureMetrics(meter));

        using var heldLease = (await gate.AcquireAsync("rest", "remove", "rest:client-a", cancellationToken)).Lease;
        using var cts = new CancellationTokenSource();
        var queuedTask = gate.AcquireAsync("rest", "remove", "rest:client-b", cts.Token).AsTask();

        _ = await Assert.That(queuedTask.IsCompleted).IsFalse();
        await cts.CancelAsync();

        await queuedTask.WaitUntilAsync(static t => t.IsCompleted, cancellationToken);
        _ = await Assert.That(queuedTask.IsCanceled).IsTrue();
        _ = await Assert.That(sink.HasEvent("squirix_backpressure_queue_cancellations_total", ("transport", "rest"), ("op", "remove"))).IsTrue();
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static async Task RunClientAsync(IBackpressureGate gate, int clientIndex, int[] current, int[] observedMax, CancellationToken cancellationToken)
    {
        var (decision, lease) = await gate.AcquireAsync("grpc", "insert", $"grpc:client-{NodeInvariantIndexStrings.Format(clientIndex)}", cancellationToken);
        if (!decision.IsAccepted)
            return;

        using (lease)
        {
            var now = Interlocked.Increment(ref MemoryMarshal.GetArrayDataReference(current));
            UpdateMax(now, ref MemoryMarshal.GetArrayDataReference(observedMax));

            try
            {
                await Task.Yield();
            }
            finally
            {
                _ = Interlocked.Decrement(ref MemoryMarshal.GetArrayDataReference(current));
            }
        }
    }

    private static void UpdateMax(int candidate, ref int target)
    {
        while (true)
        {
            var current = Volatile.Read(ref target);
            if (candidate <= current)
                return;

            if (Interlocked.CompareExchange(ref target, candidate, current) == current)
                return;
        }
    }
}
