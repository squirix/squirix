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
using Xunit;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Unit tests for node-level backpressure admission control.</summary>
[Immutable]
public sealed class BackpressureGateTests : DisposableServerUnitTestBase
{
    private readonly Meter _testMeter = new("test");

    /// <summary>Verifies disabled backpressure returns an accepted empty lease and emits bypass metrics.</summary>
    [Fact]
    public async Task AcquireBypassesDisabledBackpressure()
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

        var (decision, lease) = await gate.AcquireAsync("rest", "insert", "rest:client-a", DefaultCancellationToken);
        lease.Dispose();

        Assert.True(decision.IsAccepted);
        Assert.True(sink.HasEvent("squirix_backpressure_bypass_total", ("transport", "rest"), ("op", "insert")));
    }

    /// <summary>Verifies admission succeeds immediately while slots are available.</summary>
    [Fact]
    public async Task AcquireSucceedsWithFreeCapacity()
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

        var (decision, lease) = await gate.AcquireAsync("grpc", "get", "grpc:client-a", DefaultCancellationToken);
        using (lease)
        {
            Assert.True(decision.IsAccepted);
            Assert.Null(decision.RejectReason);
        }
    }

    /// <summary>Verifies concurrent acquire and release does not exceed configured in-flight capacity.</summary>
    [Fact]
    public async Task ConcurrencyCannotExceedConfiguredCap()
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
            clients[i] = RunClientAsync(gateForClients, i, current, observedMax, DefaultCancellationToken);

        var runClients = Task.WhenAll(clients);

        await runClients;

        Assert.True(observedMax[0] <= maxInFlight);
    }

    /// <summary>Verifies disposing the same lease twice follows current release behavior.</summary>
    [Fact]
    public async Task LeaseDoubleDisposeKeepsCurrentBehavior()
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

        var lease = (await gate.AcquireAsync("grpc", "get", "grpc:client-a", DefaultCancellationToken)).Lease;
        lease.Dispose();
        _ = NodeExceptionAssert.For<SemaphoreFullException>().Throws(lease, static value => value.Dispose());
    }

    /// <summary>Verifies requests are rejected once the hard threshold is reached while another request is queued.</summary>
    [Fact]
    public async Task QueueFullRejectsImmediately()
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

        var first = (await gate.AcquireAsync("grpc", "insert", "grpc:client-a", DefaultCancellationToken)).Lease;
        using var secondCts = new CancellationTokenSource();

        // With MaxSlowdownDelay = 0 the queued acquire runs synchronously up to the queue-slot
        // await, so client-b is already counted in the queue depth (incremented before that
        // await suspends) by the time the task is created - no wall-clock delay is needed.
        var secondAcquire = gate.AcquireAsync("grpc", "insert", "grpc:client-b", secondCts.Token).AsTask();

        var (decision, rejectedLease) = await gate.AcquireAsync("grpc", "insert", "grpc:client-c", DefaultCancellationToken);
        rejectedLease.Dispose();

        // Check the reason first so a recurring flake reports the observed rejection instead of
        // a bare boolean failure.
        Assert.Equal("hard_threshold", decision.RejectReason);
        Assert.False(decision.IsAccepted);
        Assert.True(sink.HasEvent("squirix_backpressure_reject_total", ("transport", "grpc"), ("op", "insert"), ("reason", "hard_threshold")));

        await secondCts.CancelAsync();
        await WaitUntilCanceledAsync(secondAcquire);
        first.Dispose();
    }

    /// <summary>Verifies a queued request is rejected after exceeding the configured queue wait budget.</summary>
    [Fact]
    public async Task QueueTimeoutRejectsAndEmitsMetrics()
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

        using var lease = (await gate.AcquireAsync("rest", "get", "rest:client-a", DefaultCancellationToken)).Lease;

        var (decision, queuedLease) = await gate.AcquireAsync("rest", "get", "rest:client-b", DefaultCancellationToken);
        queuedLease.Dispose();

        Assert.False(decision.IsAccepted);
        Assert.Equal("queue_wait_timeout", decision.RejectReason);
        Assert.True(sink.HasEvent("squirix_backpressure_reject_total", ("transport", "rest"), ("op", "get"), ("reason", "queue_wait_timeout")));
        Assert.True(sink.HasEvent("squirix_backpressure_queue_timeouts_total", ("transport", "rest"), ("op", "get")));
    }

    /// <summary>Verifies a queued acquire completes after a held lease is released.</summary>
    [Fact]
    public async Task QueuedAcquireCompletesAfterLeaseRelease()
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

        var first = (await gate.AcquireAsync("grpc", "insert", "grpc:client-a", DefaultCancellationToken)).Lease;
        var queuedTask = gate.AcquireAsync("grpc", "insert", "grpc:client-b", DefaultCancellationToken).AsTask();

        Assert.False(queuedTask.IsCompleted);
        first.Dispose();

        var (decision, secondLease) = await queuedTask;
        using (secondLease)
            Assert.True(decision.IsAccepted);
    }

    /// <summary>Verifies queued admission observes caller cancellation and records queue cancellation metrics.</summary>
    [Fact]
    public async Task QueuedAcquireObservesCallerCancellation()
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

        using var heldLease = (await gate.AcquireAsync("rest", "remove", "rest:client-a", DefaultCancellationToken)).Lease;
        using var cts = new CancellationTokenSource();
        var queuedTask = gate.AcquireAsync("rest", "remove", "rest:client-b", cts.Token).AsTask();

        Assert.False(queuedTask.IsCompleted);
        await cts.CancelAsync();

        await WaitUntilCanceledAsync(queuedTask);
        Assert.True(queuedTask.IsCanceled);
        Assert.True(sink.HasEvent("squirix_backpressure_queue_cancellations_total", ("transport", "rest"), ("op", "remove")));
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

    private static async Task WaitUntilCanceledAsync(Task task)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (!task.IsCompleted && DateTime.UtcNow < deadline)
            await Task.Delay(TimeSpan.FromMilliseconds(10), TimeProvider.System, DefaultCancellationToken);

        Assert.True(task.IsCanceled);
    }
}
