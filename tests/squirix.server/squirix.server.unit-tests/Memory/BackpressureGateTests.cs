using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Unit tests for node-level backpressure admission control.</summary>
[Immutable]
public sealed class BackpressureGateTests : ServerUnitTestBase
{
    private const string BackpressureInFlightInstrumentName = "squirix_backpressure_in_flight";
    private const string BackpressureQueueDepthInstrumentName = "squirix_backpressure_queue_depth";
    private const string BackpressureTrackedClientsInstrumentName = "squirix_backpressure_tracked_clients";
    private const string MeterName = "Squirix";

    /// <summary>Verifies disabled backpressure returns an accepted empty lease and emits bypass metrics.</summary>
    [Fact]
    public async Task AcquireBypassesDisabledBackpressure()
    {
        using var sink = new NodeMeasurementSink(MeterName);
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
            });

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
            });

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
        using var gate = new AdmissionGate(backpressureOptions);
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

    /// <summary>Verifies observable gauges report both in-flight work and queued requests.</summary>
    [Fact]
    public async Task GaugesReflectInFlightAndQueueDepth()
    {
        var inFlight = new List<int>();
        var queueDepth = new List<int>();
        var trackedClients = new List<int>();
        var measurements = new Dictionary<string, List<int>>(StringComparer.Ordinal)
        {
            [BackpressureInFlightInstrumentName] = inFlight,
            [BackpressureQueueDepthInstrumentName] = queueDepth,
            [BackpressureTrackedClientsInstrumentName] = trackedClients,
        }.ToFrozenDictionary(StringComparer.Ordinal);

        using var listener = CreateBackpressureGaugeListener(measurements);
        var backpressureOptions = new AdmissionOptions
        {
            MaxInFlight = 1,
            MaxQueue = 1,
            SlowdownThreshold = 1,
            RejectThreshold = 1,
            MaxSlowdownDelay = TimeSpan.Zero,
            MaxQueueWait = TimeSpan.FromMilliseconds(200),
        };
        using var gate = new AdmissionGate(backpressureOptions);
        var first = (await gate.AcquireAsync("rest", "get", "rest:client-a", DefaultCancellationToken)).Lease;
        var secondAcquire = gate.AcquireAsync("rest", "get", "rest:client-b", DefaultCancellationToken).AsTask();
        await WaitForGaugeSnapshotAsync(listener, inFlight, queueDepth, trackedClients, DefaultCancellationToken);
        first.Dispose();

        var (_, secondLease) = await secondAcquire;
        secondLease.Dispose();
    }

    /// <summary>Verifies observable gauges are not overwritten by an idle gate and remain correct after that gate is disposed.</summary>
    [Fact]
    public async Task GaugesStayBoundAfterIdleGateDispose()
    {
        var inFlight = new List<int>();
        var queueDepth = new List<int>();
        var trackedClients = new List<int>();
        var measurements = new Dictionary<string, List<int>>(StringComparer.Ordinal)
        {
            [BackpressureInFlightInstrumentName] = inFlight,
            [BackpressureQueueDepthInstrumentName] = queueDepth,
            [BackpressureTrackedClientsInstrumentName] = trackedClients,
        }.ToFrozenDictionary(StringComparer.Ordinal);

        using var listener = CreateBackpressureGaugeListener(measurements);
        var options = new AdmissionOptions
        {
            MaxInFlight = 1,
            MaxQueue = 1,
            SlowdownThreshold = 1,
            RejectThreshold = 1,
            MaxSlowdownDelay = TimeSpan.Zero,
            MaxQueueWait = TimeSpan.FromMilliseconds(200),
        };

        using var gateA = new AdmissionGate(options);

        var firstA = (await gateA.AcquireAsync("rest", "get", "rest:gateA:client-a", DefaultCancellationToken)).Lease;
        var queuedA = gateA.AcquireAsync("rest", "get", "rest:gateA:client-b", DefaultCancellationToken).AsTask();

        var gateB = new AdmissionGate(options);
        gateB.Dispose();

        await WaitForGaugeSnapshotAsync(listener, inFlight, queueDepth, trackedClients, DefaultCancellationToken);

        firstA.Dispose();

        var (_, secondA) = await queuedA;
        secondA.Dispose();
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
            });

        var lease = (await gate.AcquireAsync("grpc", "get", "grpc:client-a", DefaultCancellationToken)).Lease;
        lease.Dispose();
        _ = NodeExceptionAssert.For<SemaphoreFullException>().Throws(lease, static value => value.Dispose());
    }

    /// <summary>Verifies node-level rate limiting rejects excess requests and emits a node-scoped metric.</summary>
    [Fact]
    public async Task NodeRateLimitRejectsAndEmitsScopeMetric()
    {
        using var sink = new NodeMeasurementSink(MeterName);
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 4,
                MaxQueue = 0,
                SlowdownThreshold = 4,
                RejectThreshold = 4,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(100),
                NodeRateLimitPerSecond = 1,
                NodeRateLimitBurst = 1,
            });

        using var first = (await gate.AcquireAsync("rest", "get", "rest:client-a", DefaultCancellationToken)).Lease;

        var (decision, rejectedLease) = await gate.AcquireAsync("rest", "get", "rest:client-b", DefaultCancellationToken);
        rejectedLease.Dispose();

        Assert.False(decision.IsAccepted);
        Assert.Equal("node_rate_limit", decision.RejectReason);
        Assert.True(sink.HasEvent("squirix_backpressure_rate_limit_reject_total", ("transport", "rest"), ("op", "get"), ("scope", "node")));
    }

    /// <summary>Verifies a single client cannot monopolize node slots beyond its configured concurrency budget.</summary>
    [Fact]
    public async Task PerClientCapRejectsWhenNodeExhausted()
    {
        using var sink = new NodeMeasurementSink(MeterName);
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 4,
                PerClientMaxInFlight = 1,
                PerClientMaxQueue = 0,
                MaxQueue = 4,
                SlowdownThreshold = 4,
                RejectThreshold = 4,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(100),
            });

        using var first = (await gate.AcquireAsync("grpc", "get", "grpc:client-a", DefaultCancellationToken)).Lease;

        var (decision, rejectedLease) = await gate.AcquireAsync("grpc", "get", "grpc:client-a", DefaultCancellationToken);
        rejectedLease.Dispose();

        Assert.False(decision.IsAccepted);
        Assert.Equal("client_queue_full", decision.RejectReason);
        Assert.True(sink.HasEvent("squirix_backpressure_reject_total", ("transport", "grpc"), ("op", "get"), ("reason", "client_queue_full")));
    }

    /// <summary>Verifies per-client rate limiting rejects one client without blocking unrelated clients.</summary>
    [Fact]
    public async Task PerClientRateLimitIsolatedByClient()
    {
        using var sink = new NodeMeasurementSink(MeterName);
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 4,
                MaxQueue = 0,
                SlowdownThreshold = 4,
                RejectThreshold = 4,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(100),
                PerClientRateLimitPerSecond = 1,
                PerClientRateLimitBurst = 1,
            });

        using var first = (await gate.AcquireAsync("grpc", "get", "grpc:client-a", DefaultCancellationToken)).Lease;

        var (rejectedDecision, rejectedLease) = await gate.AcquireAsync("grpc", "get", "grpc:client-a", DefaultCancellationToken);
        rejectedLease.Dispose();

        using var secondClient = (await gate.AcquireAsync("grpc", "get", "grpc:client-b", DefaultCancellationToken)).Lease;

        Assert.False(rejectedDecision.IsAccepted);
        Assert.Equal("client_rate_limit", rejectedDecision.RejectReason);
        Assert.True(sink.HasEvent("squirix_backpressure_rate_limit_reject_total", ("transport", "grpc"), ("op", "get"), ("scope", "client")));
    }

    /// <summary>Verifies requests are rejected once the hard threshold is reached while another request is queued.</summary>
    [Fact]
    public async Task QueueFullRejectsImmediately()
    {
        using var sink = new NodeMeasurementSink(MeterName);
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 1,
                MaxQueue = 1,
                SlowdownThreshold = 1,
                RejectThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(200),
            });

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
        using var sink = new NodeMeasurementSink(MeterName);
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 1,
                MaxQueue = 1,
                SlowdownThreshold = 1,
                RejectThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(40),
            });

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
            });

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
        using var sink = new NodeMeasurementSink(MeterName);
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 1,
                MaxQueue = 1,
                SlowdownThreshold = 1,
                RejectThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromSeconds(2),
            });

        using var heldLease = (await gate.AcquireAsync("rest", "remove", "rest:client-a", DefaultCancellationToken)).Lease;
        using var cts = new CancellationTokenSource();
        var queuedTask = gate.AcquireAsync("rest", "remove", "rest:client-b", cts.Token).AsTask();

        Assert.False(queuedTask.IsCompleted);
        await cts.CancelAsync();

        await WaitUntilCanceledAsync(queuedTask);
        Assert.True(queuedTask.IsCanceled);
        Assert.True(sink.HasEvent("squirix_backpressure_queue_cancellations_total", ("transport", "rest"), ("op", "remove")));
    }

    /// <summary>Verifies the slowdown counter is emitted when load crosses the soft threshold.</summary>
    [Fact]
    public async Task SlowdownCounterIncrementsPastThreshold()
    {
        using var sink = new NodeMeasurementSink(MeterName);
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 2,
                MaxQueue = 1,
                SlowdownThreshold = 1,
                RejectThreshold = 2,
                MaxSlowdownDelay = TimeSpan.FromMilliseconds(5),
                MaxQueueWait = TimeSpan.FromMilliseconds(100),
            });

        using var first = (await gate.AcquireAsync("rest", "put", "rest:client-a", DefaultCancellationToken)).Lease;
        using var second = (await gate.AcquireAsync("rest", "put", "rest:client-b", DefaultCancellationToken)).Lease;

        Assert.True(sink.HasEvent("squirix_backpressure_slowdown_total", ("transport", "rest"), ("op", "put")));
    }

    private static MeterListener CreateBackpressureGaugeListener(FrozenDictionary<string, List<int>> measurements)
    {
        var subscription = new BackpressureGaugeSubscription(measurements);
        var listener = new MeterListener
        {
            InstrumentPublished = subscription.OnInstrumentPublished,
        };
        listener.SetMeasurementEventCallback<int>(static (instrument, measurement, _, state) =>
        {
            if (state is FrozenDictionary<string, List<int>> map && map.TryGetValue(instrument.Name, out var target))
                target.Add(measurement);
        });
        listener.Start();
        return listener;
    }

    private static bool HasAtLeast(List<int> values, int min)
    {
        for (var i = 0; i < values.Count; i++)
        {
            if (values[i] >= min)
                return true;
        }

        return false;
    }

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

    private static async Task WaitForGaugeSnapshotAsync(
        MeterListener listener,
        List<int> inFlight,
        List<int> queueDepth,
        List<int> trackedClients,
        CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(1);
        while (DateTime.UtcNow < deadline)
        {
            listener.RecordObservableInstruments();
            if (HasAtLeast(inFlight, 1) && HasAtLeast(queueDepth, 1) && HasAtLeast(trackedClients, 2))
                return;

            await Task.Delay(TimeSpan.FromMilliseconds(10), TimeProvider.System, cancellationToken);
        }

        Assert.True(HasAtLeast(inFlight, 1));
        Assert.True(HasAtLeast(queueDepth, 1));
        Assert.True(HasAtLeast(trackedClients, 2));
    }

    private static async Task WaitUntilCanceledAsync(Task task)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (!task.IsCompleted && DateTime.UtcNow < deadline)
            await Task.Delay(TimeSpan.FromMilliseconds(10), TimeProvider.System, DefaultCancellationToken);

        Assert.True(task.IsCanceled);
    }

    [Immutable]
    private sealed class BackpressureGaugeSubscription
    {
        private readonly FrozenDictionary<string, List<int>> _measurements;

        internal BackpressureGaugeSubscription(FrozenDictionary<string, List<int>> measurements)
        {
            _measurements = measurements;
        }

        internal void OnInstrumentPublished(Instrument instrument, MeterListener listener)
        {
            if (!string.Equals(instrument.Meter.Name, MeterName, StringComparison.OrdinalIgnoreCase))
                return;

            if (IsBackpressureGauge(instrument.Name))
                listener.EnableMeasurementEvents(instrument, _measurements);
        }

        private static bool IsBackpressureGauge(string name) => string.Equals(name, BackpressureInFlightInstrumentName, StringComparison.Ordinal) ||
                                                                string.Equals(name, BackpressureQueueDepthInstrumentName, StringComparison.Ordinal) || string.Equals(
                                                                    name,
                                                                    BackpressureTrackedClientsInstrumentName,
                                                                    StringComparison.Ordinal);
    }
}
