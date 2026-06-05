using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Backpressure;

/// <summary>
/// Unit tests for node-level backpressure admission control.
/// </summary>
public sealed class BackpressureGateTests : ServerUnitTestBase
{
    private const string BackpressureInFlightInstrumentName = "squirix_backpressure_in_flight";
    private const string BackpressureQueueDepthInstrumentName = "squirix_backpressure_queue_depth";
    private const string BackpressureTrackedClientsInstrumentName = "squirix_backpressure_tracked_clients";
    private const string MeterName = "Squirix";

    /// <summary>
    /// Verifies disabled backpressure returns an accepted empty lease and emits bypass metrics.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task AcquireBypassesWhenBackpressureIsDisabled()
    {
        using var sink = new MeasurementSink(MeterName);
        using var gate = new BackpressureGate(
            new BackpressureOptions
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

    /// <summary>
    /// Verifies admission succeeds immediately while slots are available.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task AcquireSucceedsImmediatelyWhenCapacityAvailable()
    {
        using var gate = new BackpressureGate(
            new BackpressureOptions
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

    /// <summary>
    /// Verifies concurrent acquire and release does not exceed configured in-flight capacity.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task ConcurrentAcquireReleaseDoesNotExceedConfiguredCapacity()
    {
        const int maxInFlight = 3;
        var backpressureOptions = new BackpressureOptions
        {
            MaxInFlight = maxInFlight,
            MaxQueue = 64,
            SlowdownThreshold = maxInFlight,
            RejectThreshold = maxInFlight,
            MaxSlowdownDelay = TimeSpan.Zero,
            MaxQueueWait = TimeSpan.FromSeconds(2),
        };
        using var gate = new BackpressureGate(backpressureOptions);
        var current = new int[1];
        var observedMax = new int[1];
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tasks = new List<Task>(24);

        for (var i = 0; i < 24; i++)
        {
            tasks.Add(RunClientAsync(gate, start.Task, current, observedMax, $"grpc:client-{i}"));
        }

        _ = start.TrySetResult();
        await Task.WhenAll(tasks);

        Assert.True(observedMax[0] <= maxInFlight, $"Observed max in-flight {observedMax[0]} exceeded limit {maxInFlight}.");
        return;

        static async Task RunClientAsync(BackpressureGate gate, Task start, int[] current, int[] observedMax, string clientId)
        {
            await start.ConfigureAwait(false);

            var (decision, lease) = await gate.AcquireAsync("grpc", "insert", clientId, DefaultCancellationToken).ConfigureAwait(false);

            if (!decision.IsAccepted)
            {
                return;
            }

            using (lease)
            {
                var now = Interlocked.Increment(ref current[0]);
                UpdateMax(ref observedMax[0], now);

                try
                {
                    await Task.Yield();
                }
                finally
                {
                    _ = Interlocked.Decrement(ref current[0]);
                }
            }
        }
    }

    /// <summary>
    /// Verifies observable gauges report both in-flight work and queued requests.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
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
        };

        using var listener = new MeterListener();
        listener.InstrumentPublished = static (instrument, meterListener) =>
        {
            if (instrument.Meter.Name != MeterName)
                return;

            if (IsBackpressureGauge(instrument.Name))
                meterListener.EnableMeasurementEvents(instrument);
        };

        listener.SetMeasurementEventCallback<int>((instrument, measurement, _, _) =>
        {
            if (measurements.TryGetValue(instrument.Name, out var target))
                target.Add(measurement);
        });

        listener.Start();

        using var gate = new BackpressureGate(
            new BackpressureOptions
            {
                MaxInFlight = 1,
                MaxQueue = 1,
                SlowdownThreshold = 1,
                RejectThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(200),
            });

        var first = (await gate.AcquireAsync("rest", "get", "rest:client-a", DefaultCancellationToken)).Lease;
        var secondAcquire = gate.AcquireAsync("rest", "get", "rest:client-b", DefaultCancellationToken).AsTask();

        await WaitForGaugeSnapshotAsync(listener, inFlight, queueDepth, trackedClients, DefaultCancellationToken);

        first.Dispose();

        var (_, secondLease) = await secondAcquire;
        secondLease.Dispose();
    }

    /// <summary>
    /// Verifies observable gauges are not overwritten by an idle gate and remain correct after that gate is disposed.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task GaugesRemainBoundToActiveGateAfterIdleGateDispose()
    {
        var inFlight = new List<int>();
        var queueDepth = new List<int>();
        var trackedClients = new List<int>();
        var measurements = new Dictionary<string, List<int>>(StringComparer.Ordinal)
        {
            [BackpressureInFlightInstrumentName] = inFlight,
            [BackpressureQueueDepthInstrumentName] = queueDepth,
            [BackpressureTrackedClientsInstrumentName] = trackedClients,
        };

        using var listener = new MeterListener();
        listener.InstrumentPublished = static (instrument, meterListener) =>
        {
            if (instrument.Meter.Name != MeterName)
                return;

            if (IsBackpressureGauge(instrument.Name))
                meterListener.EnableMeasurementEvents(instrument);
        };

        listener.SetMeasurementEventCallback<int>((instrument, measurement, _, _) =>
        {
            if (measurements.TryGetValue(instrument.Name, out var target))
                target.Add(measurement);
        });

        listener.Start();

        var options = new BackpressureOptions
        {
            MaxInFlight = 1,
            MaxQueue = 1,
            SlowdownThreshold = 1,
            RejectThreshold = 1,
            MaxSlowdownDelay = TimeSpan.Zero,
            MaxQueueWait = TimeSpan.FromMilliseconds(200),
        };

        using var gateA = new BackpressureGate(options);

        var firstA = (await gateA.AcquireAsync("rest", "get", "rest:gateA:client-a", DefaultCancellationToken)).Lease;
        var queuedA = gateA.AcquireAsync("rest", "get", "rest:gateA:client-b", DefaultCancellationToken).AsTask();

        var gateB = new BackpressureGate(options);
        gateB.Dispose();

        await WaitForGaugeSnapshotAsync(listener, inFlight, queueDepth, trackedClients, DefaultCancellationToken);

        firstA.Dispose();

        var (_, secondA) = await queuedA;
        secondA.Dispose();
    }

    /// <summary>
    /// Verifies disposing the same lease twice follows current release behavior.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task LeaseDoubleDisposeKeepsCurrentBehavior()
    {
        using var gate = new BackpressureGate(
            new BackpressureOptions
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
        _ = Assert.Throws<SemaphoreFullException>(lease.Dispose);
    }

    /// <summary>
    /// Verifies node-level rate limiting rejects excess requests and emits a node-scoped metric.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task NodeRateLimitRejectsAndEmitsScopeMetric()
    {
        using var sink = new MeasurementSink(MeterName);
        using var gate = new BackpressureGate(
            new BackpressureOptions
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

    /// <summary>
    /// Verifies a single client cannot monopolize node slots beyond its configured concurrency budget.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task PerClientConcurrencyRejectsBeforeNodeCapacityIsExhausted()
    {
        using var sink = new MeasurementSink(MeterName);
        using var gate = new BackpressureGate(
            new BackpressureOptions
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

    /// <summary>
    /// Verifies per-client rate limiting rejects one client without blocking unrelated clients.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task PerClientRateLimitIsolatedByClient()
    {
        using var sink = new MeasurementSink(MeterName);
        using var gate = new BackpressureGate(
            new BackpressureOptions
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

    /// <summary>
    /// Verifies a queued acquire completes after a held lease is released.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task QueuedAcquireCompletesAfterLeaseRelease()
    {
        using var gate = new BackpressureGate(
            new BackpressureOptions
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
        {
            Assert.True(decision.IsAccepted);
        }
    }

    /// <summary>
    /// Verifies queued admission observes caller cancellation and records queue cancellation metrics.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task QueuedAcquireObservesCallerCancellation()
    {
        using var sink = new MeasurementSink(MeterName);
        using var gate = new BackpressureGate(
            new BackpressureOptions
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

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await queuedTask);
        Assert.True(sink.HasEvent("squirix_backpressure_queue_cancellations_total", ("transport", "rest"), ("op", "remove")));
    }

    /// <summary>
    /// Verifies requests are rejected once the hard threshold is reached while another request is queued.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task QueueFullRejectsImmediately()
    {
        using var sink = new MeasurementSink(MeterName);
        using var gate = new BackpressureGate(
            new BackpressureOptions
            {
                MaxInFlight = 1,
                MaxQueue = 1,
                SlowdownThreshold = 1,
                RejectThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(200),
            });

        var first = (await gate.AcquireAsync("grpc", "insert", "grpc:client-a", DefaultCancellationToken)).Lease;
        var secondAcquire = gate.AcquireAsync("grpc", "insert", "grpc:client-b", DefaultCancellationToken).AsTask();

        await Task.Delay(20, DefaultCancellationToken);

        var (decision, rejectedLease) = await gate.AcquireAsync("grpc", "insert", "grpc:client-c", DefaultCancellationToken);
        rejectedLease.Dispose();

        Assert.False(decision.IsAccepted);
        Assert.Equal("hard_threshold", decision.RejectReason);
        Assert.True(sink.HasEvent("squirix_backpressure_reject_total", ("transport", "grpc"), ("op", "insert"), ("reason", "hard_threshold")));

        first.Dispose();

        var (_, secondLease) = await secondAcquire;
        secondLease.Dispose();
    }

    /// <summary>
    /// Verifies a queued request is rejected after exceeding the configured queue wait budget.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task QueueTimeoutRejectsAndEmitsMetrics()
    {
        using var sink = new MeasurementSink(MeterName);
        using var gate = new BackpressureGate(
            new BackpressureOptions
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

    /// <summary>
    /// Verifies the slowdown counter is emitted when load crosses the soft threshold.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task SlowdownCounterIncrementsWhenThresholdIsExceeded()
    {
        using var sink = new MeasurementSink(MeterName);
        using var gate = new BackpressureGate(
            new BackpressureOptions
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

    private static bool HasAtLeast(IEnumerable<int> values, int min)
    {
        foreach (var value in values)
        {
            if (value >= min)
                return true;
        }

        return false;
    }

    private static bool IsBackpressureGauge(string instrumentName) =>
        instrumentName is BackpressureInFlightInstrumentName or BackpressureQueueDepthInstrumentName or BackpressureTrackedClientsInstrumentName;

    private static void UpdateMax(ref int target, int candidate)
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
        IReadOnlyCollection<int> inFlight,
        IReadOnlyCollection<int> queueDepth,
        IReadOnlyCollection<int> trackedClients,
        CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(1);
        while (DateTime.UtcNow < deadline)
        {
            listener.RecordObservableInstruments();
            if (HasAtLeast(inFlight, 1) && HasAtLeast(queueDepth, 1) && HasAtLeast(trackedClients, 2))
                return;

            await Task.Delay(TimeSpan.FromMilliseconds(10), cancellationToken).ConfigureAwait(false);
        }

        Assert.Contains(inFlight, static x => x >= 1);
        Assert.Contains(queueDepth, static x => x >= 1);
        Assert.Contains(trackedClients, static x => x >= 2);
    }
}
