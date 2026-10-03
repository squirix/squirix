using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
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

/// <summary>Unit tests for node-level backpressure observable gauge metrics.</summary>
[Immutable]
public sealed class BackpressureGaugeTests : ServerUnitTestBase
{
    private const string BackpressureInFlightInstrumentName = "squirix_backpressure_in_flight";
    private const string BackpressureQueueDepthInstrumentName = "squirix_backpressure_queue_depth";
    private const string BackpressureTrackedClientsInstrumentName = "squirix_backpressure_tracked_clients";
    private const string MeterName = "Squirix";

    /// <summary>Verifies observable gauges report both in-flight work and queued requests.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GaugesReflectInFlightAndQueueDepth(CancellationToken cancellationToken)
    {
        using var meter = new Meter(MeterName);
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
            PerClientMaxInFlight = 1,
        };
        using var gate = new AdmissionGate(backpressureOptions, new BackpressureMetrics(meter));
        var first = (await gate.AcquireAsync("rest", "get", "rest:client-a", cancellationToken)).Lease;
        var secondAcquire = gate.AcquireAsync("rest", "get", "rest:client-b", cancellationToken).AsTask();
        await WaitForGaugeSnapshotAsync(listener, inFlight, queueDepth, trackedClients, cancellationToken);
        first.Dispose();

        var (_, secondLease) = await secondAcquire;
        secondLease.Dispose();
    }

    /// <summary>Verifies a gate without per-client limits keeps no client entries while callers with different ids hold leases.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GateWithoutClientLimitsTracksNoClients(CancellationToken cancellationToken)
    {
        using var meter = new Meter(MeterName);
        var inFlight = new List<int>();
        var trackedClients = new List<int>();
        var measurements = new Dictionary<string, List<int>>(StringComparer.Ordinal)
        {
            [BackpressureInFlightInstrumentName] = inFlight,
            [BackpressureTrackedClientsInstrumentName] = trackedClients,
        }.ToFrozenDictionary(StringComparer.Ordinal);

        using var listener = CreateListenerFor(meter, measurements);
        using var gate = new AdmissionGate(new AdmissionOptions(), new BackpressureMetrics(meter));
        var (_, first) = await gate.AcquireAsync("rest", "get", "rest:client-a", cancellationToken);
        var (_, second) = await gate.AcquireAsync("rest", "get", "rest:client-b", cancellationToken);
        listener.RecordObservableInstruments();
        first.Dispose();
        second.Dispose();

        _ = await Assert.That(inFlight).Contains(2);
        _ = await Assert.That(trackedClients).IsNotEmpty();
        _ = await Assert.That(trackedClients.TrueForAll(static count => count == 0)).IsTrue();
    }

    /// <summary>Verifies the gauges read one gate at a time: a second gate is refused until the first one is disposed, then the gauges read it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MetricsObserveOneGateAtATime(CancellationToken cancellationToken)
    {
        using var meter = new Meter(MeterName);
        var inFlight = new List<int>();
        var measurements = new Dictionary<string, List<int>>(StringComparer.Ordinal)
        {
            [BackpressureInFlightInstrumentName] = inFlight,
        }.ToFrozenDictionary(StringComparer.Ordinal);
        using var listener = CreateListenerFor(meter, measurements);
        var metrics = new BackpressureMetrics(meter);

        InvalidOperationException refused;
        using (new AdmissionGate(new AdmissionOptions(), metrics))
            refused = NodeExceptionAssert.For<InvalidOperationException>().Throws(metrics, static m => _ = new AdmissionGate(new AdmissionOptions(), m));

        using var next = new AdmissionGate(new AdmissionOptions(), metrics);
        var (_, lease) = await next.AcquireAsync("rest", "get", "rest:client-a", cancellationToken);
        listener.RecordObservableInstruments();
        lease.Dispose();

        _ = await Assert.That(refused.Message).Contains("already observe", StringComparison.Ordinal);
        _ = await Assert.That(inFlight).Contains(1);
    }

    /// <summary>Listens to the gauges of <paramref name="meter" /> only; other tests publish the same gauges under the same meter name in parallel.</summary>
    /// <param name="meter">The meter whose gauges are read.</param>
    /// <param name="measurements">Measurement lists keyed by instrument name.</param>
    /// <returns>The started listener.</returns>
    private static MeterListener CreateListenerFor(Meter meter, FrozenDictionary<string, List<int>> measurements)
    {
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (ReferenceEquals(instrument.Meter, meter) && measurements.ContainsKey(instrument.Name))
                    l.EnableMeasurementEvents(instrument, measurements);
            },
        };
        listener.SetMeasurementEventCallback<int>(static (instrument, measurement, _, state) =>
        {
            if (state is FrozenDictionary<string, List<int>> map)
                map[instrument.Name].Add(measurement);
        });
        listener.Start();
        return listener;
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

    private static async Task WaitForGaugeSnapshotAsync(
        MeterListener listener,
        List<int> inFlight,
        List<int> queueDepth,
        List<int> trackedClients,
        CancellationToken cancellationToken)
    {
        var state = (Listener: listener, InFlight: inFlight, QueueDepth: queueDepth, TrackedClients: trackedClients);
        await state.WaitUntilAsync(
            static s =>
            {
                s.Listener.RecordObservableInstruments();
                return HasAtLeast(s.InFlight, 1) && HasAtLeast(s.QueueDepth, 1) && HasAtLeast(s.TrackedClients, 2);
            },
            TimeSpan.FromSeconds(1),
            cancellationToken);

        _ = await Assert.That(HasAtLeast(inFlight, 1)).IsTrue();
        _ = await Assert.That(HasAtLeast(queueDepth, 1)).IsTrue();
        _ = await Assert.That(HasAtLeast(trackedClients, 2)).IsTrue();
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
