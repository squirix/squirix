using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.UnitTests.Support;
using Xunit;

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
    [Fact]
    public async Task GaugesReflectInFlightAndQueueDepth()
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
        };
        using var gate = new AdmissionGate(backpressureOptions, new BackpressureMetrics(meter));
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
        var options = new AdmissionOptions
        {
            MaxInFlight = 1,
            MaxQueue = 1,
            SlowdownThreshold = 1,
            RejectThreshold = 1,
            MaxSlowdownDelay = TimeSpan.Zero,
            MaxQueueWait = TimeSpan.FromMilliseconds(200),
        };

        using var gateA = new AdmissionGate(options, new BackpressureMetrics(meter));

        var firstA = (await gateA.AcquireAsync("rest", "get", "rest:gateA:client-a", DefaultCancellationToken)).Lease;
        var queuedA = gateA.AcquireAsync("rest", "get", "rest:gateA:client-b", DefaultCancellationToken).AsTask();

        var gateB = new AdmissionGate(options, new BackpressureMetrics(meter));
        gateB.Dispose();

        await WaitForGaugeSnapshotAsync(listener, inFlight, queueDepth, trackedClients, DefaultCancellationToken);

        firstA.Dispose();

        var (_, secondA) = await queuedA;
        secondA.Dispose();
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
