using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;

namespace Squirix.Server.Node.Observability;

/// <summary>Low-cardinality idempotency store metrics on the shared <see cref="MeterRegistry.Meter" />.</summary>
internal static class IdempotencyMetrics
{
    private static readonly Lock InitLock = new();

    private static readonly Counter<long> EvictionsTotal = MeterRegistry.Meter.CreateCounter<long>(
        "squirix_idempotency_evictions_total",
        "{eviction}",
        "Idempotency store evictions when enforcing the in-flight record cap");

    private static readonly Counter<long> RejectionsTotal = MeterRegistry.Meter.CreateCounter<long>(
        "squirix_idempotency_rejections_total",
        "{rejection}",
        "Idempotency store rejections when the in-flight record cap cannot be satisfied");

    private static IdempotencyMetricRegistration[] _registrations = [];

    private static int _instrumentsCreated;

    public static void RecordEviction(string nodeId) => EvictionsTotal.Add(1, Tags(nodeId));

    public static void RecordRejection(string nodeId) => RejectionsTotal.Add(1, Tags(nodeId));

    internal static void Register(IdempotencyMetricRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(registration);
        lock (InitLock)
        {
            var previous = _registrations;
            var next = new IdempotencyMetricRegistration[previous.Length + 1];
            previous.CopyTo(next, 0);
            next[previous.Length] = registration;
            _registrations = next;
            EnsureInstrumentsLocked();
        }
    }

    internal static void Unregister(IdempotencyMetricRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(registration);
        lock (InitLock)
        {
            var previous = _registrations;
            var index = Array.IndexOf(previous, registration);
            if (index < 0)
                return;

            if (previous.Length is 1)
            {
                _registrations = [];
                return;
            }

            var next = new IdempotencyMetricRegistration[previous.Length - 1];
            previous.AsSpan(0, index).CopyTo(next);
            previous.AsSpan(index + 1).CopyTo(next.AsSpan(index));
            _registrations = next;
        }
    }

    private static void EnsureInstrumentsLocked()
    {
        if (Interlocked.CompareExchange(ref _instrumentsCreated, 1, 0) is not 0)
            return;

        _ = MeterRegistry.Meter.CreateObservableGauge(
            "squirix_idempotency_records",
            ObserveRecordCount,
            "{record}",
            "Current in-memory idempotency record count");
    }

    private static IEnumerable<Measurement<long>> ObserveRecordCount()
    {
        foreach (var registration in Volatile.Read(ref _registrations))
            yield return new Measurement<long>(registration.Store.RecordCount, Tags(registration.NodeId));
    }

    private static KeyValuePair<string, object?>[] Tags(string nodeId) => [new("node", nodeId)];
}
