using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading;

namespace Squirix.Server.Node.Observability;

/// <summary>Low-cardinality idempotency store metrics on the shared <see cref="ServerMeterRegistry.Meter" />.</summary>
internal static class IdempotencyMetrics
{
    private static readonly RegistrationCatalog Catalog = new();

    private static readonly Counter<long> EvictionsTotal = ServerMeterRegistry.Meter.CreateCounter<long>(
        "squirix_idempotency_evictions_total",
        "{eviction}",
        "Idempotency store evictions when enforcing the in-flight record cap");

    private static readonly Lock InitLock = new();

    private static readonly Counter<long> RejectionsTotal = ServerMeterRegistry.Meter.CreateCounter<long>(
        "squirix_idempotency_rejections_total",
        "{rejection}",
        "Idempotency store rejections when the in-flight record cap cannot be satisfied");

    internal static void RecordEviction(string nodeId)
    {
        var tags = NodeTags(nodeId);
        EvictionsTotal.Add(1, in tags);
    }

    internal static void RecordRejection(string nodeId)
    {
        var tags = NodeTags(nodeId);
        RejectionsTotal.Add(1, in tags);
    }

    internal static void Register(IdempotencyMetricRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(registration);
        lock (InitLock)
        {
            Catalog.Add(registration);
            EnsureInstrumentsLocked();
        }
    }

    internal static void Unregister(IdempotencyMetricRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(registration);
        lock (InitLock)
            Catalog.Remove(registration);
    }

    private static void EnsureInstrumentsLocked()
    {
        if (!Catalog.TryCreateInstruments())
            return;

        _ = ServerMeterRegistry.Meter.CreateObservableGauge("squirix_idempotency_records", ObserveRecordCount, "{record}", "Current in-memory idempotency record count");
    }

    private static TagList NodeTags(string nodeId) =>
        new()
        {
            { "node", nodeId },
        };

    private static IEnumerable<Measurement<long>> ObserveRecordCount()
    {
        var snapshot = Catalog.SnapshotItems();
        for (var i = 0; i < snapshot.Length; i++)
        {
            var registration = snapshot[i];
            var tags = NodeTags(registration.NodeId);
            yield return new Measurement<long>(registration.RecordCount(), in tags);
        }
    }

    private sealed class RegistrationCatalog
    {
        private int _instrumentsCreated;

        private ImmutableArray<IdempotencyMetricRegistration> _items = [];

        internal void Add(IdempotencyMetricRegistration registration) => _items = _items.Add(registration);

        internal void Remove(IdempotencyMetricRegistration registration)
        {
            var previous = _items;
            var index = previous.IndexOf(registration);
            if (index < 0)
                return;

            _items = previous.Length is 1 ? [] : previous.RemoveAt(index);
        }

        internal ImmutableArray<IdempotencyMetricRegistration> SnapshotItems() => _items;

        internal bool TryCreateInstruments() => Interlocked.CompareExchange(ref _instrumentsCreated, 1, 0) is 0;
    }
}
