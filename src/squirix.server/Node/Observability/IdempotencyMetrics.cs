using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability;

/// <summary>Low-cardinality idempotency store metrics on the host-scoped <see cref="Meter" />.</summary>
[ThreadSafe]
internal sealed class IdempotencyMetrics
{
    private readonly RegistrationCatalog _catalog = new();
    private readonly Counter<long> _evictionsTotal;
    private readonly Lock _initLock = new();
    private readonly Meter _meter;
    private readonly Counter<long> _rejectionsTotal;

    internal IdempotencyMetrics(Meter meter)
    {
        _meter = meter;
        _evictionsTotal = meter.CreateCounter<long>("squirix_idempotency_evictions_total", "{eviction}", "Idempotency store evictions when enforcing the in-flight record cap");
        _rejectionsTotal = meter.CreateCounter<long>(
            "squirix_idempotency_rejections_total",
            "{rejection}",
            "Idempotency store rejections when the in-flight record cap cannot be satisfied");
    }

    internal void RecordEviction(string nodeId)
    {
        var tags = NodeTags(nodeId);
        _evictionsTotal.Add(1, in tags);
    }

    internal void RecordRejection(string nodeId)
    {
        var tags = NodeTags(nodeId);
        _rejectionsTotal.Add(1, in tags);
    }

    internal void Register(IdempotencyMetricRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(registration);
        lock (_initLock)
        {
            _catalog.Add(registration);
            EnsureInstrumentsLocked();
        }
    }

    internal void Unregister(IdempotencyMetricRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(registration);
        lock (_initLock)
            _catalog.Remove(registration);
    }

    private static TagList NodeTags(string nodeId) => new()
    {
        { "node", nodeId },
    };

    private void EnsureInstrumentsLocked()
    {
        if (!_catalog.TryCreateInstruments())
            return;

        _ = _meter.CreateObservableGauge("squirix_idempotency_records", ObserveRecordCount, "{record}", "Current in-memory idempotency record count");
    }

    private IEnumerable<Measurement<long>> ObserveRecordCount()
    {
        var snapshot = _catalog.SnapshotItems();
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

            _items = previous.Length == 1 ? [] : previous.RemoveAt(index);
        }

        internal ImmutableArray<IdempotencyMetricRegistration> SnapshotItems() => _items;

        internal bool TryCreateInstruments() => Interlocked.CompareExchange(ref _instrumentsCreated, 1, 0) == 0;
    }
}
