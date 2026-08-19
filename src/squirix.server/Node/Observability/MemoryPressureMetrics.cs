using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Node.MemoryPressure;

namespace Squirix.Server.Node.Observability;

/// <summary>
/// Low-cardinality memory pressure metrics on the shared <see cref="ServerMeterRegistry.Meter" />.
/// Observable gauges aggregate active node registrations so multiple hosts in one process do not duplicate instruments.
/// </summary>
[ThreadSafe]
internal static class MemoryPressureMetrics
{
    private static readonly Lock InitLock = new();

    private static RegistrationCatalog Catalog { get; } = new();

    internal static void Register(MetricRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(registration);
        lock (InitLock)
        {
            Catalog.Add(registration);
            EnsureInstrumentsLocked();
        }
    }

    internal static void Unregister(MetricRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(registration);
        lock (InitLock)
            Catalog.Remove(registration);
    }

    private static (int Value, string Name) DescribePressureState(PressureLevel state)
    {
        return state switch
        {
            PressureLevel.Normal => (0, "normal"),
            PressureLevel.High => (1, "high"),
            PressureLevel.Critical => (2, "critical"),
            _ => throw new ArgumentOutOfRangeException(nameof(state), "Unsupported memory pressure state."),
        };
    }

    private static void EnsureInstrumentsLocked()
    {
        if (!Catalog.TryCreateInstruments())
            return;

        _ = ServerMeterRegistry.Meter.CreateObservableGauge(
            "squirix_cache_estimated_bytes",
            ObserveEstimatedBytes,
            "By",
            "Approximate total estimated bytes for accounted live cache entries");

        _ = ServerMeterRegistry.Meter.CreateObservableGauge(
            "squirix_cache_entries",
            ObserveEntryCount,
            "{entry}",
            "Approximate total entry count for accounted live cache entries");

        _ = ServerMeterRegistry.Meter.CreateObservableGauge(
            "squirix_memory_pressure_state",
            ObservePressureState,
            description: "Memory pressure state as 0=normal, 1=high, 2=critical (tags: node, state)");
    }

    private static Measurement<long> MeasureNode(long value, string nodeId)
    {
        var tags = new TagList
        {
            { "node", nodeId },
        };
        return new Measurement<long>(value, in tags);
    }

    private static IEnumerable<Measurement<long>> ObserveEntryCount()
    {
        var snapshot = Catalog.SnapshotItems();
        for (var i = 0; i < snapshot.Length; i++)
        {
            var registration = snapshot[i];
            yield return MeasureNode(registration.Accounting.ReadEntryCount(), registration.NodeId);
        }
    }

    private static IEnumerable<Measurement<long>> ObserveEstimatedBytes()
    {
        var snapshot = Catalog.SnapshotItems();
        for (var i = 0; i < snapshot.Length; i++)
        {
            var registration = snapshot[i];
            yield return MeasureNode(registration.Accounting.ReadEstimatedBytes(), registration.NodeId);
        }
    }

    private static IEnumerable<Measurement<int>> ObservePressureState()
    {
        var snapshot = Catalog.SnapshotItems();
        for (var i = 0; i < snapshot.Length; i++)
        {
            var registration = snapshot[i];
            var (value, name) = DescribePressureState(registration.Evaluator.Evaluate(registration.Accounting.ReadEstimatedBytes()));

            var tags = new TagList
            {
                { "node", registration.NodeId },
                { "state", name },
            };
            yield return new Measurement<int>(value, in tags);
        }
    }

    private sealed class RegistrationCatalog
    {
        private int _instrumentsCreated;

        private ImmutableArray<MetricRegistration> _items = [];

        internal void Add(MetricRegistration registration) => _items = _items.Add(registration);

        internal void Remove(MetricRegistration registration)
        {
            var previous = _items;
            var index = previous.IndexOf(registration);
            if (index < 0)
                return;

            _items = previous.Length == 1 ? [] : previous.RemoveAt(index);
        }

        internal ImmutableArray<MetricRegistration> SnapshotItems() => _items;

        internal bool TryCreateInstruments() => Interlocked.CompareExchange(ref _instrumentsCreated, 1, 0) == 0;
    }
}
