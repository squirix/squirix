using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading;
using Squirix.Server.Node.MemoryPressure;

namespace Squirix.Server.Node.Observability;

/// <summary>
/// Low-cardinality memory pressure metrics on the shared <see cref="MeterRegistry.Meter" />.
/// Observable gauges aggregate active node registrations so multiple hosts in one process do not duplicate instruments.
/// </summary>
internal static class MemoryPressureMetrics
{
    private static readonly Lock InitLock = new();

    private static readonly List<MemoryPressureMetricRegistration> Registrations = [];

    private static readonly Counter<long> RejectionsTotal = MeterRegistry.Meter.CreateCounter<long>(
        "squirix_memory_rejections_total",
        "{rejection}",
        "Memory admission rejections by operation and reason");

    private static int _instrumentsCreated;

    public static void RecordRejection(string nodeId, string operation, string reason)
    {
        var tags = new TagList
        {
            { "node", nodeId },
            { "operation", operation },
            { "reason", reason },
        };
        RejectionsTotal.Add(1, tags);
    }

    internal static void Register(MemoryPressureMetricRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(registration);
        lock (InitLock)
        {
            Registrations.Add(registration);
            EnsureInstrumentsLocked();
        }
    }

    internal static void Unregister(MemoryPressureMetricRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(registration);
        lock (InitLock)
            _ = Registrations.Remove(registration);
    }

    private static (int Value, string Name) DescribePressureState(MemoryPressureState state)
    {
        return state switch
        {
            MemoryPressureState.Normal => (0, "normal"),
            MemoryPressureState.High => (1, "high"),
            MemoryPressureState.Critical => (2, "critical"),
            _ => throw new ArgumentOutOfRangeException(nameof(state), state, "Unsupported memory pressure state."),
        };
    }

    private static void EnsureInstrumentsLocked()
    {
        if (Interlocked.CompareExchange(ref _instrumentsCreated, 1, 0) != 0)
            return;

        _ = MeterRegistry.Meter.CreateObservableGauge(
            "squirix_cache_estimated_bytes",
            ObserveEstimatedBytes,
            "By",
            "Approximate total estimated bytes for accounted live cache entries");

        _ = MeterRegistry.Meter.CreateObservableGauge("squirix_cache_entries", ObserveEntryCount, "{entry}", "Approximate total entry count for accounted live cache entries");

        _ = MeterRegistry.Meter.CreateObservableGauge(
            "squirix_memory_pressure_state",
            ObservePressureState,
            description: "Memory pressure state as 0=normal, 1=high, 2=critical (tags: node, state)");
    }

    private static IEnumerable<Measurement<long>> ObserveEntryCount()
    {
        foreach (var r in SnapshotRegistrations())
            yield return new Measurement<long>(r.Accounting.EntryCount, TagsNode(r.NodeId));
    }

    private static IEnumerable<Measurement<long>> ObserveEstimatedBytes()
    {
        foreach (var r in SnapshotRegistrations())
            yield return new Measurement<long>(r.Accounting.EstimatedBytes, TagsNode(r.NodeId));
    }

    private static IEnumerable<Measurement<int>> ObservePressureState()
    {
        foreach (var r in SnapshotRegistrations())
        {
            var (value, name) = DescribePressureState(r.Evaluator.Evaluate(r.Accounting.EstimatedBytes));

            var tags = new KeyValuePair<string, object?>[]
            {
                new("node", r.NodeId),
                new("state", name),
            };
            yield return new Measurement<int>(value, tags);
        }
    }

    private static MemoryPressureMetricRegistration[] SnapshotRegistrations()
    {
        lock (InitLock)
            return [.. Registrations];
    }

    private static KeyValuePair<string, object?>[] TagsNode(string nodeId) => [new("node", nodeId)];
}
