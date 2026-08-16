using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>
/// Tests for <see cref="ItemsGaugeReporterService" /> observable gauge wiring.
/// </summary>
[Immutable]
public sealed class ItemsGaugeReporterServiceTests
{
    /// <summary>Verifies observable gauge measurements, empty-cache reporting, error propagation, and hosted lifecycle hooks.</summary>
    [Fact]
    public async Task ObservableGaugeReflectsStatsAsync()
    {
        using var sink = new NodeMeasurementSink();
        using var listener = CreateListener(sink);

        using (var service = new ItemsGaugeReporterService(new StubStats(9)))
        {
            await service.StartAsync(CancellationToken.None);
            listener.RecordObservableInstruments();
            Assert.Contains(9L, sink.Values);
            await service.StopAsync(CancellationToken.None);
        }

        using (var empty = new ItemsGaugeReporterService(new StubStats(0)))
        {
            await empty.StartAsync(CancellationToken.None);
            listener.RecordObservableInstruments();
            Assert.Contains(0L, sink.Values);
            await empty.StopAsync(CancellationToken.None);
        }

        using var faulting = new ItemsGaugeReporterService(new FaultingStats());
        await faulting.StartAsync(CancellationToken.None);
        var aggregate = NodeExceptionAssert.For<AggregateException>().Throws(listener, static value => value.RecordObservableInstruments());
        var inner = Assert.Single(aggregate.InnerExceptions);
        var statsDown = Assert.IsType<InvalidOperationException>(inner);
        Assert.Equal("stats-down", statsDown.Message);
        await faulting.StopAsync(CancellationToken.None);
    }

    private static MeterListener CreateListener(NodeMeasurementSink sink)
    {
        var subscription = new ItemsGaugeSubscription(sink.Values);
        var listener = new MeterListener
        {
            InstrumentPublished = subscription.OnInstrumentPublished,
        };

        listener.SetMeasurementEventCallback<long>(static (instrument, measurement, _, state) =>
        {
            if (ItemsGaugeSubscription.IsItemsTotal(instrument) && state is List<long> target)
                target.Add(measurement);
        });

        listener.Start();
        return listener;
    }

    [Immutable]
    private sealed class FaultingStats : ILocalCacheStats
    {
        public int EntryCount => throw new InvalidOperationException("stats-down");
    }

    [Immutable]
    private sealed class ItemsGaugeSubscription
    {
        private readonly List<long> _values;

        internal ItemsGaugeSubscription(List<long> values)
        {
            _values = values;
        }

        internal static bool IsItemsTotal(Instrument instrument) => string.Equals(instrument.Meter.Name, "Squirix", StringComparison.OrdinalIgnoreCase) &&
                                                                    string.Equals(instrument.Name, "squirix_items_total", StringComparison.OrdinalIgnoreCase);

        internal void OnInstrumentPublished(Instrument instrument, MeterListener listener)
        {
            if (IsItemsTotal(instrument))
                listener.EnableMeasurementEvents(instrument, _values);
        }
    }

    [Immutable]
    private sealed class NodeMeasurementSink : IDisposable
    {
        internal List<long> Values { get; } = [];

        public void Dispose() => Values.Clear();
    }

    [Immutable]
    private sealed class StubStats : ILocalCacheStats
    {
        internal StubStats(int entryCount)
        {
            EntryCount = entryCount;
        }

        public int EntryCount { get; }
    }
}
