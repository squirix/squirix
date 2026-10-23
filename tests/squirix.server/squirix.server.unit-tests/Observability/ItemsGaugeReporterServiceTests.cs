using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>Tests for <see cref="ItemsGaugeReporterService" /> observable gauge wiring.</summary>
[Immutable]
public sealed class ItemsGaugeReporterServiceTests
{
    /// <summary>Verifies observable gauge measurements, empty-cache reporting, error propagation, and hosted lifecycle hooks.</summary>
    [Test]
    public async Task ObservableGaugeReflectsStatsAsync()
    {
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink();
        using var listener = CreateListener(sink);

        using (var service = new ItemsGaugeReporterService(CreateFixedStats(9), meter))
        {
            await service.StartAsync(CancellationToken.None);
            listener.RecordObservableInstruments();
            _ = await Assert.That(sink.Values).Contains(9L);
            await service.StopAsync(CancellationToken.None);
        }

        using (var empty = new ItemsGaugeReporterService(CreateFixedStats(0), meter))
        {
            await empty.StartAsync(CancellationToken.None);
            listener.RecordObservableInstruments();
            _ = await Assert.That(sink.Values).Contains(0L);
            await empty.StopAsync(CancellationToken.None);
        }

        using var faulting = new ItemsGaugeReporterService(CreateFaultingStats(), meter);
        await faulting.StartAsync(CancellationToken.None);
        var aggregate = NodeExceptionAssert.For<AggregateException>().Throws(listener, static value => value.RecordObservableInstruments());
        var inner = await Assert.That(aggregate.InnerExceptions).HasSingleItem();
        var statsDown = (await Assert.That(inner).IsTypeOf<InvalidOperationException>())!;
        _ = await Assert.That(statsDown.Message).IsEqualTo("stats-down");
        await faulting.StopAsync(CancellationToken.None);
    }

    private static ILocalCacheStats CreateFaultingStats()
    {
        var expectations = new ILocalCacheStatsCreateExpectations();
        _ = expectations.Setups.EntryCount.Gets().Throws(new InvalidOperationException("stats-down"));
        return expectations.Instance();
    }

    private static ILocalCacheStats CreateFixedStats(int entryCount)
    {
        var expectations = new ILocalCacheStatsCreateExpectations();
        _ = expectations.Setups.EntryCount.Gets().ReturnValue(entryCount);
        return expectations.Instance();
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
}
