using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using FakeItEasy;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// Tests for <see cref="ItemsGaugeReporterService" /> observable gauge wiring.
/// </summary>
public sealed class ItemsGaugeReporterServiceTests
{
    /// <summary>
    /// Verifies observable gauge measurements, empty-cache reporting, error propagation, and hosted lifecycle hooks.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task ObservableGaugeReflectsStatsAndPropagatesErrors()
    {
        using var sink = new MeasurementSink();
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

        var faultStats = A.Fake<ILocalCacheStats>();
        _ = A.CallTo(() => faultStats.EntryCount).Throws(new InvalidOperationException("stats-down"));
        using var faulting = new ItemsGaugeReporterService(faultStats);
        await faulting.StartAsync(CancellationToken.None);
        var aggregate = Assert.Throws<AggregateException>(listener.RecordObservableInstruments);
        var inner = Assert.Single(aggregate.InnerExceptions);
        var statsDown = Assert.IsType<InvalidOperationException>(inner);
        Assert.Equal("stats-down", statsDown.Message);
        await faulting.StopAsync(CancellationToken.None);
    }

    private static MeterListener CreateListener(MeasurementSink sink)
    {
        var listener = new MeterListener
        {
            InstrumentPublished = static (instrument, meterListener) =>
            {
                if (IsItemsTotal(instrument))
                    meterListener.EnableMeasurementEvents(instrument);
            },
        };

        listener.SetMeasurementEventCallback<long>((instrument, measurement, _, _) =>
        {
            if (IsItemsTotal(instrument))
                sink.Values.Add(measurement);
        });

        listener.Start();
        return listener;

        static bool IsItemsTotal(Instrument instrument)
        {
            return string.Equals(instrument.Meter.Name, "Squirix", StringComparison.OrdinalIgnoreCase)
                   && string.Equals(instrument.Name, "squirix_items_total", StringComparison.OrdinalIgnoreCase);
        }
    }

    private sealed class MeasurementSink : IDisposable
    {
        public List<long> Values { get; } = [];

        public void Dispose() => Values.Clear();
    }

    private sealed class StubStats : ILocalCacheStats
    {
        public StubStats(int entryCount)
        {
            EntryCount = entryCount;
        }

        public int EntryCount { get; }
    }
}
