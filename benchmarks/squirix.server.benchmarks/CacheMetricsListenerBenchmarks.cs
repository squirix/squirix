using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Observability;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>Cache operations with no metric listener or with independently enabled instruments.</summary>
[MemoryDiagnoser]
public class CacheMetricsListenerBenchmarks
{
    private static readonly StoredValue ValueText = StoredValue.FromText("value");

    private readonly NodeCacheEntry<StoredValue> _entry = new(ValueText);
    private MetricsCacheDecorator<StoredValue>? _cache;
    private MeterListener? _listener;
    private Meter? _meter;

    /// <summary>Gets or sets enabled instruments: zero for none, one for counter, two for histogram, three for both.</summary>
    [Params(0, 1, 2, 3)]
    public int Instruments { get; set; }

    /// <summary>Creates the decorator and optionally attaches a meter-specific listener.</summary>
    /// <returns>The setup operation.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        _meter = new Meter("Squirix.Benchmarks.CacheMetrics");
        var physical = new PhysicalCache<StoredValue>(TimeProvider.System, new CacheEntrySizeEstimator<StoredValue>());
        var local = new ClientCache<StoredValue>(physical, physical);
        await local.SetEntryAsync("setup", "cache", "key", _entry, CancellationToken.None).ConfigureAwait(false);
        _cache = new MetricsCacheDecorator<StoredValue>(local, new CacheMetrics(_meter));
        if (Instruments == 0)
            return;

        _listener = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                if (ReferenceEquals(instrument.Meter, _meter) &&
                    ((instrument is Counter<long> && (Instruments & 1) != 0) || (instrument is Histogram<double> && (Instruments & 2) != 0)))
                    listener.EnableMeasurementEvents(instrument);
            },
        };
        _listener.SetMeasurementEventCallback<long>(static (_, _, _, _) => { });
        _listener.SetMeasurementEventCallback<double>(static (_, _, _, _) => { });
        _listener.Start();
    }

    /// <summary>Reads a cached value through the metrics decorator.</summary>
    /// <returns>Whether the read found the seeded value.</returns>
    /// <exception cref="InvalidOperationException">The benchmark has not been initialized.</exception>
    [Benchmark]
    public async ValueTask<bool> GetAsync() => (await ThrowHelper.Required(_cache, "Benchmark was not initialized.").GetValueAsync("cache", "key", null, CancellationToken.None).ConfigureAwait(false)).Found;

    /// <summary>Overwrites a cached value through the metrics decorator.</summary>
    /// <returns>The completion of the write.</returns>
    /// <exception cref="InvalidOperationException">The benchmark has not been initialized.</exception>
    [Benchmark]
    public ValueTask SetAsync() => ThrowHelper.Required(_cache, "Benchmark was not initialized.").SetEntryAsync("op", "cache", "key", _entry, null, CancellationToken.None);

    /// <summary>Releases the listener and instruments.</summary>
    [GlobalCleanup]
    public void Cleanup()
    {
        _listener?.Dispose();
        _meter?.Dispose();
    }
}
