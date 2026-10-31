using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>String writes through admission, payload validation and physical memory accounting, without persistence.</summary>
[MemoryDiagnoser]
public class CacheWriteSizingBenchmarks
{
    private MemoryAdmissionCacheDecorator<object?>? _cache;
    private Meter? _meter;
    private string _value = string.Empty;

    /// <summary>Gets or sets the number of UTF-16 code units in the value.</summary>
    [Params(16, 4096, 65536)]
    public int Length { get; set; }

    /// <summary>Gets or sets a value indicating whether the value uses three-byte UTF-8 characters.</summary>
    [Params(false, true)]
    public bool Unicode { get; set; }

    /// <summary>Creates the real owner-local write pipeline with ample memory capacity.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _meter = new Meter("Squirix.Benchmarks.StringSizing");
        _value = new string(Unicode ? '\u754c' : 'x', Length);
        var estimator = new ObjectCacheEntrySizeEstimator();
        var physical = new PhysicalCache<object?>(TimeProvider.System, estimator);
        var client = new ClientCache<object?>(physical, physical);
        var gate = new PressureGate(
            new StateEvaluator(new PressureOptions { MaxEstimatedCacheBytes = 1_000_000_000 }),
            new MemoryUsageAccounting(physical),
            "node",
            _meter);
        _cache = new MemoryAdmissionCacheDecorator<object?>(new OwnerPutPayloadGuardDecorator<object?>(client), gate, estimator);
    }

    /// <summary>Writes a fresh request entry, overwriting one key so memory usage stays bounded.</summary>
    /// <returns>The completion of the write.</returns>
    /// <exception cref="InvalidOperationException">The benchmark has not been initialized.</exception>
    [Benchmark]
    public ValueTask SetAsync() => ThrowHelper.Required(_cache, "Benchmark was not initialized.").SetEntryAsync("op", "cache", "key", new NodeCacheEntry<object?>(_value), CancellationToken.None);

    /// <summary>Releases the instruments.</summary>
    [GlobalCleanup]
    public void Cleanup() => _meter?.Dispose();
}
