using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit.Benchmarks;

namespace Squirix.Server.Benchmarks;

/// <summary>Snapshot write throughput benchmarks (JSON vs binary backends).</summary>
[SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global", Justification = "BenchmarkDotNet [Params] properties require public setters.")]
[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 2)]
public class SnapshotWriteBenchmarks
{
    private SnapshotBenchmarkHost? _host;
    private int _operationsPerInvoke;

    /// <summary>Gets or sets the snapshot backend discriminator (0 = JSON, 1 = binary).</summary>
    [Params(0, 1)]
    public int BackendValue { get; set; }

    /// <summary>Writes repeated full snapshots using the selected backend.</summary>
    [Benchmark]
    public async Task WriteSnapshotAsync()
    {
        var host = _host ?? throw new InvalidOperationException("Benchmark host was not initialized.");
        for (var i = 0; i < _operationsPerInvoke; i++)
            _ = await host.WriteNextSnapshotAsync().ConfigureAwait(false);
    }

    /// <summary>Disposes the benchmark host and temporary data directory.</summary>
    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        if (_host is not null)
            await _host.DisposeAsync().ConfigureAwait(false);
        _host = null;
    }

    /// <summary>Creates a warmed snapshot writer for the selected backend.</summary>
    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        _operationsPerInvoke = SnapshotBenchmarkSupport.ResolveOperationsPerInvoke();
        var entryCount = SnapshotBenchmarkSupport.ResolveEntryCount();
        var options = new PersistenceOptions
        {
            SnapshotBackend = SnapshotBenchmarkOptions.BackendFromValue(BackendValue),
            ManifestRetentionCount = ManifestBenchmarkSupport.ResolveRetentionCount(),
            SnapshotRetentionCount = ManifestBenchmarkSupport.ResolveRetentionCount(),
        };
        _host = await SnapshotBenchmarkHost.CreateAsync($"snapshot-write-{BackendValue}", options, entryCount).ConfigureAwait(false);
        _ = await _host.WriteNextSnapshotAsync().ConfigureAwait(false);
    }
}
