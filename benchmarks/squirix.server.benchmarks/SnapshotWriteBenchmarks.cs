using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.Benchmarks;

namespace Squirix.Server.Benchmarks;

/// <summary>Binary snapshot write throughput benchmarks.</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 2)]
public class SnapshotWriteBenchmarks
{
    private SnapshotBenchmarkHost? _host;
    private int _operationsPerInvoke;

    /// <summary>Disposes the benchmark host and temporary data directory.</summary>
    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        if (_host is not null)
            await _host.DisposeAsync().ConfigureAwait(false);
        _host = null;
    }

    /// <summary>Creates a warmed binary snapshot writer.</summary>
    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        _operationsPerInvoke = SnapshotBenchmarkSupport.ResolveOperationsPerInvoke();
        var entryCount = SnapshotBenchmarkSupport.ResolveEntryCount();
        var options = new PersistenceOptions
        {
            ManifestRetentionCount = ManifestBenchmarkSupport.ResolveRetentionCount(),
            SnapshotRetentionCount = ManifestBenchmarkSupport.ResolveRetentionCount(),
        };
        _host = await SnapshotBenchmarkHost.CreateAsync("snapshot-write-binary", options, entryCount).ConfigureAwait(false);
        _ = await _host.WriteNextSnapshotAsync().ConfigureAwait(false);
    }

    /// <summary>Writes repeated full snapshots using the binary backend.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark host was not initialized.</exception>
    [Benchmark]
    public async Task WriteSnapshotAsync()
    {
        var host = _host ?? throw new InvalidOperationException("Benchmark host was not initialized.");
        for (var i = 0; i < _operationsPerInvoke; i++)
            _ = await host.WriteNextSnapshotAsync().ConfigureAwait(false);
    }
}
