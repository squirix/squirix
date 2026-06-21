using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.Benchmarks;

namespace Squirix.Server.Benchmarks;

/// <summary>Manifest publish throughput (segment-roll manifest updates).</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 2)]
public class ManifestPublishBenchmarks
{
    private ManifestBenchmarkHost? _host;
    private int _operationsPerInvoke;
    private ulong _nextSequence = 1;

    /// <summary>Publishes sequential manifest snapshots (simulates segment-roll manifest updates).</summary>
    [Benchmark]
    public void PublishManifest()
    {
        var host = _host ?? throw new InvalidOperationException("Benchmark host was not initialized.");
        for (var journal = 1; journal <= _operationsPerInvoke; journal++)
            host.Store.PublishRollBlocking(journal, _nextSequence++);
    }

    /// <summary>Disposes the manifest store and temporary data directory.</summary>
    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        if (_host is not null)
            await _host.DisposeAsync().ConfigureAwait(false);
        _host = null;
    }

    /// <summary>Creates a warmed manifest store.</summary>
    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        _operationsPerInvoke = ManifestBenchmarkSupport.ResolvePublishOperationsPerInvoke();
        var retention = ManifestBenchmarkSupport.ResolveRetentionCount();
        var options = new PersistenceOptions
        {
            ManifestRetentionCount = retention,
            SnapshotRetentionCount = retention,
        };
        _host = await ManifestBenchmarkHost.CreateAsync("manifest-bench", options).ConfigureAwait(false);
        _nextSequence = 1;

        // Warm steady-state in-memory index/cache before measured iterations.
        _host.Store.PublishRollBlocking(1, _nextSequence++);
    }
}
