using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.Benchmarks;

namespace Squirix.Server.Benchmarks;

/// <summary>Manifest publish throughput comparing JSON and binary backends.</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 2)]
public class ManifestPublishBenchmarks
{
    private ManifestBenchmarkHost? _host;
    private int _operationsPerInvoke;
    private ulong _nextSequence = 1;

    /// <summary>Gets or sets the manifest store backend under test.</summary>
    [Params(ManifestBackend.Json, ManifestBackend.Binary)]
    public ManifestBackend Backend { get; set; }

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

    /// <summary>Creates the manifest store for the selected backend.</summary>
    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        _operationsPerInvoke = ManifestBenchmarkSupport.ResolvePublishOperationsPerInvoke();
        var retention = ManifestBenchmarkSupport.ResolveRetentionCount();
        var options = new PersistenceOptions
        {
            ManifestBackend = Backend,
            ManifestRetentionCount = retention,
            SnapshotRetentionCount = retention,
        };
        _host = await ManifestBenchmarkHost.CreateAsync($"manifest-bench-{Backend}", options).ConfigureAwait(false);
        _nextSequence = 1;

        // Warm steady-state in-memory index/cache before measured iterations.
        _host.Store.PublishRollBlocking(1, _nextSequence++);
    }
}
