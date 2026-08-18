using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit.Benchmarks;
using Squirix.Server.TestKit.IO;

namespace Squirix.Server.Benchmarks;

/// <summary>Manifest publish throughput (segment-roll manifest updates).</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 2)]
public class ManifestPublishBenchmarks
{
    private Host? _host;
    private ulong _nextSequence = 1;
    private int _operationsPerInvoke;

    /// <summary>Disposes the manifest store and temporary data directory.</summary>
    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        if (_host != null)
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
        _host = await Host.CreateAsync("manifest-bench", options).ConfigureAwait(false);
        _nextSequence = 1;

        // Warm steady-state in-memory index/cache before measured iterations.
        _host.Ledger.PublishRollBlocking(1, _nextSequence++);
    }

    /// <summary>Publishes sequential manifest snapshots (simulates segment-roll manifest updates).</summary>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark host was not initialized.</exception>
    [Benchmark]
    public void PublishManifest()
    {
        var host = _host ?? throw new InvalidOperationException("Benchmark host was not initialized.");
        for (var journal = 1; journal <= _operationsPerInvoke; journal++)
            host.Ledger.PublishRollBlocking(journal, _nextSequence++);
    }

    /// <summary>Hosts a manifest store for manifest publish benchmarks.</summary>
    [Immutable]
    private sealed class Host : IAsyncDisposable
    {
        private readonly TempDirectory _dataDir;

        private Host(TempDirectory dataDir, Ledger manifestStore)
        {
            _dataDir = dataDir;
            Ledger = manifestStore;
        }

        internal Ledger Ledger { get; }

        public ValueTask DisposeAsync()
        {
            Ledger.Dispose();
            _dataDir.Dispose();
            return ValueTask.CompletedTask;
        }

        internal static Task<Host> CreateAsync(string tempDirectoryPrefix, PersistenceOptions options)
        {
            ArgumentException.ThrowIfNullOrEmpty(tempDirectoryPrefix);
            ArgumentNullException.ThrowIfNull(options);

            var dataDir = new TempDirectory(tempDirectoryPrefix);
            var persistence = options with { DataDir = dataDir.Path };
            var manifestStore = new Ledger(persistence);
            return Task.FromResult(new Host(dataDir, manifestStore));
        }
    }
}
