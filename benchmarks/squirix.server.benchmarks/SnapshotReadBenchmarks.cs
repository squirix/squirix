using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.Benchmarks;

namespace Squirix.Server.Benchmarks;

/// <summary>Binary snapshot strict-load throughput benchmarks.</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 2)]
public class SnapshotReadBenchmarks
{
    private SnapshotBenchmarkHost? _host;
    private string? _snapshotPath;

    /// <summary>Disposes the benchmark host and temporary data directory.</summary>
    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _host?.Dispose();
        _host = null;
        _snapshotPath = null;
    }

    /// <summary>Creates a warmed binary snapshot file.</summary>
    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        var entryCount = SnapshotBenchmarkSupport.ResolveEntryCount();
        var options = new PersistenceOptions
        {
            ManifestRetentionCount = ManifestBenchmarkSupport.ResolveRetentionCount(),
            SnapshotRetentionCount = ManifestBenchmarkSupport.ResolveRetentionCount(),
        };
        _host = await SnapshotBenchmarkHost.CreateAsync("snapshot-read-binary", options, entryCount).ConfigureAwait(false);
        _snapshotPath = await _host.WriteNextSnapshotAsync().ConfigureAwait(false);
    }

    /// <summary>Loads the warmed snapshot with strict validation.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark host was not initialized.</exception>
    [Benchmark]
    public async Task LoadStrictAsync()
    {
        var host = _host ?? throw new InvalidOperationException("Benchmark host was not initialized.");
        _ = await host.Reader.LoadStrictAsync<object?>(_snapshotPath!, cancellationToken: CancellationToken.None).ConfigureAwait(false);
    }
}
