using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit.Benchmarks;

namespace Squirix.Server.Benchmarks;

/// <summary>Snapshot strict-load throughput benchmarks (JSON vs binary backends).</summary>
[SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global", Justification = "BenchmarkDotNet [Params] properties require public setters.")]
[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 2)]
public class SnapshotReadBenchmarks
{
    private SnapshotBenchmarkHost? _host;
    private string? _snapshotPath;

    /// <summary>Gets or sets the snapshot backend discriminator (0 = JSON, 1 = binary).</summary>
    [Params(0, 1)]
    public int BackendValue { get; set; }

    /// <summary>Loads the warmed snapshot with strict validation.</summary>
    [Benchmark]
    public async Task LoadStrictAsync()
    {
        var host = _host ?? throw new InvalidOperationException("Benchmark host was not initialized.");
        _ = await host.Reader.LoadStrictAsync<object?>(_snapshotPath!, cancellationToken: CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Disposes the benchmark host and temporary data directory.</summary>
    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        if (_host is not null)
            await _host.DisposeAsync().ConfigureAwait(false);
        _host = null;
        _snapshotPath = null;
    }

    /// <summary>Creates a warmed snapshot file for the selected backend.</summary>
    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        var entryCount = SnapshotBenchmarkSupport.ResolveEntryCount();
        var options = new PersistenceOptions
        {
            SnapshotBackend = SnapshotBenchmarkOptions.BackendFromValue(BackendValue),
            ManifestRetentionCount = ManifestBenchmarkSupport.ResolveRetentionCount(),
            SnapshotRetentionCount = ManifestBenchmarkSupport.ResolveRetentionCount(),
        };
        _host = await SnapshotBenchmarkHost.CreateAsync($"snapshot-read-{BackendValue}", options, entryCount).ConfigureAwait(false);
        _snapshotPath = await _host.WriteNextSnapshotAsync().ConfigureAwait(false);
    }
}
