using System;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.IO;

namespace Squirix.Server.Benchmarks;

/// <summary>Hosts a manifest store for manifest publish benchmarks.</summary>
internal sealed class ManifestBenchmarkHost : IAsyncDisposable
{
    private readonly TempDirectory _dataDir;

    private ManifestBenchmarkHost(TempDirectory dataDir, ManifestStore manifestStore)
    {
        _dataDir = dataDir;
        Store = manifestStore;
    }

    internal ManifestStore Store { get; }

    public static Task<ManifestBenchmarkHost> CreateAsync(string tempDirectoryPrefix, PersistenceOptions options)
    {
        ArgumentException.ThrowIfNullOrEmpty(tempDirectoryPrefix);
        ArgumentNullException.ThrowIfNull(options);

        var dataDir = new TempDirectory(tempDirectoryPrefix);
        var persistence = options with { DataDir = dataDir.Path };
        var manifestStore = new ManifestStore(persistence);
        return Task.FromResult(new ManifestBenchmarkHost(dataDir, manifestStore));
    }

    public ValueTask DisposeAsync()
    {
        Store.Dispose();
        _dataDir.Dispose();
        return ValueTask.CompletedTask;
    }
}
