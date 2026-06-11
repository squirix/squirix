using System;
using System.Threading.Tasks;
using Squirix.Server.LocalCache;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.IO;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>
/// Owns common recovery test infrastructure for focused journal and manifest scenarios.
/// </summary>
internal sealed class RecoveryScenarioBuilder : IAsyncDisposable
{
    private bool _disposed;

    private RecoveryScenarioBuilder(string dataDir)
    {
        DataDir = dataDir;
        Persistence = new PersistenceOptions { DataDir = dataDir, JournalMaxSegmentMb = 16, FlushIntervalMs = 5 };
        ManifestStore = new ManifestStore(Persistence);
        Cache = new PhysicalCache<object?>();
    }

    /// <summary>
    /// Gets the cache populated by recovery.
    /// </summary>
    internal PhysicalCache<object?> Cache { get; }

    /// <summary>
    /// Gets the scenario data directory.
    /// </summary>
    internal string DataDir { get; }

    /// <summary>
    /// Gets the scenario manifest store.
    /// </summary>
    internal ManifestStore ManifestStore { get; }

    /// <summary>
    /// Gets the scenario persistence options.
    /// </summary>
    private PersistenceOptions Persistence { get; }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        await Cache.DisposeAsync().ConfigureAwait(false);
        DirectoryKit.TryDeleteDirectory(DataDir);
    }

    /// <summary>
    /// Creates a recovery scenario with an owned temporary data directory.
    /// </summary>
    /// <param name="prefix">Temporary directory prefix.</param>
    /// <returns>A configured recovery scenario.</returns>
    internal static RecoveryScenarioBuilder Create(string prefix) => new(DirectoryKit.CreateTempDirectory(prefix));
}
