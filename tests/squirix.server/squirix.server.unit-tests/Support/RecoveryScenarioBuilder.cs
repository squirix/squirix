using System;
using System.Threading;
using Squirix.Server.LocalCache;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit.IO;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Owns common recovery test infrastructure for focused journal and manifest scenarios.</summary>
internal sealed class RecoveryScenarioBuilder : IDisposable
{
    private readonly TempDirectory _dataDirectory;
    private int _disposed;

    private RecoveryScenarioBuilder(TempDirectory dataDirectory)
    {
        _dataDirectory = dataDirectory;
        DataDir = dataDirectory.Path;
        Persistence = new PersistenceOptions { DataDir = dataDirectory.Path, JournalMaxSegmentMb = 16, FlushIntervalMs = 5 };
        Ledger = new Ledger(Persistence);
        Cache = new PhysicalCache<object?>();
    }

    /// <summary>Gets the cache populated by recovery.</summary>
    internal PhysicalCache<object?> Cache { get; }

    /// <summary>Gets the scenario data directory.</summary>
    internal string DataDir { get; }

    /// <summary>Gets the scenario manifest store.</summary>
    internal Ledger Ledger { get; }

    /// <summary>Gets the scenario persistence options.</summary>
    private PersistenceOptions Persistence { get; }

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        Ledger.Dispose();
        _dataDirectory.Dispose();
    }

    /// <summary>Creates a recovery scenario with an owned temporary data directory.</summary>
    /// <param name="prefix">Temporary directory prefix.</param>
    /// <returns>A configured recovery scenario.</returns>
    internal static RecoveryScenarioBuilder Create(string prefix) => new(new TempDirectory(prefix));
}
