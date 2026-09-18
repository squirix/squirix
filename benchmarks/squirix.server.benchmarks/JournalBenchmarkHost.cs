using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Threading;

namespace Squirix.Server.Benchmarks;

/// <summary>Hosts a journal coordinator for journal benchmarks.</summary>
[Immutable]
internal sealed class JournalBenchmarkHost : IAsyncDisposable
{
    private readonly TempDirectory _dir;
    private readonly Ledger _manifestStore;

    private JournalBenchmarkHost(TempDirectory dir, IJournalCoordinator coordinator, Ledger manifestStore)
    {
        _dir = dir;
        Coordinator = coordinator;
        _manifestStore = manifestStore;
    }

    internal IJournalCoordinator Coordinator { get; }

    public async ValueTask DisposeAsync()
    {
        await Coordinator.DisposeAsync().ConfigureAwait(false);
        _manifestStore.Dispose();
        _dir.Dispose();
    }

    internal static async Task<JournalBenchmarkHost> CreateAsync(string tempDirectoryPrefix, PersistenceOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tempDirectoryPrefix);
        ArgumentNullException.ThrowIfNull(options);

        var dir = new TempDirectory(tempDirectoryPrefix);
        var persistence = options with { DataDir = dir };
        var manifestStore = new Ledger(persistence);
        var gate = new AsyncManualResetEvent(true);
        var manifest = await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        var coordinator = JournalCoordinatorFactory.Create(persistence, manifest, manifestStore, gate);
        return new JournalBenchmarkHost(dir, coordinator, manifestStore);
    }
}
