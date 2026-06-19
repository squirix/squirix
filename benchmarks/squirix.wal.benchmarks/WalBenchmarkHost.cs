using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.PipelinedWal;

namespace Squirix.Wal.Benchmarks;

/// <summary>Hosts a journal coordinator for WAL benchmarks.</summary>
internal sealed class WalBenchmarkHost : IAsyncDisposable
{
    private readonly ManifestStore _manifestStore;

    private WalBenchmarkHost(IJournalCoordinator coordinator, ManifestStore manifestStore, string dataDir)
    {
        Coordinator = coordinator;
        _manifestStore = manifestStore;
        DataDir = dataDir;
    }

    public IJournalCoordinator Coordinator { get; }

    public string DataDir { get; }

    public static async Task<WalBenchmarkHost> CreateAsync(PersistenceOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        var manifestStore = new ManifestStore(options);
        var gate = new JournalStartupGate(isOpen: true);
        var manifest = await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        var coordinator = await JournalCoordinatorFactory.CreateAsync(options, manifest, manifestStore, gate, cancellationToken).ConfigureAwait(false);
        return new WalBenchmarkHost(coordinator, manifestStore, options.DataDir);
    }

    public async ValueTask DisposeAsync()
    {
        await Coordinator.DisposeAsync().ConfigureAwait(false);
        _manifestStore.Dispose();
    }
}
