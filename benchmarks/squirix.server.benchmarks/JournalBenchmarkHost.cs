using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.JsonFramed;

namespace Squirix.Server.Benchmarks;

/// <summary>Hosts a journal coordinator for journal benchmarks.</summary>
internal sealed class JournalBenchmarkHost : IAsyncDisposable
{
    private readonly ManifestStore _manifestStore;

    private JournalBenchmarkHost(IJournalCoordinator coordinator, ManifestStore manifestStore)
    {
        Coordinator = coordinator;
        _manifestStore = manifestStore;
    }

    public IJournalCoordinator Coordinator { get; }

    public static async Task<JournalBenchmarkHost> CreateAsync(PersistenceOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        var manifestStore = new ManifestStore(options);
        var gate = new JournalStartupGate();
        var manifest = await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        var coordinator = await JournalCoordinatorFactory.CreateAsync(options, manifest, manifestStore, gate, cancellationToken).ConfigureAwait(false);
        return new JournalBenchmarkHost(coordinator, manifestStore);
    }

    public async ValueTask DisposeAsync()
    {
        await Coordinator.DisposeAsync().ConfigureAwait(false);
        _manifestStore.Dispose();
    }
}
