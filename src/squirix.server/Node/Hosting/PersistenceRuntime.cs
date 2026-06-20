using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.JsonFramed;

namespace Squirix.Server.Node.Hosting;

/// <summary>Groups persistence singleton instances for dependency injection registration.</summary>
internal sealed class PersistenceRuntime
{
    private PersistenceRuntime(PersistenceOptions persistence)
    {
        Retention = new StorageRetentionCleanupReadiness(persistence);
        ManifestStore = new ManifestStore(persistence, retentionReadiness: Retention);
        Gate = new JournalStartupGate(false);
        JournalCoordinator = new JournalCoordinatorHost();
    }

    public StorageRetentionCleanupReadiness Retention { get; }

    public ManifestStore ManifestStore { get; }

    public JournalStartupGate Gate { get; }

    public JournalCoordinatorHost JournalCoordinator { get; }

    public static async Task<PersistenceRuntime> CreateAsync(PersistenceOptions persistence, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(persistence);
        var runtime = new PersistenceRuntime(persistence);
        var manifest = await runtime.ManifestStore.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        await runtime.JournalCoordinator.InitializeAsync(persistence, manifest, runtime.ManifestStore, runtime.Gate, cancellationToken).ConfigureAwait(false);
        return runtime;
    }
}
