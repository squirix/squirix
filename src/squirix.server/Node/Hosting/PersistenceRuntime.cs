using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Node.Hosting;

/// <summary>Groups persistence singleton instances for dependency injection registration.</summary>
internal sealed class PersistenceRuntime : IDisposable
{
    private bool _disposed;

    private PersistenceRuntime(PersistenceOptions persistence)
    {
        Retention = new RetentionCleanupReadiness(persistence);
        ManifestStore = new ManifestStore(persistence, retentionReadiness: Retention, failureMetrics: new OtelManifestRetentionMetrics());
        Gate = new JournalStartupGate(false);
        JournalCoordinator = new JournalCoordinatorHost();
    }

    internal JournalStartupGate Gate { get; }

    internal JournalCoordinatorHost JournalCoordinator { get; }

    internal ManifestStore ManifestStore { get; }

    internal RetentionCleanupReadiness Retention { get; }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
            return;

        ManifestStore.Dispose();
        _disposed = true;
    }

    internal static async Task<PersistenceRuntime> CreateAsync(PersistenceOptions persistence, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(persistence);
        var runtime = new PersistenceRuntime(persistence);
        var manifest = await runtime.ManifestStore.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        await runtime.JournalCoordinator.InitializeAsync(persistence, manifest, runtime.ManifestStore, runtime.Gate, cancellationToken).ConfigureAwait(false);
        return runtime;
    }
}
