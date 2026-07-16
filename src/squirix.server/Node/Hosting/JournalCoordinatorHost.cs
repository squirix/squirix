using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;

namespace Squirix.Server.Node.Hosting;

/// <summary>Owns the journal coordinator singleton lifetime for dependency injection.</summary>
internal sealed class JournalCoordinatorHost : IAsyncDisposable
{
    private IJournalCoordinator? _coordinator;

    internal IJournalCoordinator Coordinator => _coordinator ?? throw new InvalidOperationException("Journal coordinator is not initialized.");

    public async ValueTask DisposeAsync()
    {
        if (_coordinator is null)
            return;

        await _coordinator.DisposeAsync().ConfigureAwait(false);
        _coordinator = null;
    }

    internal async Task InitializeAsync(
        PersistenceOptions persistence,
        State manifest,
        ManifestStore manifestStore,
        JournalStartupGate gate,
        CancellationToken cancellationToken)
    {
        if (_coordinator is not null)
            return;

        _coordinator = await JournalCoordinatorFactory.CreateAsync(persistence, manifest, manifestStore, gate, cancellationToken).ConfigureAwait(false);
    }
}
