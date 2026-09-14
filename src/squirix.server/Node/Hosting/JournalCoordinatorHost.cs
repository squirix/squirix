using System;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Hosting;

/// <summary>Owns the journal coordinator singleton lifetime for dependency injection.</summary>
internal sealed class JournalCoordinatorHost : IAsyncDisposable
{
    private IJournalCoordinator? _coordinator;

    internal IJournalCoordinator Coordinator => ThrowHelper.Required(_coordinator, "Journal coordinator is not initialized.");

    public async ValueTask DisposeAsync()
    {
        if (_coordinator == null)
            return;

        await _coordinator.DisposeAsync().ConfigureAwait(false);
        _coordinator = null;
    }

    internal void Initialize(PersistenceOptions persistence, State manifest, Ledger manifestStore, AsyncManualResetEvent gate)
    {
        if (_coordinator != null)
            return;

        _coordinator = JournalCoordinatorFactory.Create(persistence, manifest, manifestStore, gate);
    }
}
