using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
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
    private ILogger? _log;

    internal IJournalCoordinator Coordinator => ThrowHelper.Required(_coordinator, "Journal coordinator is not initialized.");

    public async ValueTask DisposeAsync()
    {
        var coordinator = _coordinator;
        if (coordinator == null)
            return;

        // The container disposes this host as a root and stops at the first exception, so a throw here
        // would skip the manifest ledger, the replica group registry, and its follower logs. The journal
        // may throw on a leaked I/O thread (it already reported the leak loudly): log and continue.
        try
        {
            await coordinator.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is TimeoutException or AggregateException or ObjectDisposedException or IOException or InvalidOperationException)
        {
            LogManager.JournalDisposeFailedOnHostShutdown(_log ?? LogManager.GetLogger<JournalCoordinatorHost>(), ex);
        }

        _coordinator = null;
    }

    /// <summary>Replaces the owned coordinator; test seam for host disposal over a failing coordinator.</summary>
    /// <param name="coordinator">The coordinator this host owns and disposes from now on.</param>
    /// <remarks>The caller takes ownership of the displaced coordinator, if any.</remarks>
    /// <exception cref="ArgumentNullException"><paramref name="coordinator" /> is <see langword="null" />.</exception>
    internal void Attach(IJournalCoordinator coordinator)
    {
        ArgumentNullException.ThrowIfNull(coordinator);
        _coordinator = coordinator;
    }

    /// <summary>Captures the host logger while the container resolves this host, before any disposal.</summary>
    /// <param name="factory">The host logger factory, when logging is registered.</param>
    /// <returns>This host.</returns>
    internal JournalCoordinatorHost AttachLog(ILoggerFactory? factory)
    {
        _log = factory?.CreateLogger<JournalCoordinatorHost>();
        return this;
    }

    internal void Initialize(PersistenceOptions persistence, State manifest, Ledger manifestStore, AsyncManualResetEvent gate)
    {
        if (_coordinator != null)
            return;

        _coordinator = JournalCoordinatorFactory.Create(persistence, manifest, manifestStore, gate);
    }
}
