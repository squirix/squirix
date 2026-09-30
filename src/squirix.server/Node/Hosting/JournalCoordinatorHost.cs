using System;
using System.Collections.Generic;
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
    private IReadOnlyList<JournalRepair> _startupRepairs = [];

    internal IJournalCoordinator Coordinator => ThrowHelper.Required(_coordinator, "Journal coordinator is not initialized.");

    public async ValueTask DisposeAsync()
    {
        var coordinator = _coordinator;
        if (coordinator == null)
            return;

        // The container disposes this host as a root and stops at the first exception, so a throw here
        // would skip the manifest ledger, the replica group registry, and its follower logs. The journal
        // itself never throws from disposal; the filter is defence in depth for any other coordinator this host is handed.
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

    /// <summary>Stops the owned journal within its own shutdown budget, so a failure to drain it reaches the caller instead of a disposal.</summary>
    /// <returns>A task that completes when the journal is stopped.</returns>
    /// <remarks>Rethrows whatever <see cref="IJournalCoordinatorShutdown.StopAsync" /> reports; see it for the exact contract of a retry.</remarks>
    /// <exception cref="TimeoutException">The journal thread did not exit in time; the journal stays open until a later stop or disposal.</exception>
    /// <exception cref="IOException">The final write or flush failed, or accepted frames were lost, so acknowledged frames may not be durable.</exception>
    /// <exception cref="InvalidOperationException">The journal thread latched a failure, so acknowledged frames may not be durable.</exception>
    /// <exception cref="ObjectDisposedException">The journal thread latched a failure other than a refused shutdown step.</exception>
    internal ValueTask StopAsync() => _coordinator is IJournalCoordinatorShutdown shutdown ? shutdown.StopAsync() : ValueTask.CompletedTask;

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
        if (_log != null)
        {
            JournalCoordinatorFactory.LogRepairs(_startupRepairs, _log);
            _startupRepairs = [];
        }

        return this;
    }

    internal void Initialize(PersistenceOptions persistence, State manifest, Ledger manifestStore, AsyncManualResetEvent gate)
    {
        if (_coordinator != null)
            return;

        _coordinator = JournalCoordinatorFactory.CreateReporting(persistence, manifest, manifestStore, gate, out _startupRepairs);
    }
}
