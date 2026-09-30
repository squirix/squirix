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
    private readonly ILogger<JournalCoordinatorHost> _log;
    private IJournalCoordinator? _coordinator;

    /// <summary>Initializes a new instance of the <see cref="JournalCoordinatorHost" /> class.</summary>
    /// <param name="log">The logger for startup repairs and disposal failures.</param>
    internal JournalCoordinatorHost(ILogger<JournalCoordinatorHost> log)
    {
        ArgumentNullException.ThrowIfNull(log);
        _log = log;
    }

    /// <summary>Gets the owned journal coordinator.</summary>
    /// <exception cref="InvalidOperationException">Storage has not been opened.</exception>
    internal IJournalCoordinator Coordinator => ThrowHelper.Required(_coordinator, "Squirix storage is not opened; call MapSquirixServerAsync before starting the application.");

    public async ValueTask DisposeAsync()
    {
        var coordinator = _coordinator;
        if (coordinator == null)
            return;

        // The container disposes this host as a root and stops at the first exception, so a throw here
        // would skip the manifest ledger, the replica group registry, and its follower logs. The journal
        // itself never throws from disposal; the filter is defence in depth for any other coordinator this host is handed.
        var failure = await coordinator.DisposeAsync()
                                       .CaptureFailureAsync(static ex => ex is TimeoutException or AggregateException or ObjectDisposedException or IOException or InvalidOperationException)
                                       .ConfigureAwait(false);
        if (failure != null)
            LogManager.JournalDisposeFailedOnHostShutdown(_log, failure);

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

    /// <summary>Creates the journal coordinator, running startup repair and logging what it repaired.</summary>
    /// <param name="persistence">Resolved persistence options.</param>
    /// <param name="manifest">The current manifest state.</param>
    /// <param name="manifestStore">The manifest ledger.</param>
    /// <param name="gate">The recovery readiness gate.</param>
    internal void Open(PersistenceOptions persistence, State manifest, Ledger manifestStore, AsyncManualResetEvent gate)
    {
        if (_coordinator != null)
            return;

        _coordinator = JournalCoordinatorFactory.CreateReporting(persistence, manifest, manifestStore, gate, out var repairs);
        JournalCoordinatorFactory.LogRepairs(repairs, _log);
    }
}
