using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Cluster.Transport;

/// <summary>The connections of a pool's owned handlers: counted from the connect attempt until the stream is disposed, and abortable as a set.</summary>
/// <remarks>
/// The count gates the release of the certificate material. Aborting closes the connections so a stalled handshake fails at once instead of
/// running to the connect timeout; the count still drops only when the owning stream is disposed.
/// </remarks>
[Mutable]
internal sealed class TrackedConnections
{
    private readonly QuiescenceGate _gate = new();

    private readonly Lock _lock = new();

    private readonly HashSet<IDisposable> _connections = [];

    private bool _aborted;

    /// <summary>Gets the task that releases a material hold once the connections drained; completed until <see cref="ReleaseWhenDrained" /> ran.</summary>
    internal Task LateRelease { get; private set; } = Task.CompletedTask;

    /// <summary>Gets the number of connections that are still open.</summary>
    internal int Pending => _gate.Pending;

    /// <summary>Closes every open connection and every connection registered later.</summary>
    internal void AbortAll()
    {
        IDisposable[] open;
        lock (_lock)
        {
            _aborted = true;
            open = new IDisposable[_connections.Count];
            _connections.CopyTo(open);
        }

        for (var i = 0; i < open.Length; i++)
            open[i].Dispose();
    }

    /// <summary>Refuses new connections.</summary>
    internal void Close() => _gate.Close();

    /// <summary>Tracks one more connection regardless of whether new connections are admitted.</summary>
    internal void Enter() => _gate.Enter();

    /// <summary>Ends tracking of one connection.</summary>
    internal void Exit() => _gate.Exit();

    /// <summary>Makes the connection abortable; a connection registered after <see cref="AbortAll" /> is closed at once.</summary>
    /// <param name="connection">The socket or stream of a connection that was admitted.</param>
    internal void Register(IDisposable connection)
    {
        bool aborted;
        lock (_lock)
        {
            aborted = _aborted;
            if (!aborted)
                _ = _connections.Add(connection);
        }

        if (aborted)
            connection.Dispose();
    }

    /// <summary>Releases the hold once every connection ended; the task never faults, a failure is logged and the hold is kept.</summary>
    /// <param name="hold">The hold on the material the connections read.</param>
    /// <param name="logger">The logger of the outcome.</param>
    internal void ReleaseWhenDrained(MtlsCertificate.Hold hold, ILogger logger) => LateRelease = ReleaseWhenDrainedAsync(hold, logger);

    /// <summary>Tracks one more connection unless new connections are refused.</summary>
    /// <returns><see langword="true" /> when the connection was admitted and must <see cref="Exit" />.</returns>
    internal bool TryEnter() => _gate.TryEnter();

    /// <summary>Makes the connection no longer abortable.</summary>
    /// <param name="connection">The connection passed to <see cref="Register" />.</param>
    internal void Unregister(IDisposable connection)
    {
        lock (_lock)
            _ = _connections.Remove(connection);
    }

    /// <summary>Waits until every connection ended.</summary>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <returns>A task that completes when no connection is open.</returns>
    internal ValueTask WaitAsync(CancellationToken cancellationToken) => _gate.WaitAsync(cancellationToken);

    private async Task ReleaseWhenDrainedAsync(MtlsCertificate.Hold hold, ILogger logger)
    {
        var failure = await _gate.WaitAsync(CancellationToken.None).AsTask().CaptureFailureAsync().ConfigureAwait(false);
        if (failure != null)
        {
            ServerLog.ClientPoolLateMaterialReleaseFailed(logger, failure);
            return;
        }

        hold.Dispose();
        ServerLog.ClientPoolMaterialReleasedLate(logger);
    }
}
