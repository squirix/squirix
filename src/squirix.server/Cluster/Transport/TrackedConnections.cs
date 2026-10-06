using System.Collections.Generic;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;

namespace Squirix.Server.Cluster.Transport;

/// <summary>The connections of a pool's owned handlers: counted from the connect attempt until the stream is disposed, and abortable as a set.</summary>
/// <remarks>
/// The count gates the release of the certificate material. Aborting closes the sockets so a stalled handshake fails at once instead of
/// running to the connect timeout; the count still drops only when the owning stream is disposed.
/// </remarks>
[Mutable]
internal sealed class TrackedConnections
{
    private readonly QuiescenceGate _gate = new();

    private readonly Lock _lock = new();

    private readonly HashSet<Socket> _sockets = [];

    private bool _aborted;

    /// <summary>Gets the number of connections that are still open.</summary>
    internal int Pending => _gate.Pending;

    /// <summary>Closes the sockets of every open connection and of every connection registered later.</summary>
    internal void AbortAll()
    {
        Socket[] sockets;
        lock (_lock)
        {
            _aborted = true;
            sockets = new Socket[_sockets.Count];
            _sockets.CopyTo(sockets);
        }

        for (var i = 0; i < sockets.Length; i++)
            sockets[i].Dispose();
    }

    /// <summary>Refuses new connections.</summary>
    internal void Close() => _gate.Close();

    /// <summary>Tracks one more connection regardless of whether new connections are admitted.</summary>
    internal void Enter() => _gate.Enter();

    /// <summary>Ends tracking of one connection.</summary>
    internal void Exit() => _gate.Exit();

    /// <summary>Makes the socket abortable; a socket registered after <see cref="AbortAll" /> is closed at once.</summary>
    /// <param name="socket">The socket of a connection that was admitted.</param>
    internal void Register(Socket socket)
    {
        bool aborted;
        lock (_lock)
        {
            aborted = _aborted;
            if (!aborted)
                _ = _sockets.Add(socket);
        }

        if (aborted)
            socket.Dispose();
    }

    /// <summary>Tracks one more connection unless new connections are refused.</summary>
    /// <returns><see langword="true" /> when the connection was admitted and must <see cref="Exit" />.</returns>
    internal bool TryEnter() => _gate.TryEnter();

    /// <summary>Makes the socket no longer abortable.</summary>
    /// <param name="socket">The socket passed to <see cref="Register" />.</param>
    internal void Unregister(Socket socket)
    {
        lock (_lock)
            _ = _sockets.Remove(socket);
    }

    /// <summary>Waits until every connection ended.</summary>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <returns>A task that completes when no connection is open.</returns>
    internal ValueTask WaitAsync(CancellationToken cancellationToken) => _gate.WaitAsync(cancellationToken);
}
