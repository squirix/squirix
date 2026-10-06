using System;
using System.IO;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;

namespace Squirix.Server.Cluster.Transport;

/// <summary>A connection of an owned internode handler, tracked in the pool's connection gate from the connect attempt until the stream is disposed.</summary>
/// <remarks>
/// The TLS stack reads the client certificate through the stream's lifetime: the handshake runs on it, and a failed or cancelled handshake disposes
/// it. The pool therefore waits until the gate drains before it releases the certificate material.
/// </remarks>
[Mutable]
internal sealed class TrackedConnectionStream : NetworkStream
{
    private readonly QuiescenceGate _connections;

    private int _released;

    private TrackedConnectionStream(Socket socket, QuiescenceGate connections)
        : base(socket, true)
    {
        _connections = connections;
    }

    /// <summary>Connects to the peer as the runtime default does (TCP, no delay, dual-stack DNS endpoint) and tracks the connection in <paramref name="connections" />.</summary>
    /// <param name="connections">The pool's connection gate.</param>
    /// <param name="context">The connection request.</param>
    /// <param name="cancellationToken">Cancels the connect attempt.</param>
    /// <returns>The tracked connection stream, which owns the socket.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="connections" /> or <paramref name="context" /> is null.</exception>
    /// <exception cref="ObjectDisposedException">The pool started to dispose and admits no new connection.</exception>
    internal static async ValueTask<Stream> ConnectAsync(QuiescenceGate connections, SocketsHttpConnectionContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connections);
        ArgumentNullException.ThrowIfNull(context);
        if (!connections.TryEnter())
            throw new ObjectDisposedException(nameof(ServerClientPool), "The server client pool is disposing and admits no new connection.");

        Socket? socket = new(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        try
        {
            await socket.ConnectAsync(context.DnsEndPoint, cancellationToken).ConfigureAwait(false);
            var stream = new TrackedConnectionStream(socket, connections);
            socket = null;
            return stream;
        }
        finally
        {
            // Still set when the connect or the stream failed: the socket never reached a stream, so the gate is left here.
            if (socket != null)
            {
                socket.Dispose();
                connections.Exit();
            }
        }
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        try
        {
            base.Dispose(disposing);
        }
        finally
        {
            if (Interlocked.Exchange(ref _released, 1) == 0)
                _connections.Exit();
        }
    }
}
