using System;
using System.IO;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Transport;

/// <summary>A connection of an owned internode handler, tracked in the pool's connection gate from the connect attempt until the stream is disposed.</summary>
/// <remarks>
/// The TLS stack reads the client certificate through the stream's lifetime: the handshake runs on it, and a failed or cancelled handshake disposes
/// it. The pool therefore waits until the gate drains before it releases the certificate material.
/// </remarks>
[Mutable]
internal sealed class TrackedConnectionStream : NetworkStream
{
    private readonly TrackedConnections _connections;

    private readonly Socket _socket;

    /// <summary>One while the stream holds its connection entry; set only once the base constructor succeeded, so the finalizer of a half-built stream releases nothing.</summary>
    private int _tracked;

    /// <summary>Initializes a new instance of the <see cref="TrackedConnectionStream" /> class.</summary>
    /// <param name="socket">The connected socket, owned by the stream.</param>
    /// <param name="connections">The pool's connections, left when the stream is disposed.</param>
    /// <exception cref="IOException"><paramref name="socket" /> is not connected.</exception>
    internal TrackedConnectionStream(Socket socket, TrackedConnections connections)
        : base(socket, true)
    {
        _socket = socket;
        _connections = connections;
        _tracked = 1;
    }

    /// <summary>Connects to the peer as the runtime default does (TCP, no delay, dual-stack DNS endpoint) and tracks the connection in <paramref name="connections" />.</summary>
    /// <param name="connections">The pool's connections.</param>
    /// <param name="context">The connection request.</param>
    /// <param name="cancellationToken">Cancels the connect attempt.</param>
    /// <returns>The tracked connection stream, which owns the socket.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="connections" /> or <paramref name="context" /> is null.</exception>
    /// <exception cref="ObjectDisposedException">The pool started to dispose and admits no new connection.</exception>
    internal static async ValueTask<Stream> ConnectAsync(TrackedConnections connections, SocketsHttpConnectionContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connections);
        ArgumentNullException.ThrowIfNull(context);
        if (!connections.TryEnter())
            throw new ObjectDisposedException(nameof(ServerClientPool), "The server client pool is disposing and admits no new connection.");

        Socket? socket = null;
        var created = false;
        try
        {
            socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
            connections.Register(socket);
            await socket.ConnectAsync(context.DnsEndPoint, cancellationToken).ConfigureAwait(false);
            var stream = new TrackedConnectionStream(socket, connections);
            created = true;
            return stream;
        }
        finally
        {
            // Whatever failed, no stream exists to leave the gate later, so the entry is left here.
            if (!created)
            {
                if (socket != null)
                {
                    connections.Unregister(socket);
                    socket.Dispose();
                }

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
            if (Interlocked.Exchange(ref _tracked, 0) == 1)
            {
                _connections.Unregister(_socket);
                _connections.Exit();
            }
        }
    }
}
