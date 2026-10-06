using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Transport;

namespace Squirix.Server.TestKit.Mtls;

/// <summary>Per-peer handlers one node startup's outbound handler factories create, and the hold on the certificate material they use; the cluster client pool leaves them to the factory's owner.</summary>
[Mutable]
internal sealed class PeerHandlers : IDisposable
{
    private readonly Lock _gate = new();
    private readonly List<HttpMessageHandler> _handlers = [];
    private MtlsCertificate.Hold? _hold;
    private int _disposed;

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        lock (_gate)
        {
            for (var i = _handlers.Count - 1; i >= 0; i--)
                _handlers[i].Dispose();

            _handlers.Clear();

            // The handlers are gone, so nothing reads the certificates through this hold any more.
            _hold?.Dispose();
            _hold = null;
        }
    }

    /// <summary>Takes ownership of the hold on the certificate material the handlers use.</summary>
    /// <param name="hold">The hold, released after the handlers are disposed.</param>
    internal void Hold(MtlsCertificate.Hold hold)
    {
        lock (_gate)
        {
            if (Volatile.Read(ref _disposed) == 1)
            {
                hold.Dispose();
                return;
            }

            _hold = hold;
        }
    }

    /// <summary>Takes ownership of a handler created for one peer.</summary>
    /// <param name="handler">The handler created for one peer.</param>
    /// <returns>The same handler, disposed when the node that created it is released or the owning identity is disposed.</returns>
    /// <exception cref="ObjectDisposedException">Thrown when the owning identity has already been disposed.</exception>
    internal SocketsHttpHandler Track(SocketsHttpHandler handler)
    {
        lock (_gate)
        {
            if (Volatile.Read(ref _disposed) == 1)
            {
                handler.Dispose();
                throw new ObjectDisposedException(nameof(ClusterIdentity));
            }

            _handlers.Add(handler);
        }

        return handler;
    }
}
