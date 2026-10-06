using System;
using System.Collections.Generic;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.TestKit.Mtls;

/// <summary>The outbound handlers of each node started from one <see cref="ClusterIdentity" />, from the node's startup until its host is disposed.</summary>
[Mutable]
internal sealed class PeerHandlerRegistry
{
    private readonly Lock _gate = new();
    private readonly Dictionary<string, PeerHandlers> _nodes = [with(StringComparer.Ordinal)];

    /// <summary>Registers fresh handlers for a node's startup; handlers of an earlier startup that was never released are disposed.</summary>
    /// <param name="nodeId">Identifier of the node that starts.</param>
    /// <returns>The handlers the node's startup creates its outbound handlers in.</returns>
    internal PeerHandlers Begin(string nodeId)
    {
        var created = new PeerHandlers();
        PeerHandlers? previous;
        lock (_gate)
        {
            _ = _nodes.Remove(nodeId, out previous);
            _nodes[nodeId] = created;
        }

        previous?.Dispose();
        return created;
    }

    /// <summary>Creates the scope that releases a node's handlers when the node's host is disposed.</summary>
    /// <param name="nodeId">Identifier of the node whose host owns the scope.</param>
    /// <returns>A disposable that calls <see cref="Release" /> once.</returns>
    internal IDisposable CreateScope(string nodeId) => new NodeScope(this, nodeId);

    /// <summary>Disposes the handlers of every node.</summary>
    internal void DisposeAll()
    {
        PeerHandlers[] all;
        lock (_gate)
        {
            all = new PeerHandlers[_nodes.Count];
            _nodes.Values.CopyTo(all, 0);
            _nodes.Clear();
        }

        for (var i = 0; i < all.Length; i++)
            all[i].Dispose();
    }

    /// <summary>Disposes the handlers a node's last startup created and releases their hold on the node's certificate material.</summary>
    /// <param name="nodeId">Identifier of the node that stopped.</param>
    /// <remarks>Call it after the node's host stopped, so its client pool has already drained its connections. The node's next startup creates new handlers.</remarks>
    internal void Release(string nodeId)
    {
        PeerHandlers? handlers;
        lock (_gate)
        {
            if (!_nodes.Remove(nodeId, out handlers))
                return;
        }

        handlers.Dispose();
    }

    [Mutable]
    private sealed class NodeScope : IDisposable
    {
        private readonly string _nodeId;
        private readonly PeerHandlerRegistry _registry;
        private int _released;

        internal NodeScope(PeerHandlerRegistry registry, string nodeId)
        {
            _registry = registry;
            _nodeId = nodeId;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _released, 1) == 0)
                _registry.Release(_nodeId);
        }
    }
}
