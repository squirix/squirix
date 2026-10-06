using System;
using System.Collections.Generic;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.TestKit.Mtls;

/// <summary>The outbound handlers of the node startups made from one <see cref="ClusterIdentity" />, from a startup until its node's host is disposed.</summary>
[Mutable]
internal sealed class PeerHandlerRegistry
{
    private readonly Lock _gate = new();
    private readonly HashSet<PeerHandlers> _live = [];

    /// <summary>Registers fresh handlers for a node's startup; handlers of other startups, even of the same node, are left alone.</summary>
    /// <returns>The handlers the node's startup creates its outbound handlers in.</returns>
    internal PeerHandlers Begin()
    {
        var created = new PeerHandlers();
        lock (_gate)
            _ = _live.Add(created);

        return created;
    }

    /// <summary>Creates the scope that releases one startup's handlers when the node's host is disposed.</summary>
    /// <param name="owner">The handlers the node's startup created.</param>
    /// <returns>A disposable that calls <see cref="Release" /> once.</returns>
    internal IDisposable CreateScope(PeerHandlers owner) => new NodeScope(this, owner);

    /// <summary>Disposes the handlers of every startup that was not released.</summary>
    internal void DisposeAll()
    {
        PeerHandlers[] all;
        lock (_gate)
        {
            all = new PeerHandlers[_live.Count];
            _live.CopyTo(all);
            _live.Clear();
        }

        for (var i = 0; i < all.Length; i++)
            all[i].Dispose();
    }

    /// <summary>Disposes the handlers of one startup and releases their hold on the node's certificate material.</summary>
    /// <param name="owner">The handlers the startup created.</param>
    /// <remarks>Call it after the node's host stopped, so its client pool has already drained its connections. Handlers of any other startup stay usable.</remarks>
    internal void Release(PeerHandlers owner)
    {
        lock (_gate)
            _ = _live.Remove(owner);

        owner.Dispose();
    }

    [Mutable]
    private sealed class NodeScope : IDisposable
    {
        private readonly PeerHandlers _owner;
        private readonly PeerHandlerRegistry _registry;
        private int _released;

        internal NodeScope(PeerHandlerRegistry registry, PeerHandlers owner)
        {
            _registry = registry;
            _owner = owner;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _released, 1) == 0)
                _registry.Release(_owner);
        }
    }
}
