using System;
using System.Collections.Generic;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.TestKit.Mtls;

/// <summary>The outbound socket options of the node startups made from one <see cref="ClusterIdentity" />, from a startup until its node's host is disposed.</summary>
[Mutable]
internal sealed class PeerOptionsRegistry
{
    private readonly Lock _gate = new();
    private readonly HashSet<PeerOptionsLifetime> _live = [];

    /// <summary>Registers a fresh options lifetime for a node's startup; options of other startups, even of the same node, are left alone.</summary>
    /// <returns>The lifetime of the node's outbound socket options.</returns>
    internal PeerOptionsLifetime Begin()
    {
        var created = new PeerOptionsLifetime();
        lock (_gate)
            _ = _live.Add(created);

        return created;
    }

    /// <summary>Creates the scope that releases one startup's options lifetime when the node's host is disposed.</summary>
    /// <param name="owner">The startup's options lifetime.</param>
    /// <returns>A disposable that calls <see cref="Release" /> once.</returns>
    internal IDisposable CreateScope(PeerOptionsLifetime owner) => new NodeScope(this, owner);

    /// <summary>Disposes the options lifetime of every startup that was not released.</summary>
    internal void DisposeAll()
    {
        PeerOptionsLifetime[] all;
        lock (_gate)
        {
            all = new PeerOptionsLifetime[_live.Count];
            _live.CopyTo(all);
            _live.Clear();
        }

        for (var i = 0; i < all.Length; i++)
            all[i].Dispose();
    }

    /// <summary>Disposes the options lifetime of one startup and releases its hold on the node's certificate material.</summary>
    /// <param name="owner">The startup's options lifetime.</param>
    /// <remarks>Call it after the node's host stopped, so its client pool has already drained its connections. The other startup lifetimes stay usable.</remarks>
    internal void Release(PeerOptionsLifetime owner)
    {
        lock (_gate)
            _ = _live.Remove(owner);

        owner.Dispose();
    }

    [Mutable]
    private sealed class NodeScope : IDisposable
    {
        private readonly PeerOptionsLifetime _owner;
        private readonly PeerOptionsRegistry _registry;
        private int _released;

        internal NodeScope(PeerOptionsRegistry registry, PeerOptionsLifetime owner)
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
