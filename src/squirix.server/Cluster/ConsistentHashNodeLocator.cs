using System;
using System.Collections.Generic;

namespace Squirix.Server.Cluster;

/// <summary>
/// Node locator backed by a <see cref="ConsistentHashRing" /> built from the cluster peer list at startup.
/// </summary>
internal sealed class ConsistentHashNodeLocator : INodeLocator
{
    private readonly ConsistentHashRing _ring;

    public ConsistentHashNodeLocator(IEnumerable<string> nodes, int virtualNodes = 128)
    {
        ArgumentNullException.ThrowIfNull(nodes);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(virtualNodes);

        _ring = new ConsistentHashRing(nodes, virtualNodes);
    }

    public string GetOwner(string cacheName, string key) => _ring.GetOwner(cacheName, key);
}
