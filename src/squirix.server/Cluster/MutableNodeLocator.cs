using System;
using System.Collections.Generic;

namespace Squirix.Server.Cluster;

/// <summary>
/// Thread-safe, updatable node locator that wraps an immutable ConsistentHashRing.
/// </summary>
internal sealed class MutableNodeLocator : INodeLocator
{
    private volatile ConsistentHashRing _ring;

    public MutableNodeLocator(IEnumerable<string> nodes, int virtualNodes = 128)
    {
        ArgumentNullException.ThrowIfNull(nodes);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(virtualNodes);

        _ring = new ConsistentHashRing(nodes, virtualNodes);
    }

    public string GetOwner(string cacheName, string key) => _ring.GetOwner(cacheName, key);
}
