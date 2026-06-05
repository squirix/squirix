using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using Squirix.Server.Utils;

namespace Squirix.Server.Cluster;

/// <summary>
/// Immutable consistent hashing ring with virtual nodes (vnodes).
/// </summary>
internal sealed class ConsistentHashRing : INodeLocator
{
    private readonly IHash _hash;
    private readonly ImmutableArray<(ulong Hash, string Node)> _ring;

    public ConsistentHashRing(IEnumerable<string> nodes, int virtualNodes = 128, IHash? hash = null)
    {
        ArgumentNullException.ThrowIfNull(nodes);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(virtualNodes);

        _hash = hash ?? new Sha256Hasher();

        var distinct = EnumerableHelper.GetDistinct(nodes);
        if (distinct.Length == 0)
            throw new ArgumentException("At least one node must be provided.", nameof(nodes));

        var list = new List<(ulong Hash, string Node)>(checked(distinct.Length * virtualNodes));
        foreach (var node in distinct)
        {
            for (var i = 0; i < virtualNodes; i++)
            {
                var key = $"{node}#{i}";
                var h = _hash.HashString(key);
                list.Add((h, node));
            }
        }

        list.Sort(static (a, b) => a.Hash.CompareTo(b.Hash));
        _ring = [.. list];
    }

    public string GetOwner(string routeKey)
    {
        if (string.IsNullOrWhiteSpace(routeKey))
            throw new ArgumentException("Route key cannot be null or empty.", nameof(routeKey));

        var kh = _hash.HashString(routeKey);
        var idx = FindFirstGreaterOrEqual(kh);
        return _ring[idx].Node;
    }

    public string GetOwner(string cacheName, string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(cacheName);
        ArgumentNullException.ThrowIfNull(key);

        var kh = _hash.HashCacheRouteKey(cacheName, key);
        var idx = FindFirstGreaterOrEqual(kh);
        return _ring[idx].Node;
    }

    private int FindFirstGreaterOrEqual(ulong hash)
    {
        int lo = 0, hi = _ring.Length - 1;
        while (lo <= hi)
        {
            var mid = (lo + hi) >> 1;
            var midHash = _ring[mid].Hash;
            if (midHash < hash)
                lo = mid + 1;
            else
                hi = mid - 1;
        }

        return lo == _ring.Length ? 0 : lo;
    }
}
