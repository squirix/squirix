using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Runtime.InteropServices;

namespace Squirix.Server.Cluster;

/// <summary>Immutable consistent hashing ring with virtual nodes (vnodes).</summary>
internal sealed class ConsistentHashRing : INodeLocator
{
    private readonly IHash _hash;
    private readonly ImmutableArray<(ulong Hash, string Node)> _ring;

    public ConsistentHashRing(ReadOnlySpan<string> nodes, int virtualNodes = 128, IHash? hash = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(virtualNodes);

        _hash = hash ?? new Sha256Hasher();

        var distinct = CollectDistinctNodes(nodes);
        if (distinct.Length is 0)
            throw new ArgumentException("At least one node must be provided.", nameof(nodes));

        var ringSize = checked(distinct.Length * virtualNodes);
        var ring = new (ulong Hash, string Node)[ringSize];
        var index = 0;
        for (var nodeIndex = 0; nodeIndex < distinct.Length; nodeIndex++)
        {
            var node = distinct[nodeIndex];
            for (var i = 0; i < virtualNodes; i++)
                ring[index++] = (_hash.HashVNode(node, i), node);
        }

        Array.Sort(ring, static (a, b) => a.Hash.CompareTo(b.Hash));
        _ring = ImmutableCollectionsMarshal.AsImmutableArray(ring);
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

    private static string[] CollectDistinctNodes(ReadOnlySpan<string> nodes)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        string[] buffer = [];
        var writeIndex = 0;

        for (var i = 0; i < nodes.Length; i++)
        {
            var value = nodes[i];
            if (string.IsNullOrWhiteSpace(value))
                continue;

            if (!seen.Add(value))
                continue;

            if (writeIndex == buffer.Length)
            {
                var nextLength = buffer.Length is 0 ? 4 : buffer.Length * 2;
                var grown = new string[nextLength];
                buffer.AsSpan(0, writeIndex).CopyTo(grown);
                buffer = grown;
            }

            buffer[writeIndex++] = value;
        }

        if (writeIndex is 0)
            return [];

        if (writeIndex == buffer.Length)
            return buffer;

        var result = new string[writeIndex];
        buffer.AsSpan(0, writeIndex).CopyTo(result);
        return result;
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
