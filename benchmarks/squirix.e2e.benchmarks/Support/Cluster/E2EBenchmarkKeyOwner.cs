using System;
using System.Collections.Generic;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Squirix.E2EBenchmarks.Support.Cluster;

/// <summary>Mirrors the Squirix consistent-hash owner selection for benchmark setup.</summary>
internal sealed class E2EBenchmarkKeyOwner
{
    private readonly (ulong Hash, string Node)[] _ring;

    internal E2EBenchmarkKeyOwner(HashSet<string> uniqueNodes, int virtualNodes = 128)
    {
        if (uniqueNodes.Count is 0)
            throw new ArgumentException("At least one node is required.", nameof(uniqueNodes));

        var nodes = new List<string>(uniqueNodes);

        var ring = new List<(ulong Hash, string Node)>(nodes.Count * virtualNodes);
        for (var nodeIndex = 0; nodeIndex < nodes.Count; nodeIndex++)
        {
            var node = nodes[nodeIndex];
            for (var vnode = 0; vnode < virtualNodes; vnode++)
                ring.Add((HashString($"{node}#{vnode.ToString(CultureInfo.InvariantCulture)}"), node));
        }

        ring.Sort(static (a, b) => a.Hash.CompareTo(b.Hash));
        _ring = [.. ring];
    }

    internal string[] FindKeysOwnedBy(string cacheName, string ownerId, int count, string prefix)
    {
        var keys = new List<string>(count);
        for (var i = 0; i < 200_000 && keys.Count < count; i++)
        {
            var candidate = $"{prefix}:{i.ToString(CultureInfo.InvariantCulture)}";
            if (string.Equals(GetOwner(cacheName, candidate), ownerId, StringComparison.Ordinal))
                keys.Add(candidate);
        }

        return keys.Count == count ? [.. keys] : throw new InvalidOperationException($"Unable to find {count.ToString(CultureInfo.InvariantCulture)} benchmark keys owned by {ownerId}.");
    }

    private static ulong HashCacheRouteKey(string cacheName, string key)
    {
        var canonical = string.IsNullOrWhiteSpace(cacheName) ? "default" : cacheName;
        return HashString($"{canonical.Length.ToString(CultureInfo.InvariantCulture)}:{canonical}\x1F{key}");
    }

    private static ulong HashString(string text)
    {
        var bytes = Encoding.UTF8.GetBytes(text);
        Span<byte> digest = stackalloc byte[32];
        _ = SHA256.HashData(bytes, digest);
        return BitConverter.ToUInt64(digest);
    }

    private int FindFirstGreaterOrEqual(ulong hash)
    {
        var lo = 0;
        var hi = _ring.Length - 1;
        while (lo <= hi)
        {
            var mid = (lo + hi) >> 1;
            if (_ring[mid].Hash < hash)
                lo = mid + 1;
            else
                hi = mid - 1;
        }

        return lo == _ring.Length ? 0 : lo;
    }

    private string GetOwner(string cacheName, string key)
    {
        var hash = HashCacheRouteKey(cacheName, key);
        var idx = FindFirstGreaterOrEqual(hash);
        return _ring[idx].Node;
    }
}
