using System;
using System.Collections.Generic;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Squirix.E2EBenchmarks.Infrastructure;

/// <summary>
/// Mirrors the Squirix consistent-hash owner selection for benchmark setup.
/// </summary>
internal sealed class E2EBenchmarkKeyOwner
{
    private readonly (ulong Hash, string Node)[] _ring;

    internal E2EBenchmarkKeyOwner(IEnumerable<string> nodeIds, int virtualNodes = 128)
    {
        var nodes = new List<string>();
        foreach (var nodeId in nodeIds)
        {
            if (!string.IsNullOrWhiteSpace(nodeId) && !nodes.Exists(existing => string.Equals(existing, nodeId, StringComparison.Ordinal)))
                nodes.Add(nodeId);
        }

        var ring = new List<(ulong Hash, string Node)>(nodes.Count * virtualNodes);
        foreach (var node in nodes)
        {
            for (var vnode = 0; vnode < virtualNodes; vnode++)
                ring.Add((HashString(string.Concat(node, "#", vnode.ToString(CultureInfo.InvariantCulture))), node));
        }

        ring.Sort(static (a, b) => a.Hash.CompareTo(b.Hash));
        _ring = [.. ring];
    }

    internal string[] FindKeysOwnedBy(string cacheName, string ownerId, int count, string prefix)
    {
        var keys = new List<string>(count);
        for (var i = 0; i < 200_000 && keys.Count < count; i++)
        {
            var candidate = string.Concat(prefix, ":", i.ToString(CultureInfo.InvariantCulture));
            if (string.Equals(GetOwner(cacheName, candidate), ownerId, StringComparison.Ordinal))
                keys.Add(candidate);
        }

        return keys.Count == count ? [.. keys] : throw new InvalidOperationException($"Unable to find {count} benchmark keys owned by {ownerId}.");
    }

    private static ulong HashCacheRouteKey(string cacheName, string key)
    {
        var canonical = string.IsNullOrWhiteSpace(cacheName) ? "default" : cacheName;
        return HashString(string.Concat(canonical.Length.ToString(CultureInfo.InvariantCulture), ":", canonical, "\x1F", key));
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
