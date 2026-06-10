using System;
using System.Collections.Generic;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Squirix.Server.TestKit.Cluster;

/// <summary>
/// Key ownership helper mirroring server consistent-hash route behavior for multi-node tests.
/// </summary>
internal sealed class TestKeyOwnerHelper
{
    private readonly (ulong Hash, string Node)[] _ring;

    public TestKeyOwnerHelper(IEnumerable<string> nodeIds, int virtualNodes = 128)
    {
        ArgumentNullException.ThrowIfNull(nodeIds);
        var nodes = new List<string>();
        foreach (var nodeId in nodeIds)
        {
            if (!string.IsNullOrWhiteSpace(nodeId) && !nodes.Exists(existing => string.Equals(existing, nodeId, StringComparison.Ordinal)))
                nodes.Add(nodeId);
        }

        if (nodes.Count == 0)
            throw new ArgumentException("At least one node is required.", nameof(nodeIds));

        var ring = new List<(ulong Hash, string Node)>(nodes.Count * virtualNodes);
        for (var i = 0; i < nodes.Count; i++)
        {
            var node = nodes[i];
            for (var vnode = 0; vnode < virtualNodes; vnode++)
            {
                var key = string.Concat(node, "#", vnode.ToString(CultureInfo.InvariantCulture));
                ring.Add((HashString(key), node));
            }
        }

        ring.Sort(static (a, b) => a.Hash.CompareTo(b.Hash));
        _ring = [.. ring];
    }

    public string FindKeyOwnedBy(string cacheName, string ownerId, string prefix) => FindKeysOwnedBy(cacheName, ownerId, 1, prefix)[0];

    private static ulong HashBytes(ReadOnlySpan<byte> bytes)
    {
        Span<byte> digest = stackalloc byte[32];
        _ = SHA256.HashData(bytes, digest);
        return BitConverter.ToUInt64(digest);
    }

    private static ulong HashCacheRouteKey(string cacheName, string key)
    {
        var canonical = string.IsNullOrWhiteSpace(cacheName) ? "default" : cacheName;
        var routeKey = string.Concat(canonical.Length.ToString(CultureInfo.InvariantCulture), ":", canonical, "\x1F", key);
        return HashString(routeKey);
    }

    private static ulong HashString(string text)
    {
        var bytes = Encoding.UTF8.GetBytes(text);
        return HashBytes(bytes);
    }

    private string[] FindKeysOwnedBy(string cacheName, string ownerId, int count, string prefix, int maxAttempts = 200_000)
    {
        var keys = new List<string>(count);
        for (var i = 0; i < maxAttempts && keys.Count < count; i++)
        {
            var candidate = string.Concat(prefix, ":", i.ToString(CultureInfo.InvariantCulture));
            if (string.Equals(GetOwner(cacheName, candidate), ownerId, StringComparison.Ordinal))
                keys.Add(candidate);
        }

        return keys.Count == count ? [.. keys] : throw new InvalidOperationException($"Unable to find {count} keys owned by '{ownerId}'.");
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
