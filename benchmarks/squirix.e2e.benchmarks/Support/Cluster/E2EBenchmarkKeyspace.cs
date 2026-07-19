using System;
using System.Collections.Generic;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Squirix.E2EBenchmarks.Scenarios;
using Squirix.Server.TestKit;

namespace Squirix.E2EBenchmarks.Support.Cluster;

/// <summary>Precomputed benchmark keyspace for hits, misses, unique writes, and owner-aware routing.</summary>
internal sealed class E2EBenchmarkKeyspace
{
    private const int HotKeyCount = 16;
    private const int LargeKeyCount = 2_048;

    private E2EBenchmarkKeyspace(string[] hitKeys, string[] missKeys, string[] addKeys, string[] hotKeys, string[] expiringHitKeys)
    {
        HitKeys = hitKeys;
        MissKeys = missKeys;
        AddKeys = addKeys;
        HotKeys = hotKeys;
        ExpiringHitKeys = expiringHitKeys;
    }

    internal string[] ExpiringHitKeys { get; }

    internal string[] HitKeys { get; }

    private string[] AddKeys { get; }

    private string[] HotKeys { get; }

    private string[] MissKeys { get; }

    internal static E2EBenchmarkKeyspace Create(string cacheName, BenchmarkTopology topology)
    {
        if (topology is BenchmarkTopology.SingleNode)
            return CreateSequential("single", LargeKeyCount, HotKeyCount);

        var owner = new KeyOwner(["nodeA", "nodeB"]);
        return topology switch
        {
            BenchmarkTopology.TwoNodeLocalOwner => CreateOwned(cacheName, owner, "nodeA", "local"),
            BenchmarkTopology.TwoNodeRemoteOwner => CreateOwned(cacheName, owner, "nodeB", "remote"),
            BenchmarkTopology.TwoNodeHotKeys => CreateUniform(cacheName, owner, "hot", HotKeyCount),
            _ => CreateUniform(cacheName, owner, "uniform", LargeKeyCount),
        };
    }

    internal string AddKey(int index) => AddKeys[index % AddKeys.Length];

    internal string ExpiringHitKey(int index) => ExpiringHitKeys[index % ExpiringHitKeys.Length];

    internal string HitKey(int index) => HitKeys[index % HitKeys.Length];

    internal string HotKey(int index) => HotKeys[index % HotKeys.Length];

    internal string MissKey(int index) => MissKeys[index % MissKeys.Length];

    private static string[] CreateKeys(string prefix, int count)
    {
        var keys = new string[count];
        for (var i = 0; i < keys.Length; i++)
            keys[i] = $"{prefix}:{i.ToString("D6", CultureInfo.InvariantCulture)}";
        return keys;
    }

    private static E2EBenchmarkKeyspace CreateOwned(string cacheName, KeyOwner owner, string nodeId, string prefix)
    {
        var hit = owner.FindKeysOwnedBy(cacheName, nodeId, LargeKeyCount, $"{prefix}:hit");
        var miss = owner.FindKeysOwnedBy(cacheName, nodeId, LargeKeyCount, $"{prefix}:miss");
        var add = owner.FindKeysOwnedBy(cacheName, nodeId, LargeKeyCount, $"{prefix}:add");
        var hot = owner.FindKeysOwnedBy(cacheName, nodeId, HotKeyCount, $"{prefix}:hot");
        var expiring = owner.FindKeysOwnedBy(cacheName, nodeId, LargeKeyCount, $"{prefix}:expiring");
        return new E2EBenchmarkKeyspace(hit, miss, add, hot, expiring);
    }

    private static E2EBenchmarkKeyspace CreateSequential(string prefix, int count, int hotCount)
    {
        var hit = CreateKeys($"{prefix}:hit", count);
        var miss = CreateKeys($"{prefix}:miss", count);
        var add = CreateKeys($"{prefix}:add", count);
        var hot = CreateKeys($"{prefix}:hot", hotCount);
        var expiring = CreateKeys($"{prefix}:expiring", count);
        return new E2EBenchmarkKeyspace(hit, miss, add, hot, expiring);
    }

    private static E2EBenchmarkKeyspace CreateUniform(string cacheName, KeyOwner owner, string prefix, int count)
    {
        var nodeA = owner.FindKeysOwnedBy(cacheName, "nodeA", count / 2, $"{prefix}:a");
        var nodeB = owner.FindKeysOwnedBy(cacheName, "nodeB", count / 2, $"{prefix}:b");
        var hit = Interleave(nodeA, nodeB);
        var miss = CreateKeys($"{prefix}:miss", count);
        var add = CreateKeys($"{prefix}:add", count);
        var hot = new string[HotKeyCount];
        Array.Copy(hit, hot, HotKeyCount);
        var expiring = CreateKeys($"{prefix}:expiring", count);
        return new E2EBenchmarkKeyspace(hit, miss, add, hot, expiring);
    }

    private static string[] Interleave(string[] left, string[] right)
    {
        var result = new string[left.Length + right.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i * 2] = left[i];
            result[(i * 2) + 1] = right[i];
        }

        return result;
    }

    /// <summary>Mirrors the Squirix consistent-hash owner selection for benchmark setup.</summary>
    private sealed class KeyOwner
    {
        private readonly (ulong Hash, string Node)[] _ring;

        internal KeyOwner(HashSet<string> uniqueNodes, int virtualNodes = 128)
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
}
