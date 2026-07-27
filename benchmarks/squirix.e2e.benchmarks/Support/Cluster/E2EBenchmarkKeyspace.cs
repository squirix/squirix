using System;
using System.Buffers;
using System.Collections.Generic;
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

        var owner = KeyOwner.TwoNode;
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
            keys[i] = InvariantIndexStrings.FormatPrefixedPadded(prefix, i, "D6", 6);
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
        internal static readonly KeyOwner TwoNode = new(["nodeA", "nodeB"]);

        private readonly (ulong Hash, string Node)[] _ring;

        private KeyOwner(ReadOnlySpan<string> nodeIds, int virtualNodes = 128)
        {
            var uniqueNodes = new HashSet<string>(StringComparer.Ordinal);
            foreach (var nodeId in nodeIds)
            {
                if (!string.IsNullOrWhiteSpace(nodeId))
                    _ = uniqueNodes.Add(nodeId);
            }

            if (uniqueNodes.Count is 0)
                throw new ArgumentException("At least one node is required.", nameof(nodeIds));

            var nodes = new string[uniqueNodes.Count];
            uniqueNodes.CopyTo(nodes);

            var ring = new (ulong Hash, string Node)[nodes.Length * virtualNodes];
            var writeIndex = 0;
            for (var nodeIndex = 0; nodeIndex < nodes.Length; nodeIndex++)
            {
                var node = nodes[nodeIndex];
                for (var vnode = 0; vnode < virtualNodes; vnode++)
                    ring[writeIndex++] = (HashVNode(node, vnode), node);
            }

            Array.Sort(ring, static (a, b) => a.Hash.CompareTo(b.Hash));
            _ring = ring;
        }

        internal string[] FindKeysOwnedBy(string cacheName, string ownerId, int count, string prefix)
        {
            var keys = new string[count];
            var found = 0;
            for (var i = 0; i < 200_000 && found < count; i++)
            {
                var candidate = InvariantIndexStrings.FormatPrefixed(prefix, i);
                if (string.Equals(GetOwner(cacheName, candidate), ownerId, StringComparison.Ordinal))
                    keys[found++] = candidate;
            }

            return found == count ? keys : throw new InvalidOperationException("Unable to find enough benchmark keys owned by the requested node.");
        }

        private static int CountDigits(int value)
        {
            var digits = 1;
            while (value >= 10)
            {
                value /= 10;
                digits++;
            }

            return digits;
        }

        private static ulong HashBytes(ReadOnlySpan<byte> bytes)
        {
            Span<byte> digest = stackalloc byte[32];
            _ = SHA256.HashData(bytes, digest);
            return BitConverter.ToUInt64(digest);
        }

        private static ulong HashCacheRouteKey(string cacheName, string key)
        {
            var canonical = string.IsNullOrWhiteSpace(cacheName) ? "default" : cacheName;
            var byteCount = checked(CountDigits(canonical.Length) + 1 + Encoding.UTF8.GetByteCount(canonical) + 1 + Encoding.UTF8.GetByteCount(key));
            if (byteCount <= 512)
            {
                Span<byte> buffer = stackalloc byte[byteCount];
                WriteRouteKey(canonical, key, buffer);
                return HashBytes(buffer);
            }

            var rented = ArrayPool<byte>.Shared.Rent(byteCount);
            try
            {
                var buffer = rented.AsSpan(0, byteCount);
                WriteRouteKey(canonical, key, buffer);
                return HashBytes(buffer);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        private static ulong HashVNode(string node, int index)
        {
            var byteCount = checked(Encoding.UTF8.GetByteCount(node) + 1 + CountDigits(index));
            if (byteCount <= 512)
            {
                Span<byte> buffer = stackalloc byte[byteCount];
                return HashBytes(WriteVNodeKey(node, index, buffer));
            }

            var rented = ArrayPool<byte>.Shared.Rent(byteCount);
            try
            {
                return HashBytes(WriteVNodeKey(node, index, rented.AsSpan(0, byteCount)));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        private static int WriteNonNegativeIntUtf8(int value, Span<byte> destination)
        {
            var digitsUtf8 = "0123456789"u8;
            var digits = CountDigits(value);
            for (var i = digits - 1; i >= 0; i--)
            {
                destination[i] = digitsUtf8[value % 10];
                value /= 10;
            }

            return digits;
        }

        private static void WriteRouteKey(string canonical, string key, Span<byte> buffer)
        {
            const byte colon = 58;
            const byte unitSeparator = 0x1F;
            var written = WriteNonNegativeIntUtf8(canonical.Length, buffer);
            buffer[written++] = colon;
            written += Encoding.UTF8.GetBytes(canonical, buffer[written..]);
            buffer[written++] = unitSeparator;
            _ = Encoding.UTF8.GetBytes(key, buffer[written..]);
        }

        private static ReadOnlySpan<byte> WriteVNodeKey(string node, int index, Span<byte> buffer)
        {
            const byte hash = 35;
            var written = Encoding.UTF8.GetBytes(node, buffer);
            buffer[written++] = hash;
            written += WriteNonNegativeIntUtf8(index, buffer[written..]);
            return buffer[..written];
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
