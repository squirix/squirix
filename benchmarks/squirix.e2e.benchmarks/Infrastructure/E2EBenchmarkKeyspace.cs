using System.Globalization;
using System.Linq;
using Squirix.E2EBenchmarks.Scenarios;

namespace Squirix.E2EBenchmarks.Infrastructure;

/// <summary>
/// Precomputed benchmark keyspace for hits, misses, unique writes, and owner-aware routing.
/// </summary>
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
        if (topology == BenchmarkTopology.SingleNode)
            return CreateSequential("single", LargeKeyCount, HotKeyCount);

        var owner = new E2EBenchmarkKeyOwner(["nodeA", "nodeB"]);
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
            keys[i] = string.Concat(prefix, ":", i.ToString("D6", CultureInfo.InvariantCulture));
        return keys;
    }

    private static E2EBenchmarkKeyspace CreateOwned(string cacheName, E2EBenchmarkKeyOwner owner, string nodeId, string prefix)
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

    private static E2EBenchmarkKeyspace CreateUniform(string cacheName, E2EBenchmarkKeyOwner owner, string prefix, int count)
    {
        var nodeA = owner.FindKeysOwnedBy(cacheName, "nodeA", count / 2, $"{prefix}:a");
        var nodeB = owner.FindKeysOwnedBy(cacheName, "nodeB", count / 2, $"{prefix}:b");
        var hit = Interleave(nodeA, nodeB);
        var miss = CreateKeys($"{prefix}:miss", count);
        var add = CreateKeys($"{prefix}:add", count);
        var hot = hit.Take(HotKeyCount).ToArray();
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
}
