using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>
/// On an RF=3 cluster of three nodes, every node is a member of every group, so one node at critical memory pressure makes the leader of
/// each group refuse the writes that grow memory, while removals pass and growth resumes once the node is below critical.
/// </summary>
public sealed class ReplicaMemoryPressureTests : NodeIntegrationTestBase
{
    private const string CacheName = "replica-pressure";
    private const int LargeValueChars = 70 * 1024;

    /// <summary>The most growing writes tried before the leader is expected to have learned of the pressure; each one waits for a reply of the pressured node.</summary>
    private const int MaxGrowingAttempts = 20;

    private const string PressuredNode = "node-c";

    /// <summary>The limit of the pressured node, which one large entry exceeds.</summary>
    private const long PressuredLimitBytes = 64 * 1024;

    /// <summary>
    /// A follower at critical memory pressure makes the leader refuse growing writes for its group with RESOURCE_EXHAUSTED, without storing
    /// the refused key; a removal passes, and once the removal frees the follower, a growing write is accepted again.
    /// </summary>
    /// <param name="owner">The node that leads the group the writes go to.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments("node-a")]
    [Arguments("node-b")]
    public async Task FollowerPressureRefusesGrowthUntilFreed(string owner, CancellationToken cancellationToken)
    {
        var options = new IntegrationStartOptions
        {
            ReplicaCount = 3,
            UsePersistence = true,
            CleanTestDir = true,
            ExtraScope = "replica-memory-pressure",
            MemoryPressureByNode = new Dictionary<string, PressureOptions>(StringComparer.Ordinal) { [PressuredNode] = new() { MaxEstimatedCacheBytes = PressuredLimitBytes } },
        };
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", options, cancellationToken);
        var leader = cluster[owner];
        var pressured = cluster[PressuredNode];
        await ReplicaGroupFollowers.AwaitVerifiedAsync(leader, cancellationToken);
        var committer = ReplicaNodeCommitters.OwnCommitter(leader);
        var reader = pressured.GetRequiredService<ICriticalMemoryPressureReader>();
        var cache = leader.GetCache<object?>(CacheName);
        var keys = OwnedKeys(leader, owner, MaxGrowingAttempts + 3);

        // A static leader sends the commit index of an entry with the next append only, so the pressured node applies the large entry, and
        // becomes critical, once a second write reaches it.
        await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, keys[0], new NodeCacheEntry<object?> { Value = new string('x', LargeValueChars), Version = 1 }, cancellationToken);
        await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, keys[1], new NodeCacheEntry<object?> { Value = "carry", Version = 1 }, cancellationToken);
        await reader.WaitUntilAsync(static r => r.IsCritical(), cancellationToken);
        var refusedKey = await GrowUntilRefusedAsync(cache, committer, keys, cancellationToken);
        var stored = refusedKey == null ? default : await cache.GetValueAsync(CacheName, refusedKey, cancellationToken);
        var removed = await cache.RemoveAsync(Guid.NewGuid().ToString("N"), CacheName, keys[0], cancellationToken);
        var resumed = await GrowUntilAcceptedAsync(cache, committer, keys, cancellationToken);
        await reader.WaitUntilAsync(static r => !r.IsCritical(), cancellationToken);

        _ = await Assert.That(refusedKey).IsNotNull().Because("The leader must refuse a growing write once the pressured follower replied.");
        _ = await Assert.That(stored.Found).IsFalse().Because("A refused write appends nothing.");
        _ = await Assert.That(removed.Removed).IsTrue();
        _ = await Assert.That(resumed).IsTrue().Because("Growth resumes once the follower is below critical and its report lapsed or cleared.");
    }

    private static string[] OwnedKeys(ITestNodeHost host, string owner, int count)
    {
        var locator = host.GetRequiredService<INodeLocator>();
        var keys = new string[count];
        var found = 0;
        for (var i = 0; found < count && i < 100_000; i++)
        {
            var candidate = $"{CacheName}-{i}";
            if (string.Equals(locator.GetOwner(CacheName, candidate), owner, StringComparison.Ordinal))
                keys[found++] = candidate;
        }

        return found == count ? keys : throw new InvalidOperationException($"Only {found} keys owned by '{owner}' were found.");
    }

    /// <summary>Writes growing entries, each after the leader heard from the pressured node, until one is refused.</summary>
    /// <param name="cache">The cache of the leader.</param>
    /// <param name="committer">The committer of the group the keys belong to.</param>
    /// <param name="keys">The keys to write, the first two of which are skipped.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The refused key, or <see langword="null" /> when every write was accepted.</returns>
    private static async Task<string?> GrowUntilRefusedAsync(ILogicalNamespacedCache<object?> cache, ReplicaGroupCommitter committer, string[] keys, CancellationToken cancellationToken)
    {
        for (var i = 2; i < keys.Length; i++)
        {
            try
            {
                await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, keys[i], new NodeCacheEntry<object?> { Value = "grow", Version = 1 }, cancellationToken);
            }
            catch (ResourceExhaustedException)
            {
                return keys[i];
            }

            // The reply of the pressured node to this write is what the next write is judged by: wait for the leader to hold it.
            await committer.WaitUntilAsync(static c => c.MemberPressure.IsAnyFresh(), cancellationToken);
        }

        return null;
    }

    /// <summary>Writes growing entries until one is accepted, waiting for the report of the pressured node to clear or lapse after each refusal.</summary>
    /// <param name="cache">The cache of the leader.</param>
    /// <param name="committer">The committer of the group the keys belong to.</param>
    /// <param name="keys">The keys to write.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns><see langword="true" /> when a growing write was accepted.</returns>
    private static async Task<bool> GrowUntilAcceptedAsync(ILogicalNamespacedCache<object?> cache, ReplicaGroupCommitter committer, string[] keys, CancellationToken cancellationToken)
    {
        for (var attempt = 0; attempt < MaxGrowingAttempts; attempt++)
        {
            try
            {
                await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, keys[^1], new NodeCacheEntry<object?> { Value = "resumed", Version = 1 }, cancellationToken);
                return true;
            }
            catch (ResourceExhaustedException)
            {
                await committer.WaitUntilAsync(static c => !c.MemberPressure.IsAnyFresh(), cancellationToken);
            }
        }

        return false;
    }
}
