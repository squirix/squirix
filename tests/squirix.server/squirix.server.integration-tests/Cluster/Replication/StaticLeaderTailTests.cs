using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>The followers of a statically led RF=3 group learn the final commit index after writes stop and apply the whole tail.</summary>
public sealed class StaticLeaderTailTests : NodeIntegrationTestBase
{
    private const string CacheName = "static-leader-tail";
    private const string OwnerId = "node-a";
    private const int Writes = 24;

    /// <summary>Bounds the wait; a healthy follower learns the commit index within milliseconds, so the bound only absorbs a loaded machine.</summary>
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    private static readonly string[] FollowerIds = ["node-b", "node-c"];

    /// <summary>After concurrent writes stop, each follower reaches the owner commit index, applies every entry, and holds every key.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FollowersApplyTailAfterWritesStop(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(OwnerId, FollowerIds[0], FollowerIds[1], Options(), cancellationToken);
        var owner = cluster[OwnerId];
        await ReplicaGroupFollowers.AwaitVerifiedAsync(owner, cancellationToken);
        var keys = OwnedKeys(owner);
        var cache = owner.GetCache<object?>(CacheName);
        var writes = new Task[Writes];
        for (var i = 0; i < Writes; i++)
            writes[i] = cache.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, keys[i], new NodeCacheEntry<object?> { Value = $"value-{i}", Version = i + 1 }, cancellationToken).AsTask();

        await Task.WhenAll(writes);
        var target = (await GroupLog(owner).GetStatusAsync(cancellationToken)).CommitIndex;
        foreach (var follower in FollowerIds)
        {
            var host = cluster[follower];
            await AwaitCommittedAndAppliedAsync(host, target, cancellationToken);
            for (var i = 0; i < Writes; i++)
            {
                var entry = await LocalChain(host).GetEntryAsync(CacheName, keys[i], cancellationToken);
                _ = await Assert.That(entry?.Value).IsEqualTo($"value-{i}").Because($"{follower} must hold the committed write {i}.");
            }
        }

        _ = await Assert.That(target >= Writes).IsTrue();
    }

    private static IntegrationStartOptions Options() => new()
    {
        ReplicaCount = 3,
        UsePersistence = true,
        ExtraScope = CacheName,
    };

    private static IFollowerLog GroupLog(ITestNodeHost host) =>
        host.GetRequiredService<ReplicaGroupRegistry>().TryGetLog(OwnerId, out var log) ? log : throw new InvalidOperationException($"The node does not serve group {OwnerId}.");

    private static ILogicalNamespacedCache<object?> LocalChain(ITestNodeHost host) =>
        host.Services.GetRequiredKeyedService<ILogicalNamespacedCache<object?>>(CachePipelineRegistration.LocalChainKey);

    private static string[] OwnedKeys(ITestNodeHost owner)
    {
        var locator = owner.GetRequiredService<INodeLocator>();
        var keys = new string[Writes];
        var found = 0;
        for (var candidate = 0; candidate < 10_000 && found < Writes; candidate++)
        {
            var key = $"{CacheName}-{candidate}";
            if (string.Equals(locator.GetOwner(CacheName, key), OwnerId, StringComparison.Ordinal))
                keys[found++] = key;
        }

        return found == Writes ? keys : throw new InvalidOperationException($"Only {found} keys owned by {OwnerId} were found.");
    }

    private static async Task AwaitCommittedAndAppliedAsync(ITestNodeHost host, ulong index, CancellationToken cancellationToken)
    {
        var applier = host.GetRequiredService<ReplicaGroupAppliers>().For(OwnerId);
        var log = GroupLog(host);
        var started = Stopwatch.GetTimestamp();
        while (true)
        {
            var status = await log.GetStatusAsync(cancellationToken);
            if (status.CommitIndex >= index && applier.AppliedIndex >= index)
                return;

            if (Stopwatch.GetElapsedTime(started) >= Bound)
                throw new TimeoutException($"The follower committed {status.CommitIndex} and applied {applier.AppliedIndex}, not {index}; its log ends at {status.LastLogIndex}.");

            await Task.Delay(TimeSpan.FromMilliseconds(20), TimeProvider.System, cancellationToken);
        }
    }
}
