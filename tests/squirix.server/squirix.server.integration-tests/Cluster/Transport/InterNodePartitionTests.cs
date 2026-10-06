using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Transport;

/// <summary>A node cut off at the socket level stops serving internode traffic, the majority keeps writing, and healing the links restores the traffic.</summary>
public sealed class InterNodePartitionTests : NodeIntegrationTestBase
{
    private const string CacheName = "internode-partition";

    /// <summary>Isolating a node resets its internode connections and refuses new ones until the fabric heals, while the remaining majority still commits.</summary>
    /// <param name="cancellationToken">The test cancellation token; bounds the waits only.</param>
    [Test]
    public async Task IsolatedNodeStopsAndResumesTraffic(CancellationToken cancellationToken)
    {
        await using var fabric = new PartitionFabric();
        var options = new IntegrationStartOptions { ReplicaCount = 3, UsePersistence = true, PartitionFabric = fabric, ExtraScope = CacheName };
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", options, cancellationToken);
        var nodeA = cluster["node-a"];
        await ReplicaGroupFollowers.AwaitVerifiedAsync(nodeA, cancellationToken);
        await ReplicaGroupFollowers.AwaitVerifiedAsync(cluster["node-c"], cancellationToken);

        var cache = nodeA.GetCache<object?>(CacheName);
        var ownedKey = nodeA.FindKeyOwnedBy(CacheName, "node-a");
        await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, ownedKey, new NodeCacheEntry<object?> { Value = "v" }, cancellationToken);

        var toC = fabric["node-a", "node-c"];
        var fromC = fabric["node-c", "node-a"];
        var client = nodeA.GetRequiredService<IServerClientPool>().ForNode("node-c");
        var request = new GetValueAsyncRequest { CacheName = CacheName, Key = nodeA.FindKeyOwnedBy(CacheName, "node-c") };
        _ = await client.GetValueAsync(request, new CallOptions(cancellationToken: cancellationToken));
        _ = await Assert.That(toC.BytesForwarded(ProxyDirection.ClientToUpstream)).IsGreaterThan(0);

        await fabric.IsolateAsync("node-c");

        _ = await Assert.That(toC.ActiveConnections).IsEqualTo(0);
        _ = await Assert.That(fromC.ActiveConnections).IsEqualTo(0);

        // A pooled connection that was reset fails its request without a new dial; the next request dials the proxy and is reset there.
        var refusedBefore = toC.RefusedConnections;
        await ExpectUnavailableAsync(client, request, cancellationToken);
        while (toC.RefusedConnections == refusedBefore)
            await ExpectUnavailableAsync(client, request, cancellationToken);

        await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, ownedKey, new NodeCacheEntry<object?> { Value = "majority" }, cancellationToken);
        var majorityRead = await cache.GetValueAsync(CacheName, ownedKey, cancellationToken);
        _ = await Assert.That(majorityRead.Found).IsTrue();

        var acceptedBefore = toC.AcceptedConnections;
        var forwardedBefore = toC.BytesForwarded(ProxyDirection.ClientToUpstream);
        fabric.HealAll();
        _ = await client.GetValueAsync(request, new CallOptions(cancellationToken: cancellationToken));

        _ = await Assert.That(toC.AcceptedConnections).IsGreaterThan(acceptedBefore);
        _ = await Assert.That(toC.BytesForwarded(ProxyDirection.ClientToUpstream)).IsGreaterThan(forwardedBefore);
    }

    private static async Task ExpectUnavailableAsync(SquirixCacheService.SquirixCacheServiceClient client, GetValueAsyncRequest request, CancellationToken cancellationToken)
    {
        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(client.GetValueAsync(request, new CallOptions(cancellationToken: cancellationToken)).ResponseAsync);
        _ = await Assert.That(refused.StatusCode).IsEqualTo(StatusCode.Unavailable);
    }
}
