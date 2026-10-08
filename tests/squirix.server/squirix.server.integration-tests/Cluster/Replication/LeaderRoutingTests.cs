using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>An entry node routes a client write to the leader of the key's group, never to a stopped leader, and ends it within one deadline.</summary>
public sealed class LeaderRoutingTests : NodeIntegrationTestBase
{
    private const string CacheName = "default";
    private const string OwnerId = "node-a";

    /// <summary>Bounds every wait for an election and every client call; the timeouts below elect within seconds.</summary>
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(90);

    private static readonly string[] Nodes = [OwnerId, "node-b", "node-c"];

    /// <summary>
    /// Once the leader of the owner group stops and another node leads it, a write sent to the third node goes to the new leader and
    /// succeeds there, whether or not that leader owns the key on the ring.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EntryRoutesWriteToElectedLeader(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(Nodes[0], Nodes[1], Nodes[2], Options("leader-routing", true), cancellationToken);
        var first = await LeaderAsync(cluster, Nodes, cancellationToken);
        await cluster.StopNodeAsync(first);
        var others = Array.FindAll(Nodes, id => !string.Equals(id, first, StringComparison.Ordinal));
        var second = await LeaderAsync(cluster, others, cancellationToken);
        var entry = Array.Find(others, id => !string.Equals(id, second, StringComparison.Ordinal))!;
        await cluster.WaitUntilAsync(
            nodes => Table(nodes[entry]).TryGetLeader(OwnerId, out var route) && string.Equals(route.NodeId, second, StringComparison.Ordinal),
            Bound,
            cancellationToken);

        var key = KeyOwnedByOwner(cluster[entry]);
        using var channel = CreateGrpcChannel(cluster[entry].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var request = new SetEntryAsyncRequest
        {
            OperationId = RpcOperationIdentity.New(),
            CacheName = CacheName,
            Key = key,
            Entry = new NodeCacheEntry<object?> { Value = "routed", Version = 1 }.MapToProto(),
        };

        var refusal = await SetAsync(client, request, cancellationToken);

        _ = await Assert.That(refusal).IsNull().Because($"the write must reach the elected leader {second}, not fail with '{refusal?.Status.Detail}'");
        var read = await client.GetValueAsync(new GetValueAsyncRequest { CacheName = CacheName, Key = key }, deadline: DateTime.UtcNow.Add(Bound), cancellationToken: cancellationToken);
        _ = await Assert.That(read.Found).IsTrue();
    }

    /// <summary>
    /// An entry node whose table still names a follower as the leader forwards the write there; the follower refuses it as stale-owner naming
    /// the leader, and the entry node reroutes once to that leader, with the same operation id, where the write commits.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaleRouteReroutesOnceToLeader(CancellationToken cancellationToken)
    {
        var probe = new LeaderRouteProbe();
        await using var cluster = await StartClusterAsync(Nodes[0], Nodes[1], Nodes[2], Options("leader-reroute", true, probe.Register), cancellationToken);
        var leader = await LeaderAsync(cluster, Nodes, cancellationToken);
        var others = Array.FindAll(Nodes, id => !string.Equals(id, leader, StringComparison.Ordinal));
        var (follower, entry) = (others[0], others[1]);
        var term = 0UL;
        await cluster.WaitUntilAsync(
            nodes => Follows(nodes[follower], leader, out term) && Follows(nodes[entry], leader, out _),
            Bound,
            cancellationToken);
        probe.Arm(entry, OwnerId, new LeaderRoute(follower, term));

        var key = KeyOwnedByOwner(cluster[entry]);
        using var channel = CreateGrpcChannel(cluster[entry].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var request = new SetEntryAsyncRequest
        {
            OperationId = RpcOperationIdentity.New(),
            CacheName = CacheName,
            Key = key,
            Entry = new NodeCacheEntry<object?> { Value = "rerouted", Version = 1 }.MapToProto(),
        };

        var refusal = await SetAsync(client, request, cancellationToken);

        _ = await Assert.That(refusal).IsNull().Because($"the reroute must reach the leader {leader}, not fail with '{refusal?.Status.Detail}'");
        _ = await Assert.That(probe.Refuted).IsTrue();
        var forwards = probe.Forwards();
        _ = await Assert.That(forwards.Length).IsEqualTo(2);
        _ = await Assert.That(forwards[0]).IsEqualTo((follower, request.OperationId));
        _ = await Assert.That(forwards[1]).IsEqualTo((leader, request.OperationId));
    }

    /// <summary>Without automatic failover a write sent to another node is forwarded to the ring owner, as before leader routing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FlagOffRoutesToStaticOwner(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(Nodes[0], Nodes[1], Nodes[2], Options("leader-routing-off", false), cancellationToken);
        var entry = cluster[Nodes[1]];
        _ = await Assert.That(Table(entry)).IsTypeOf<StaticLeaderTable>();

        var key = KeyOwnedByOwner(entry);
        using var channel = CreateGrpcChannel(entry.Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var request = new SetEntryAsyncRequest
        {
            OperationId = RpcOperationIdentity.New(),
            CacheName = CacheName,
            Key = key,
            Entry = new NodeCacheEntry<object?> { Value = "static", Version = 1 }.MapToProto(),
        };

        _ = await Assert.That(await SetAsync(client, request, cancellationToken)).IsNull();

        using var ownerChannel = CreateGrpcChannel(cluster[OwnerId].Uri);
        var owner = new SquirixCacheService.SquirixCacheServiceClient(ownerChannel);
        var read = await owner.GetValueAsync(new GetValueAsyncRequest { CacheName = CacheName, Key = key }, deadline: DateTime.UtcNow.Add(Bound), cancellationToken: cancellationToken);
        _ = await Assert.That(read.Found).IsTrue();
    }

    /// <summary>Sends a write within <see cref="Bound" /> and returns its refusal.</summary>
    /// <param name="client">The client of the entry node.</param>
    /// <param name="request">The write.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The refusal; <see langword="null" /> when the write succeeded.</returns>
    private static async Task<RpcException?> SetAsync(SquirixCacheService.SquirixCacheServiceClient client, SetEntryAsyncRequest request, CancellationToken cancellationToken)
    {
        try
        {
            _ = await client.SetEntryAsync(request, deadline: DateTime.UtcNow.Add(Bound), cancellationToken: cancellationToken);
            return null;
        }
        catch (RpcException ex)
        {
            return ex;
        }
    }

    /// <summary>Finds a key of the owner group as the ring of a node places it.</summary>
    /// <param name="node">The node whose ring is read.</param>
    /// <returns>The key.</returns>
    /// <exception cref="InvalidOperationException">No key of the first thousand belongs to the owner.</exception>
    private static string KeyOwnedByOwner(ITestNodeHost node)
    {
        var ownership = node.GetRequiredService<INodeOwnershipResolver>();
        _ = ServerCacheName.TryParsePublic(CacheName, out var canonical);
        for (var i = 0; i < 1000; i++)
        {
            var key = "routed-" + i.ToString(CultureInfo.InvariantCulture);
            if (string.Equals(ownership.GetOwner(canonical!, key), OwnerId, StringComparison.Ordinal))
                return key;
        }

        throw new InvalidOperationException($"No key of the first thousand belongs to {OwnerId}.");
    }

    /// <summary>Waits until one of the given nodes has authority over the owner group, and returns the one of the highest term.</summary>
    /// <param name="cluster">The cluster.</param>
    /// <param name="candidates">The running nodes that may lead.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The leader.</returns>
    private static async Task<string> LeaderAsync(TestCluster<IntegrationStartOptions> cluster, string[] candidates, CancellationToken cancellationToken)
    {
        var leader = string.Empty;
        await cluster.WaitUntilAsync(
            nodes =>
            {
                var highest = 0UL;
                foreach (var candidate in candidates)
                {
                    if (Table(nodes[candidate]).HasLocalAuthority(OwnerId, out var term) && term > highest)
                        (leader, highest) = (candidate, term);
                }

                return highest != 0;
            },
            Bound,
            cancellationToken);
        return leader;
    }

    private static IntegrationStartOptions Options(string scope, bool failover, Action<IServiceCollection>? configure = null) => new()
    {
        ReplicaCount = 3,
        UsePersistence = true,
        CleanTestDir = true,
        ExtraScope = scope,
        AutomaticFailoverEnabled = failover,
        ServicesConfigure = services =>
        {
            _ = services.AddSingleton(new ElectionTimerOptions
            {
                ElectionTimeout = TimeSpan.FromSeconds(4),
                HeartbeatInterval = TimeSpan.FromMilliseconds(250),
                MaxJitter = TimeSpan.FromSeconds(2),
                VoteRpcTimeout = TimeSpan.FromSeconds(2),
            });
            configure?.Invoke(services);
        },
    };

    /// <summary>Tells whether a node follows the leader of the owner group.</summary>
    /// <param name="node">The node.</param>
    /// <param name="leader">The leader.</param>
    /// <param name="term">The term of the leader when the node follows it.</param>
    /// <returns><see langword="true" /> once the node knows the leader.</returns>
    private static bool Follows(ITestNodeHost node, string leader, out ulong term)
    {
        var known = Table(node).TryGetLeader(OwnerId, out var route) && string.Equals(route.NodeId, leader, StringComparison.Ordinal);
        term = known ? route.Term : 0;
        return known;
    }

    private static IGroupLeaderTable Table(ITestNodeHost host) => host.GetRequiredService<IGroupLeaderTable>();
}
