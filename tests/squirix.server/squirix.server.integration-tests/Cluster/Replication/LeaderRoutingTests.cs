using System;
using System.Collections.Generic;
using System.Diagnostics;
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
using Squirix.Server.TestKit.Networking;
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
    private const string EntryOutsideGroup = "node-d";
    private const string OwnerId = "node-a";

    /// <summary>
    /// The most writes the client sends once the owner stopped, where each failed one waits for a new leader before the next, and the most
    /// times a warm-up write is sent.
    /// </summary>
    private const int MaxWrites = 5;

    /// <summary>Bounds every wait for an election and every client call; the timeouts below elect within seconds.</summary>
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(90);

    private static readonly string[] Nodes = [OwnerId, "node-b", "node-c"];

    /// <summary>Four nodes, three replicas: the group of the owner is the owner, node-b and node-c, so node-d serves no part of it.</summary>
    private static readonly string[] FourNodes = [OwnerId, "node-b", "node-c", EntryOutsideGroup];

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
        await WarmUpAsync(client, KeyOwnedByOwner(cluster[entry], 1), cancellationToken);
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
        var key = KeyOwnedByOwner(cluster[entry]);
        using var channel = CreateGrpcChannel(cluster[entry].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        await WarmUpAsync(client, KeyOwnedByOwner(cluster[entry], 1), cancellationToken);
        probe.Arm(entry, OwnerId, new LeaderRoute(follower, term));

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

    /// <summary>
    /// With four nodes and three replicas, the owner of a group leads it and stops; a client that only reaches the node outside the group
    /// keeps sending the same write, as the client library retries, until it succeeds: the entry node falls back from the unreachable owner
    /// to another member, learns the new leader, and the write commits there within a few attempts.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(120_000)]
    public async Task NonMemberEntryReachesLeader(CancellationToken cancellationToken)
    {
        var topology = new ClusterNode[FourNodes.Length];
        for (var i = 0; i < topology.Length; i++)
            topology[i] = new ClusterNode(FourNodes[i], GetNextHttpUri());

        await using var fabric = new PartitionFabric();
        await using var cluster = await StartClusterAsync(topology, OwnerLeadsOptions("leader-routing-non-member", fabric), cancellationToken);
        _ = await Assert.That(await LeaderAsync(cluster, Nodes, cancellationToken)).IsEqualTo(OwnerId);
        var key = KeyOwnedByOwner(cluster[EntryOutsideGroup]);
        using var channel = CreateGrpcChannel(cluster[EntryOutsideGroup].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var request = new SetEntryAsyncRequest
        {
            OperationId = RpcOperationIdentity.New(),
            CacheName = CacheName,
            Key = key,
            Entry = new NodeCacheEntry<object?> { Value = "outside", Version = 1 }.MapToProto(),
        };

        // A refused loopback connect takes about two seconds on Windows and can outlast the per-attempt timeout of a forward, which is ambiguous
        // and never rerouted; resetting every dial towards the stopped owner makes its unreachability prompt on every platform.
        await cluster.StopNodeAsync(OwnerId);
        await fabric.IsolateAsync(OwnerId);
        string[] survivors = [Nodes[1], Nodes[2]];
        var outcomes = new List<string>();
        RpcException? refusal;
        do
        {
            var started = Stopwatch.GetTimestamp();
            refusal = await SetAsync(client, request, cancellationToken);
            outcomes.Add($"{(refusal == null ? "OK" : refusal.Status.Detail)} in {Stopwatch.GetElapsedTime(started).TotalMilliseconds:F0} ms");
            if (refusal is { StatusCode: StatusCode.Unavailable })
                _ = await LeaderAsync(cluster, survivors, cancellationToken);
        }
        while (refusal is { StatusCode: StatusCode.Unavailable } && outcomes.Count < MaxWrites);

        _ = await Assert.That(refusal).IsNull().Because($"the write through {EntryOutsideGroup} must reach the new leader; writes: {string.Join("; ", outcomes)}");
        var leader = await LeaderAsync(cluster, survivors, cancellationToken);
        _ = await Assert.That((Table(cluster[EntryOutsideGroup]).TryGetLearnedLeader(OwnerId, out var learned), learned.NodeId)).IsEqualTo((true, leader));
        var read = await client.GetValueAsync(
            new GetValueAsyncRequest { CacheName = CacheName, Key = key },
            deadline: DateTime.UtcNow.Add(Bound),
            cancellationToken: cancellationToken);
        _ = await Assert.That(read.Found).IsTrue();
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
        await WarmUpAsync(client, KeyOwnedByOwner(entry, 1), cancellationToken);
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

    /// <summary>
    /// Commits a write of another key of the owner group through the entry node, sending it again with the same operation id while its
    /// forward times out, as the client library retries.
    /// </summary>
    /// <remarks>
    /// A forward is sent once under a fixed per-attempt timeout. The first one from a node pays the mTLS handshake to its target and the
    /// first commit of the leader, which under load can outlast that timeout; the write under test then finds both warm.
    /// </remarks>
    /// <param name="client">The client of the entry node.</param>
    /// <param name="key">A key of the owner group the write under test does not use.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    private static async Task WarmUpAsync(SquirixCacheService.SquirixCacheServiceClient client, string key, CancellationToken cancellationToken)
    {
        var request = new SetEntryAsyncRequest
        {
            OperationId = RpcOperationIdentity.New(),
            CacheName = CacheName,
            Key = key,
            Entry = new NodeCacheEntry<object?> { Value = "warm", Version = 1 }.MapToProto(),
        };

        var outcomes = new List<string>();
        RpcException? refusal;
        do
        {
            refusal = await SetAsync(client, request, cancellationToken);
            outcomes.Add(refusal == null ? "OK" : refusal.Status.Detail);
        }
        while (refusal is { StatusCode: StatusCode.DeadlineExceeded } && outcomes.Count < MaxWrites);

        _ = await Assert.That(refusal).IsNull().Because($"the warm-up write must commit; writes: {string.Join("; ", outcomes)}");
    }

    /// <summary>Finds a key of the owner group as the ring of a node places it.</summary>
    /// <param name="node">The node whose ring is read.</param>
    /// <param name="ordinal">How many keys of the owner group to skip, so that two writes use distinct keys.</param>
    /// <returns>The key.</returns>
    /// <exception cref="InvalidOperationException">The first thousand keys hold too few of the owner.</exception>
    private static string KeyOwnedByOwner(ITestNodeHost node, int ordinal = 0)
    {
        var ownership = node.GetRequiredService<INodeOwnershipResolver>();
        _ = ServerCacheName.TryParsePublic(CacheName, out var canonical);
        var skipped = 0;
        for (var i = 0; i < 1000; i++)
        {
            var key = "routed-" + i.ToString(CultureInfo.InvariantCulture);
            if (!string.Equals(ownership.GetOwner(canonical!, key), OwnerId, StringComparison.Ordinal))
                continue;

            if (skipped == ordinal)
                return key;

            skipped++;
        }

        throw new InvalidOperationException($"The first thousand keys hold too few of {OwnerId}.");
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
        QuorumReadsEnabled = failover,
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

    /// <summary>
    /// Options under which the owner wins the first election of its group: its election timeout is a quarter of the others, so it campaigns
    /// first, while the others still elect a successor well within <see cref="Bound" /> once it stops.
    /// </summary>
    /// <param name="scope">The persistence scope.</param>
    /// <param name="fabric">The fabric the nodes dial each other through.</param>
    /// <returns>The options.</returns>
    private static IntegrationStartOptions OwnerLeadsOptions(string scope, PartitionFabric fabric) => new()
    {
        ReplicaCount = 3,
        UsePersistence = true,
        CleanTestDir = true,
        ExtraScope = scope,
        AutomaticFailoverEnabled = true,
        PartitionFabric = fabric,
        ServicesConfigure = static services => _ = services.AddSingleton(static sp => new ElectionTimerOptions
        {
            ElectionTimeout = string.Equals(sp.GetRequiredService<TopologyOptions>().NodeId, OwnerId, StringComparison.Ordinal) ? TimeSpan.FromSeconds(1) : TimeSpan.FromSeconds(4),
            HeartbeatInterval = TimeSpan.FromMilliseconds(250),
            MaxJitter = TimeSpan.FromSeconds(1),
            VoteRpcTimeout = TimeSpan.FromSeconds(2),
        }),
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
