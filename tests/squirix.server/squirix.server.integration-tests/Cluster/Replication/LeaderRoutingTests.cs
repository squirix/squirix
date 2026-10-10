using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Core;
using Squirix.Server.Errors;
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

    /// <summary>The entry node of the silent-leader test, whose election jitter is pinned.</summary>
    private const string SilentEntry = "node-b";

    /// <summary>A jitter seed whose first sixteen draws for the owner group at <see cref="SilentMaxJitter" /> are at least twenty seconds.</summary>
    private const ulong SilentJitterSeed = 3944UL;

    /// <summary>
    /// The most writes the client sends once the owner stopped, where each failed one waits for a new leader before the next, and the most
    /// times a warm-up write is sent.
    /// </summary>
    private const int MaxWrites = 5;

    /// <summary>Bounds every wait for an election and every client call; the timeouts below elect within seconds.</summary>
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(90);

    /// <summary>Bounds the forward channel's keepalive detection of a silent peer: five seconds at worst, doubled for a loaded host.</summary>
    private static readonly TimeSpan KeepAliveDetectionBound = TimeSpan.FromSeconds(10);

    private static readonly string[] Nodes = [OwnerId, "node-b", "node-c"];

    private static readonly TimeSpan SilentMaxJitter = TimeSpan.FromSeconds(40);

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

    /// <summary>
    /// With four nodes and three replicas, the owner of a group leads it and its host goes down without refusing connections: every dial
    /// towards it hangs. Once another member leads, a write sent to the node outside the group reaches the new leader, and no write ends in an
    /// ambiguous timeout: the dial bound ends each dial to the owner as a connect failure, so the entry node falls back to another member.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(120_000)]
    public async Task BlackHoledOwnerReachesNewLeader(CancellationToken cancellationToken)
    {
        var topology = new ClusterNode[FourNodes.Length];
        for (var i = 0; i < topology.Length; i++)
            topology[i] = new ClusterNode(FourNodes[i], GetNextHttpUri());

        await using var fabric = new PartitionFabric();
        await using var cluster = await StartClusterAsync(topology, OwnerLeadsOptions("leader-routing-black-hole", fabric), cancellationToken);
        _ = await Assert.That(await LeaderAsync(cluster, Nodes, cancellationToken)).IsEqualTo(OwnerId);
        var key = KeyOwnedByOwner(cluster[EntryOutsideGroup]);
        using var channel = CreateGrpcChannel(cluster[EntryOutsideGroup].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var request = new SetEntryAsyncRequest
        {
            OperationId = RpcOperationIdentity.New(),
            CacheName = CacheName,
            Key = key,
            Entry = new NodeCacheEntry<object?> { Value = "black-hole", Version = 1 }.MapToProto(),
        };

        await cluster.StopNodeAsync(OwnerId);
        await fabric.BlackHoleAsync(OwnerId);
        string[] survivors = [Nodes[1], Nodes[2]];
        var leader = await LeaderAsync(cluster, survivors, cancellationToken);
        var follower = Array.Find(survivors, id => !string.Equals(id, leader, StringComparison.Ordinal))!;
        await cluster.WaitUntilAsync(nodes => Follows(nodes[follower], leader, out _), Bound, cancellationToken);

        var outcomes = new List<string>();
        RpcException? refusal;
        do
        {
            var started = Stopwatch.GetTimestamp();
            refusal = await SetAsync(client, request, cancellationToken);
            outcomes.Add($"{(refusal == null ? "OK" : refusal.Status.Detail)} in {Stopwatch.GetElapsedTime(started).TotalMilliseconds:F0} ms");
        }
        while (refusal is { StatusCode: StatusCode.Unavailable } && outcomes.Count < MaxWrites);

        var writes = string.Join("; ", outcomes);
        _ = await Assert.That(refusal).IsNull().Because($"the write through {EntryOutsideGroup} must reach the new leader {leader}; writes: {writes}");
        _ = await Assert.That((Table(cluster[EntryOutsideGroup]).TryGetLearnedLeader(OwnerId, out var learned), learned.NodeId)).IsEqualTo((true, leader));
    }

    /// <summary>
    /// With four nodes and three replicas, the host of the owner of a group goes silent without resetting the connection the entry node outside
    /// the group already holds to it. The keepalive pings of the forward channel find that connection dead, so once another member leads, a
    /// write sent to the entry node dials the owner anew, fails at the dial bound and falls back to the new leader, instead of waiting out
    /// its per-attempt timeout on the dead connection.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(120_000)]
    public async Task SilentOwnerReachesNewLeader(CancellationToken cancellationToken)
    {
        var topology = new ClusterNode[FourNodes.Length];
        for (var i = 0; i < topology.Length; i++)
            topology[i] = new ClusterNode(FourNodes[i], GetNextHttpUri());

        await using var fabric = new PartitionFabric();
        await using var cluster = await StartClusterAsync(topology, OwnerLeadsOptions("leader-routing-silent", fabric), cancellationToken);
        _ = await Assert.That(await LeaderAsync(cluster, Nodes, cancellationToken)).IsEqualTo(OwnerId);
        var key = KeyOwnedByOwner(cluster[EntryOutsideGroup]);
        using var channel = CreateGrpcChannel(cluster[EntryOutsideGroup].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        await WarmUpAsync(client, KeyOwnedByOwner(cluster[EntryOutsideGroup], 1), cancellationToken);

        var entryPool = ThrowHelper.Required(await Assert.That(cluster[EntryOutsideGroup].GetRequiredService<IServerClientPool>()).IsTypeOf<ServerClientPool>(), "The entry node must use the transport pool.");
        var openBefore = entryPool.OpenConnections;

        // The owner's host dies: nothing it sends is delivered, nothing sent to it is answered, and no connection is reset.
        string[] survivors = [Nodes[1], Nodes[2]];
        fabric.BlackHoleSilently(OwnerId);
        foreach (var survivor in survivors)
        {
            fabric.HoldDirection(OwnerId, survivor);
            fabric.HoldDirection(survivor, OwnerId);
        }

        // The keepalive pings close the connection the entry node holds to the owner, which drops its open connection count. Without them the
        // connection stays open until the pool's one-minute idle timeout closes it, past the bound.
        await cluster.WaitUntilAsync(_ => entryPool.OpenConnections < openBefore, KeepAliveDetectionBound, cancellationToken);

        var leader = await LeaderAsync(cluster, survivors, cancellationToken);
        var follower = Array.Find(survivors, id => !string.Equals(id, leader, StringComparison.Ordinal))!;
        await cluster.WaitUntilAsync(nodes => Follows(nodes[follower], leader, out _), Bound, cancellationToken);

        // With the dead connection gone, a write dials anew: it fails at the dial bound (one second) and takes the new leader, and at worst one
        // more attempt follows the leader the entry node learns. The bound is the per-attempt timeout (three seconds) of such a failed attempt
        // plus the dial bound, twice, plus slack for a loaded host.
        // Every attempt resends the same write, as a client retry does, so a refused attempt that reached a node is not applied twice.
        var write = Write(key, "silent");
        var outcomes = new List<string>();
        var started = Stopwatch.GetTimestamp();
        RpcException? refusal;
        do
        {
            var attempt = Stopwatch.GetTimestamp();
            refusal = await SetAsync(client, write, cancellationToken);
            outcomes.Add($"{(refusal == null ? "OK" : refusal.Status.Detail)} in {Stopwatch.GetElapsedTime(attempt).TotalMilliseconds:F0} ms");
        }
        while (refusal is { StatusCode: StatusCode.Unavailable } && outcomes.Count < MaxWrites);

        var elapsed = Stopwatch.GetElapsedTime(started);
        var writes = string.Join("; ", outcomes);
        _ = await Assert.That(refusal).IsNull().Because($"the write through {EntryOutsideGroup} must reach the new leader {leader}; writes: {writes}");
        _ = await Assert.That(elapsed).IsLessThanOrEqualTo(TimeSpan.FromSeconds(15)).Because($"writes: {writes}");
        _ = await Assert.That((Table(cluster[EntryOutsideGroup]).TryGetLearnedLeader(OwnerId, out var learned), learned.NodeId)).IsEqualTo((true, leader));
        fabric.HealAll();
    }

    /// <summary>
    /// A follower whose leader stopped stops naming it once it heard nothing from it for an election timeout, even though no election can
    /// finish and the follower itself will not campaign for a long time: a write entering through it waits for the next leader instead of
    /// dialing the stopped one. The test pins the path where the third node, caught up with the stopped leader, wins the next election; the
    /// parked write is released when that node is named, may be refused by it until its authority is committed, and is sent again once it
    /// has authority.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(180_000)]
    public async Task SilentLeaderIsNotDialed(CancellationToken cancellationToken)
    {
        await AssertSilentSeedAsync();

        var topology = new ClusterNode[Nodes.Length];
        for (var i = 0; i < topology.Length; i++)
            topology[i] = new ClusterNode(Nodes[i], GetNextHttpUri());

        var probe = new LeaderRouteProbe();
        await using var fabric = new PartitionFabric();
        await using var cluster = await StartClusterAsync(topology, SilentLeaderOptions("leader-routing-silent-leader", fabric, probe), cancellationToken);
        var leader = await LeaderAsync(cluster, Nodes, cancellationToken);
        _ = await Assert.That(leader).IsNotEqualTo(SilentEntry);
        var third = Array.Find(Nodes, id => !string.Equals(id, leader, StringComparison.Ordinal) && !string.Equals(id, SilentEntry, StringComparison.Ordinal))!;
        await cluster.WaitUntilAsync(nodes => Follows(nodes[SilentEntry], leader, out _), Bound, cancellationToken);
        var key = KeyOwnedByOwner(cluster[SilentEntry]);
        using var channel = CreateGrpcChannel(cluster[SilentEntry].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        await WarmUpAsync(client, KeyOwnedByOwner(cluster[SilentEntry], 1), cancellationToken);
        await AwaitCaughtUpAsync(cluster[leader], cluster[third], cancellationToken);

        // The entry node and the third node cannot reach each other, so no election can finish while the leader is gone.
        fabric.HoldDirection(SilentEntry, third);
        fabric.HoldDirection(third, SilentEntry);
        await fabric.BlackHoleAsync(leader);
        await cluster.StopNodeAsync(leader);
        var state = cluster[SilentEntry].GetRequiredService<ReplicaGroupRegistry>().StateFor(OwnerId);
        await cluster.WaitUntilAsync(_ => !state.ObserveStatus().HasMajorityContact, Bound, cancellationToken);

        _ = await Assert.That(Table(cluster[SilentEntry]).TryGetLeader(OwnerId, out _)).IsFalse().Because($"{SilentEntry} must not name the silent leader {leader}");

        probe.Record(SilentEntry, OwnerId);
        var write = Write(key, "silent-leader");
        var pending = SetAsync(client, write, cancellationToken);
        fabric.ReleaseDirection(SilentEntry, third);
        fabric.ReleaseDirection(third, SilentEntry);
        var (refusal, outcomes) = await ResendAsync(cluster, client, write, await pending, [SilentEntry, third], cancellationToken);
        var writes = string.Join("; ", outcomes);
        _ = await Assert.That(refusal).IsNull().Because($"the write through {SilentEntry} must reach the new leader; writes: {writes}");
        _ = await Assert.That(outcomes.Contains(ServerOpContract.OwnerUnreachableDetail)).IsFalse().Because($"writes: {writes}");
        var dialed = Array.Exists(probe.Forwards(), forward => string.Equals(forward.Target, leader, StringComparison.Ordinal));
        _ = await Assert.That(dialed).IsFalse().Because($"{SilentEntry} must not forward to the stopped leader {leader}");
        fabric.HealAll();
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

    /// <summary>Builds a write of <paramref name="value" /> to <paramref name="key" /> with a new operation id.</summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value.</param>
    /// <returns>The write.</returns>
    private static SetEntryAsyncRequest Write(string key, string value) => new()
    {
        OperationId = RpcOperationIdentity.New(),
        CacheName = CacheName,
        Key = key,
        Entry = new NodeCacheEntry<object?> { Value = value, Version = 1 }.MapToProto(),
    };

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
        QuorumReadsEnabled = true,
        PartitionFabric = fabric,
        ServicesConfigure = static services => _ = services.AddSingleton(static sp => new ElectionTimerOptions
        {
            ElectionTimeout = string.Equals(sp.GetRequiredService<TopologyOptions>().NodeId, OwnerId, StringComparison.Ordinal) ? TimeSpan.FromSeconds(1) : TimeSpan.FromSeconds(4),
            HeartbeatInterval = TimeSpan.FromMilliseconds(250),
            MaxJitter = TimeSpan.FromSeconds(1),
            VoteRpcTimeout = TimeSpan.FromSeconds(2),
        }),
    };

    /// <summary>
    /// Options under which the owner wins the first election of its group and the entry node of the silent-leader test, which has a short
    /// election timeout but a very long jitter, does not campaign for a long time.
    /// </summary>
    /// <param name="scope">The persistence scope.</param>
    /// <param name="fabric">The fabric the nodes dial each other through.</param>
    /// <param name="probe">The probe that records the forwards of the entry node.</param>
    /// <returns>The options.</returns>
    private static IntegrationStartOptions SilentLeaderOptions(string scope, PartitionFabric fabric, LeaderRouteProbe probe) => new()
    {
        ReplicaCount = 3,
        UsePersistence = true,
        CleanTestDir = true,
        ExtraScope = scope,
        AutomaticFailoverEnabled = true,
        QuorumReadsEnabled = true,
        PartitionFabric = fabric,
        ServicesConfigure = services =>
        {
            _ = services.AddSingleton(static sp =>
            {
                var nodeId = sp.GetRequiredService<TopologyOptions>().NodeId;
                var entry = string.Equals(nodeId, SilentEntry, StringComparison.Ordinal);
                var election = TimeSpan.FromSeconds(string.Equals(nodeId, OwnerId, StringComparison.Ordinal) ? 1 : 4);
                return new ElectionTimerOptions
                {
                    ElectionTimeout = entry ? TimeSpan.FromSeconds(2) : election,
                    HeartbeatInterval = TimeSpan.FromMilliseconds(250),
                    MaxJitter = entry ? SilentMaxJitter : TimeSpan.FromSeconds(1),
                    VoteRpcTimeout = TimeSpan.FromSeconds(2),
                    LeaderWaitTimeoutOverride = entry ? TimeSpan.FromSeconds(30) : null,
                    JitterSeed = entry ? SilentJitterSeed : BinaryPrimitives.ReadUInt64LittleEndian(RandomNumberGenerator.GetBytes(sizeof(ulong))),
                };
            });
            probe.Register(services);
        },
    };

    /// <summary>
    /// Waits for a parked write and sends it again with the same operation id while it is refused as unavailable, each time after a survivor
    /// has authority over the owner group.
    /// </summary>
    /// <param name="cluster">The cluster.</param>
    /// <param name="client">The client of the entry node.</param>
    /// <param name="write">The write.</param>
    /// <param name="first">The refusal of the write already sent, or <see langword="null" /> when it succeeded.</param>
    /// <param name="survivors">The running nodes that may lead.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The last refusal, or <see langword="null" /> when the write succeeded, and the outcome of every send.</returns>
    private static async Task<(RpcException? Refusal, List<string> Outcomes)> ResendAsync(
        TestCluster<IntegrationStartOptions> cluster,
        SquirixCacheService.SquirixCacheServiceClient client,
        SetEntryAsyncRequest write,
        RpcException? first,
        string[] survivors,
        CancellationToken cancellationToken)
    {
        var outcomes = new List<string>();
        var refusal = first;
        outcomes.Add(refusal == null ? "OK" : refusal.Status.Detail);
        while (refusal is { StatusCode: StatusCode.Unavailable } && outcomes.Count < MaxWrites)
        {
            _ = await LeaderAsync(cluster, survivors, cancellationToken);
            refusal = await SetAsync(client, write, cancellationToken);
            outcomes.Add(refusal == null ? "OK" : refusal.Status.Detail);
        }

        return (refusal, outcomes);
    }

    /// <summary>Waits until a follower holds every entry the leader holds in the owner group log.</summary>
    /// <param name="leader">The leader.</param>
    /// <param name="follower">The follower.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="TimeoutException">The follower did not catch up within <see cref="Bound" />.</exception>
    private static Task AwaitCaughtUpAsync(ITestNodeHost leader, ITestNodeHost follower, CancellationToken cancellationToken)
    {
        _ = leader.GetRequiredService<ReplicaGroupRegistry>().TryGetLog(OwnerId, out var leaderLog);
        _ = follower.GetRequiredService<ReplicaGroupRegistry>().TryGetLog(OwnerId, out var followerLog);
        return (Leader: leaderLog!, Follower: followerLog!).WaitUntilValueAsync(
            static async (logs, token) => (await logs.Follower.GetStatusAsync(token)).LastLogIndex >= (await logs.Leader.GetStatusAsync(token)).LastLogIndex,
            Bound,
            cancellationToken);
    }

    /// <summary>Checks that the pinned seed of the entry node draws only long jitters, so it does not campaign during the silent-leader test.</summary>
    /// <returns>An asynchronous operation.</returns>
    private static async Task AssertSilentSeedAsync()
    {
        var jitter = new ElectionJitter(SilentJitterSeed, OwnerId);
        for (var i = 0; i < 16; i++)
            _ = await Assert.That(jitter.Next(SilentMaxJitter)).IsGreaterThanOrEqualTo(TimeSpan.FromSeconds(20));
    }

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
