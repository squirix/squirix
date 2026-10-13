using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>The first start of a fresh cluster: the owner of each group seeds it, and the cluster then serves writes and reads.</summary>
public sealed class ReplicaGroupBootstrapTests : NodeIntegrationTestBase
{
    private const string CacheName = "default";
    private const string OwnerId = "node-a";

    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(60);
    private static readonly string[] Nodes = [OwnerId, "node-b", "node-c"];

    /// <summary>The status and the offer calls travel the internal mTLS listener to a member whose copy is unseeded, and only the owner of a group may offer.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GatewayStatusAndOfferRoundTrip(CancellationToken cancellationToken)
    {
        await using var cluster = CreateCluster([new ClusterNode(Nodes[0], GetNextHttpUri()), new ClusterNode(Nodes[1], GetNextHttpUri()), new ClusterNode(Nodes[2], GetNextHttpUri())]);
        var options = Options("bootstrap-gateway", false);

        // The owner of the group stays down, so no first start in this cluster can complete.
        var owner = await cluster.StartNodeAsync(Nodes[1], options, cancellationToken);
        var member = await cluster.StartNodeAsync(Nodes[2], options, cancellationToken);
        var own = await Log(owner, Nodes[1]).GetStatusAsync(cancellationToken);
        var header = new ReplicaRpcHeader(Nodes[1], own.TopologyFingerprint, own.ConfigurationGeneration, 0UL, string.Empty, Nodes[1]);
        var gateway = owner.GetRequiredService<IReplicaBootstrapGateway>();

        var reported = await gateway.GetStatusAsync(Nodes[2], header, cancellationToken);
        var offered = await gateway.OfferAsync(Nodes[2], header, cancellationToken);
        var after = await Log(member, Nodes[1]).GetStatusAsync(cancellationToken);
        var foreign = await gateway.OfferAsync(Nodes[2], header with { GroupId = Nodes[2] }, cancellationToken);

        _ = await Assert.That((reported.MemberState, reported.Ready, reported.RefusalCode)).IsEqualTo((GroupMemberState.Unseeded, true, string.Empty));
        _ = await Assert.That(reported.ConfigurationGeneration).IsEqualTo(own.ConfigurationGeneration);
        _ = await Assert.That(reported.TopologyFingerprint.Span.SequenceEqual(own.TopologyFingerprint.Span)).IsTrue();
        _ = await Assert.That(offered).IsEqualTo(new ReplicaBootstrapOfferResult(true, GroupMemberState.Unseeded, string.Empty));
        _ = await Assert.That(after.MemberState).IsEqualTo(GroupMemberState.Unseeded);
        _ = await Assert.That(foreign).IsEqualTo(new ReplicaBootstrapOfferResult(false, null, RefusalCodes.NotMember));
    }

    /// <summary>Three fresh nodes without failover seed their groups; the owner then serves a write and a read, and its followers complete.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FreshClusterServesWithoutFailover(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(Nodes[0], Nodes[1], Nodes[2], Options("bootstrap-static", false), cancellationToken);
        await AssertOwnGroupsCompleteAsync(cluster, cancellationToken);
        var owner = cluster[OwnerId];
        await ReplicaGroupFollowers.AwaitVerifiedAsync(owner, cancellationToken);

        await WriteAndReadAsync(owner, cancellationToken);

        await AssertCopiesCompleteAsync(cluster, OwnerId, cancellationToken);
    }

    /// <summary>Three fresh nodes with failover seed their groups; the owner then leads term one, serves a write and a quorum read, and its followers complete.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FreshClusterServesWithFailover(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(Nodes[0], Nodes[1], Nodes[2], Options("bootstrap-elected", true), cancellationToken);
        await AssertOwnGroupsCompleteAsync(cluster, cancellationToken);
        var owner = cluster[OwnerId];
        await cluster.WaitUntilAsync(static nodes => nodes[OwnerId].GetRequiredService<IGroupLeaderTable>().HasLocalAuthority(OwnerId, out _), Bound, cancellationToken);

        await WriteAndReadAsync(owner, cancellationToken);

        await AssertCopiesCompleteAsync(cluster, OwnerId, cancellationToken);
    }

    private static IntegrationStartOptions Options(string scope, bool failover) => new()
    {
        ReplicaCount = 3,
        UsePersistence = true,
        CleanTestDir = true,
        ExtraScope = scope,
        AutomaticFailoverEnabled = failover,
        QuorumReadsEnabled = failover,
    };

    private static IFollowerLog Log(ITestNodeHost host, string groupId) =>
        host.GetRequiredService<ReplicaGroupRegistry>().TryGetLog(groupId, out var log) ? log : throw new InvalidOperationException($"The node does not serve group {groupId}.");

    /// <summary>Checks that every node completed the copy of the group it owns.</summary>
    /// <param name="cluster">The started cluster.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    private static async Task AssertOwnGroupsCompleteAsync(TestCluster<IntegrationStartOptions> cluster, CancellationToken cancellationToken)
    {
        foreach (var node in Nodes)
            _ = await Assert.That((await Log(cluster[node], node).GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Complete);
    }

    /// <summary>Waits until every member holds a complete copy of a group, which its leader completes by its first calls.</summary>
    /// <param name="cluster">The started cluster.</param>
    /// <param name="groupId">The group.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    private static Task AssertCopiesCompleteAsync(TestCluster<IntegrationStartOptions> cluster, string groupId, CancellationToken cancellationToken) =>
        (Cluster: cluster, GroupId: groupId).WaitUntilValueAsync(
            static async (state, token) =>
            {
                foreach (var node in Nodes)
                {
                    if ((await Log(state.Cluster[node], state.GroupId).GetStatusAsync(token)).MemberState != GroupMemberState.Complete)
                        return false;
                }

                return true;
            },
            Bound,
            cancellationToken);

    private static async Task WriteAndReadAsync(ITestNodeHost owner, CancellationToken cancellationToken)
    {
        var cache = owner.GetCache<object?>(CacheName);
        var key = owner.FindKeyOwnedBy(CacheName, OwnerId);

        await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, key, new NodeCacheEntry<object?> { Value = "seeded", Version = 1 }, cancellationToken);
        var read = await cache.GetValueAsync(CacheName, key, cancellationToken);

        _ = await Assert.That((read.Found, read.Value)).IsEqualTo((true, "seeded"));
    }
}
