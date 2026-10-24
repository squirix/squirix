using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.LocalCache;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Core.Enums;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>
/// A whole node, a member of every group, that loses its disk is repaired group by group while the cluster stays in the terms the node already
/// voted in: it casts no second vote there, and it serves the values it holds from its own copies.
/// </summary>
[ExecutionPriority(Priority.High)]
public sealed class WipedNodeTests : NodeIntegrationTestBase
{
    private const string Wiped = "node-c";
    private const string Peer = "node-b";
    private const ulong StaticLeaderTerm = 1UL;
    private const int KeyCount = 300;
    private const string BeforeValue = "before";
    private const string DuringValue = "during";

    private static readonly string[] Others = [WipedCluster.OwnerId, Peer];
    private static readonly string[] Followers = [Peer, Wiped];

    /// <summary>
    /// An election of the first group, held while its owner is down, makes one of the other two nodes vote for the other in an elected term. When
    /// that follower loses its whole disk and is repaired while the cluster stays in the term, each of its copies holds the term of its leader and
    /// a vote for that leader: another candidate in that term is refused, the leader is granted again, and all of its groups are complete.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WipedNodeVotesOnceInTermOfEachGroup(CancellationToken cancellationToken)
    {
        const string scope = "wiped-node-vote";
        await using var cluster = await StartAsync(scope, cancellationToken);
        var ledger = new GroupAuthorityLedger<IntegrationStartOptions>(cluster, WipedCluster.OwnerId, WipedCluster.Bound);

        // Without its owner the first group elects a leader among the other two nodes; the one that does not lead it votes for the one that does.
        await cluster.StopNodeAsync(WipedCluster.OwnerId);
        var (leaderId, term) = await ledger.LeaderAsync(Followers, StaticLeaderTerm, "the other two nodes elect a leader of the first group", cancellationToken);
        _ = await cluster.StartNodeAsync(WipedCluster.OwnerId, WipedCluster.Options(scope, true, null, false), cancellationToken);
        var wipedId = string.Equals(leaderId, Peer, StringComparison.Ordinal) ? Wiped : Peer;
        var voter = await WipedCluster.Log(cluster[wipedId], WipedCluster.OwnerId).GetStatusAsync(cancellationToken);
        _ = await Assert.That((voter.CurrentTerm, voter.VotedFor)).IsEqualTo((term, leaderId));

        var survivors = new[] { WipedCluster.OwnerId, leaderId };
        var ownGroup = new GroupAuthorityLedger<IntegrationStartOptions>(cluster, wipedId, WipedCluster.Bound);
        var wiped = await cluster.RestartWipedNodeAsync(wipedId, null, cancellationToken);
        var (ownLeader, ownTerm) = await ownGroup.LeaderAsync(survivors, StaticLeaderTerm, "the surviving nodes elect a leader of the group the wiped node owns", cancellationToken);

        // The group of the surviving follower keeps its static leader and term; the first group keeps the elected leader and term the node voted in.
        var groups = new (string Group, string LeaderId, ulong Term)[]
        {
            (WipedCluster.OwnerId, leaderId, term),
            (leaderId, leaderId, StaticLeaderTerm),
            (wipedId, ownLeader, ownTerm),
        };
        foreach (var (group, groupLeader, groupTerm) in groups)
        {
            var repaired = await WipedCluster.AwaitRepairedCopyAsync(wiped, group, groupLeader, cancellationToken);
            var leader = await WipedCluster.Log(cluster[groupLeader], group).GetStatusAsync(cancellationToken);

            // Neither the leader nor its term changed while the node was down and repaired, so every vote below falls into the term it voted in.
            _ = await Assert.That((leader.CurrentTerm, repaired.CurrentTerm)).IsEqualTo((groupTerm, groupTerm));
            if (groupTerm > StaticLeaderTerm)
                await AssertVotesOnceAsync(cluster, group, groupLeader, wipedId, leader, cancellationToken);

            await WipedCluster.AssertCompleteCopyAsync(wiped, group, cancellationToken);
        }
    }

    /// <summary>
    /// Keys written before the node lost its disk and while it was down are all read back from the repaired node's own local cache, with the values
    /// the cluster holds, for the groups it owns and for those it only follows.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RepairedNodeServesManyKeysFromItsOwnCopy(CancellationToken cancellationToken)
    {
        await using var cluster = await StartAsync("wiped-node-keys", cancellationToken);
        var locator = cluster[WipedCluster.OwnerId].GetRequiredService<INodeLocator>();
        var expected = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 0; i < KeyCount; i++)
        {
            var key = $"{WipedCluster.CacheName}-{i}";
            await WriteAsync(cluster[locator.GetOwner(WipedCluster.CacheName, key)], key, BeforeValue, 1, cancellationToken);
            expected[key] = BeforeValue;
        }

        await cluster.WipeNodeAsync(Wiped);

        // While the node is down, the keys of the groups the others own still take writes; the node must receive them with the repair.
        var ledger = new GroupAuthorityLedger<IntegrationStartOptions>(cluster, Wiped, WipedCluster.Bound);
        for (var i = 0; i < KeyCount; i++)
        {
            var key = $"{WipedCluster.CacheName}-{i}";
            var owner = locator.GetOwner(WipedCluster.CacheName, key);
            if (string.Equals(owner, Wiped, StringComparison.Ordinal))
                continue;

            await WriteAsync(cluster[owner], key, DuringValue, 2, cancellationToken);
            expected[key] = DuringValue;
        }

        var wiped = await cluster.StartWipedNodeAsync(Wiped, null, cancellationToken);
        var (electedLeader, _) = await ledger.LeaderAsync(Others, 0UL, "the other members elect a leader of the group the node owns", cancellationToken);
        foreach (var group in WipedCluster.Nodes)
            _ = await WipedCluster.AwaitRepairedCopyAsync(wiped, group, string.Equals(group, Wiped, StringComparison.Ordinal) ? electedLeader : group, cancellationToken);

        var local = wiped.GetRequiredService<PhysicalCache<object?>>();
        var mismatches = new List<string>();
        foreach (var (key, value) in expected)
        {
            var read = await local.GetValueAsync(new CacheKey(WipedCluster.CacheName, key), cancellationToken);
            if (!read.Found || !string.Equals(read.Value as string, value, StringComparison.Ordinal))
                mismatches.Add(key);
        }

        _ = await Assert.That(mismatches).IsEmpty();
        _ = await Assert.That(expected.Count).IsEqualTo(KeyCount);
    }

    /// <summary>Asks the repaired copy for a vote in the term it holds, for another candidate and for the leader that repaired it.</summary>
    /// <param name="cluster">The cluster; each candidate sends its own request, as the node is bound to the identity of its certificate.</param>
    /// <param name="group">The group.</param>
    /// <param name="leaderId">The leader of the group.</param>
    /// <param name="voterId">The node whose repaired copy is asked.</param>
    /// <param name="leader">The status of the leader's copy, which is up to date for both candidates.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the other candidate was refused and the leader granted.</returns>
    private static async Task AssertVotesOnceAsync(TestCluster<IntegrationStartOptions> cluster, string group, string leaderId, string voterId, FollowerLogStatus leader, CancellationToken cancellationToken)
    {
        var rival = FindRival(leaderId, voterId);

        var rivalHeader = new ReplicaRpcHeader(group, leader.TopologyFingerprint, leader.ConfigurationGeneration, leader.CurrentTerm, rival, rival);
        var leaderHeader = new ReplicaRpcHeader(group, leader.TopologyFingerprint, leader.ConfigurationGeneration, leader.CurrentTerm, leaderId, leaderId);
        var refused = await cluster[rival].GetRequiredService<IReplicaVoteGateway>().RequestVoteAsync(voterId, rivalHeader, leader.LastLogIndex, leader.LastLogTerm, cancellationToken);
        var granted = await cluster[leaderId].GetRequiredService<IReplicaVoteGateway>().RequestVoteAsync(voterId, leaderHeader, leader.LastLogIndex, leader.LastLogTerm, cancellationToken);

        _ = await Assert.That(refused).IsEqualTo(new FollowerLogVoteResult(false, RefusalCodes.AlreadyVoted, leader.CurrentTerm));
        _ = await Assert.That(granted.Granted).IsTrue();
    }

    /// <summary>Finds a node that is neither the leader of a group nor the voter asked.</summary>
    /// <param name="leaderId">The leader of the group.</param>
    /// <param name="voterId">The voter.</param>
    /// <returns>The third node.</returns>
    /// <exception cref="InvalidOperationException">The cluster has no third node.</exception>
    private static string FindRival(string leaderId, string voterId)
    {
        foreach (var node in WipedCluster.Nodes)
        {
            if (!string.Equals(node, leaderId, StringComparison.Ordinal) && !string.Equals(node, voterId, StringComparison.Ordinal))
                return node;
        }

        throw new InvalidOperationException("The cluster has no third node.");
    }

    /// <summary>Writes a value of a key through the node that owns it.</summary>
    /// <param name="owner">The node that owns the key and leads its group.</param>
    /// <param name="key">The key.</param>
    /// <param name="value">The value.</param>
    /// <param name="version">The version of the entry.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the write is committed.</returns>
    private static ValueTask WriteAsync(ITestNodeHost owner, string key, string value, long version, CancellationToken cancellationToken)
    {
        var entry = new NodeCacheEntry<object?> { Value = value, Version = version };
        return owner.GetCache<object?>(WipedCluster.CacheName).SetEntryAsync(Guid.NewGuid().ToString("N"), WipedCluster.CacheName, key, entry, cancellationToken);
    }

    /// <summary>Starts the three-node cluster with automatic failover and waits for every owner to lead its group.</summary>
    /// <param name="scope">The scope of the data directories.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The started cluster.</returns>
    private async ValueTask<TestCluster<IntegrationStartOptions>> StartAsync(string scope, CancellationToken cancellationToken)
    {
        var cluster = await StartClusterAsync(WipedCluster.Nodes[0], WipedCluster.Nodes[1], WipedCluster.Nodes[2], WipedCluster.Options(scope, true), cancellationToken, scope);
        try
        {
            await cluster.WaitUntilAsync(
                static nodes =>
                {
                    foreach (var nodeId in WipedCluster.Nodes)
                    {
                        if (!nodes[nodeId].GetRequiredService<IGroupLeaderTable>().HasLocalAuthority(nodeId, out _))
                            return false;
                    }

                    return true;
                },
                WipedCluster.Bound,
                TimeProvider.System,
                cancellationToken);
            return cluster;
        }
        catch
        {
            await cluster.DisposeAsync();
            throw;
        }
    }
}
