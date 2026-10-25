using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Core.Enums;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>
/// A whole node, a member of every group, that loses its disk is repaired group by group. In the term it voted in, the repaired copy casts no
/// second vote, even across a restart that keeps the repaired disk, and it serves the values of its groups from its own copies.
/// </summary>
[ExecutionPriority(Priority.High)]
public sealed class WipedNodeTests : NodeIntegrationTestBase
{
    private const string NodeB = "node-b";
    private const string NodeC = "node-c";
    private const ulong StaticLeaderTerm = 1UL;
    private const int KeyCount = 300;
    private const int LeaderOwnedKeyCount = 20;
    private const long FirstVersion = 1;
    private const long SecondVersion = 2;
    private const string BeforeValue = "before";
    private const string DuringValue = "during";
    private const string ElectedValue = "elected";

    private static readonly string[] Followers = [NodeB, NodeC];

    /// <summary>
    /// An election of the first group, held while its owner is down, makes one of the other two nodes vote for the other in an elected term. When
    /// that follower loses its whole disk and is repaired, its copy of the first group holds the term of the leader that repaired it and a
    /// vote for that leader, also after a restart that keeps the repaired disk: another candidate in that term is refused and the leader is
    /// granted again. Its other groups end complete at their unchanged terms, and the copy of its own group holds the values only the elected
    /// leader committed while the node was down.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WipedNodeVotesOnceInTermItVotedIn(CancellationToken cancellationToken)
    {
        const string scope = "wiped-node-vote";
        await using var cluster = await StartAsync(scope, cancellationToken);
        var ledger = new GroupAuthorityLedger<IntegrationStartOptions>(cluster, WipedCluster.OwnerId, WipedCluster.Bound);

        // Without its owner the first group elects a leader among the other two nodes; the one that does not lead it votes for the one that does.
        await cluster.StopNodeAsync(WipedCluster.OwnerId);
        var (leaderId, _) = await ledger.LeaderAsync(Followers, StaticLeaderTerm, "the other two nodes elect a leader of the first group", cancellationToken);
        _ = await cluster.StartNodeAsync(WipedCluster.OwnerId, WipedCluster.Options(scope, true, null, false), cancellationToken);
        var wipedId = string.Equals(leaderId, NodeB, StringComparison.Ordinal) ? NodeC : NodeB;

        // While the node is down, the surviving nodes elect a leader of the group it owns and commit values that only they hold.
        string[] survivors = [WipedCluster.OwnerId, leaderId];
        var ownGroup = new GroupAuthorityLedger<IntegrationStartOptions>(cluster, wipedId, WipedCluster.Bound);
        await cluster.WipeNodeAsync(wipedId);
        var (ownLeader, _) = await ownGroup.LeaderAsync(survivors, StaticLeaderTerm, "the surviving nodes elect a leader of the group the wiped node owns", cancellationToken);
        var committed = await CommitOwnedKeysAsync(cluster[ownLeader], wipedId, cancellationToken);
        var wiped = await cluster.StartWipedNodeAsync(wipedId, null, cancellationToken);

        // A starved host may let a heartbeat miss its election timeout, so a group may elect a new leader on its own; each repaired copy is therefore
        // checked against the leader that leads its group now, in that leader's term.
        var groups = new (string Group, GroupAuthorityLedger<IntegrationStartOptions> Watch)[]
        {
            (WipedCluster.OwnerId, ledger),
            (leaderId, new GroupAuthorityLedger<IntegrationStartOptions>(cluster, leaderId, WipedCluster.Bound)),
            (wipedId, ownGroup),
        };
        WipedCluster.RepairedCopy first = new();
        foreach (var (group, watch) in groups)
        {
            var repaired = await WipedCluster.AwaitRepairedByLeaderAsync(cluster, watch, wipedId, group, cancellationToken);
            await WipedCluster.AssertCompleteCopyAsync(wiped, group, cancellationToken);
            if (string.Equals(group, WipedCluster.OwnerId, StringComparison.Ordinal))
                first = repaired;
        }

        await AssertVotesOnceAsync(cluster, WipedCluster.OwnerId, first.LeaderId, wipedId, first.Leader, cancellationToken);
        await AssertValuesAsync(wiped, committed, cancellationToken);

        // The vote is durable: after a restart that keeps the repaired disk the copy still holds the vote of the leader of its term and refuses another candidate in it.
        _ = await cluster.RestartNodeAsync(wipedId, null, cancellationToken);
        var restarted = await WipedCluster.AwaitRepairedByLeaderAsync(cluster, ledger, wipedId, WipedCluster.OwnerId, cancellationToken);
        await AssertVotesOnceAsync(cluster, WipedCluster.OwnerId, restarted.LeaderId, wipedId, restarted.Leader, cancellationToken);
    }

    /// <summary>
    /// Keys written before the node lost its disk and while it was down are all read back from the repaired node's own local cache, with the values
    /// the cluster holds, for the groups it owns and for those it only follows.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RepairedNodeServesManyKeysFromItsOwnCopy(CancellationToken cancellationToken)
    {
        string[] others = [WipedCluster.OwnerId, NodeB];
        await using var cluster = await StartAsync("wiped-node-keys", cancellationToken);
        var locator = cluster[WipedCluster.OwnerId].GetRequiredService<INodeLocator>();
        var expected = new Dictionary<string, string>(StringComparer.Ordinal);
        var owners = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < KeyCount; i++)
        {
            var key = $"{WipedCluster.CacheName}-{i}";
            var owner = locator.GetOwner(WipedCluster.CacheName, key);
            await WriteAsync(cluster[owner], key, BeforeValue, FirstVersion, cancellationToken);
            expected[key] = BeforeValue;
            _ = owners.Add(owner);
        }

        await cluster.WipeNodeAsync(NodeC);

        // While the node is down, the keys of the groups the others own still take writes; the node must receive them with the repair.
        var ledger = new GroupAuthorityLedger<IntegrationStartOptions>(cluster, NodeC, WipedCluster.Bound);
        var duringCount = 0;
        for (var i = 0; i < KeyCount; i++)
        {
            var key = $"{WipedCluster.CacheName}-{i}";
            var owner = locator.GetOwner(WipedCluster.CacheName, key);
            if (string.Equals(owner, NodeC, StringComparison.Ordinal))
                continue;

            await WriteAsync(cluster[owner], key, DuringValue, SecondVersion, cancellationToken);
            expected[key] = DuringValue;
            duringCount++;
        }

        var wiped = await cluster.StartWipedNodeAsync(NodeC, null, cancellationToken);
        var (electedLeader, _) = await ledger.LeaderAsync(others, StaticLeaderTerm, "the other members elect a leader of the group the node owns", cancellationToken);
        foreach (var group in WipedCluster.Nodes)
            _ = await WipedCluster.AwaitRepairedCopyAsync(wiped, group, string.Equals(group, NodeC, StringComparison.Ordinal) ? electedLeader : group, cancellationToken);

        // The keys cover the groups of all three nodes, and some of them carry a value only the leaders held while the node was down.
        _ = await Assert.That(owners.Count).IsEqualTo(WipedCluster.Nodes.Length);
        _ = await Assert.That(duringCount).IsGreaterThan(0);
        await AssertValuesAsync(wiped, expected, cancellationToken);
    }

    /// <summary>Checks that the local cache of a node holds every expected value, allowing the tail of the log to arrive after the copy is complete.</summary>
    /// <param name="host">The node whose own copy is read.</param>
    /// <param name="expected">The expected value of every key.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once every key holds its value.</returns>
    /// <exception cref="TimeoutException">Some keys did not hold their value within the bound.</exception>
    private static async Task AssertValuesAsync(ITestNodeHost host, Dictionary<string, string> expected, CancellationToken cancellationToken)
    {
        var local = host.GetRequiredService<PhysicalCache<object?>>();
        try
        {
            await (Local: local, Expected: expected).WaitUntilValueAsync(
                static async (state, token) => (await FindMismatchesAsync(state.Local, state.Expected, token)).Count == 0,
                WipedCluster.Bound,
                TimeProvider.System,
                cancellationToken);
        }
        catch (TimeoutException exception)
        {
            var mismatches = await FindMismatchesAsync(local, expected, cancellationToken);
            if (mismatches.Count == 0)
                throw;

            throw new TimeoutException($"{mismatches.Count} of {expected.Count} keys do not hold their value on the repaired node, for example {mismatches[0]}.", exception);
        }

        var remaining = await FindMismatchesAsync(local, expected, cancellationToken);
        _ = await Assert.That(remaining).IsEmpty();
    }

    /// <summary>Reads every expected key from the local cache of a node.</summary>
    /// <param name="local">The local cache.</param>
    /// <param name="expected">The expected value of every key.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The keys that are missing or hold another value.</returns>
    private static async Task<List<string>> FindMismatchesAsync(PhysicalCache<object?> local, Dictionary<string, string> expected, CancellationToken cancellationToken)
    {
        var mismatches = new List<string>();
        foreach (var (key, value) in expected)
        {
            var read = await local.GetValueAsync(new CacheKey(WipedCluster.CacheName, key), cancellationToken);
            if (!read.Found || !string.Equals(read.Value as string, value, StringComparison.Ordinal))
                mismatches.Add(key);
        }

        return mismatches;
    }

    /// <summary>Commits values of keys a group owns through the committer of the node that leads it.</summary>
    /// <param name="leader">The elected leader of the group.</param>
    /// <param name="ownerId">The owner of the group.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The committed values by key.</returns>
    private static async Task<Dictionary<string, string>> CommitOwnedKeysAsync(ITestNodeHost leader, string ownerId, CancellationToken cancellationToken)
    {
        var locator = leader.GetRequiredService<INodeLocator>();
        var committer = leader.GetRequiredService<ReplicaGroupCommitters>().For(ownerId);
        var committed = new Dictionary<string, string>(StringComparer.Ordinal);
        var next = 0;
        while (committed.Count < LeaderOwnedKeyCount)
        {
            var key = $"{WipedCluster.CacheName}-{next++}";
            if (!string.Equals(locator.GetOwner(WipedCluster.CacheName, key), ownerId, StringComparison.Ordinal))
                continue;

            var entry = new NodeCacheEntry<object?> { Value = ElectedValue, Version = FirstVersion };
            await committer.CommitSetAsync(Guid.NewGuid().ToString("N"), WipedCluster.CacheName, key, entry, cancellationToken);
            committed[key] = ElectedValue;
        }

        return committed;
    }

    /// <summary>Asks the repaired copy for a vote in the term it holds, for another candidate and for the leader that repaired it.</summary>
    /// <param name="cluster">The cluster; each candidate sends its own request, as the node is bound to the identity of its certificate.</param>
    /// <param name="group">The group.</param>
    /// <param name="leaderId">The leader of the group.</param>
    /// <param name="voterId">The node whose repaired copy is asked.</param>
    /// <param name="leader">The status of the leader's copy, which is up to date for both candidates.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the other candidate was refused and the leader granted.</returns>
    private static async Task AssertVotesOnceAsync(
        TestCluster<IntegrationStartOptions> cluster,
        string group,
        string leaderId,
        string voterId,
        FollowerLogStatus leader,
        CancellationToken cancellationToken)
    {
        var rival = FindRival(leaderId, voterId);
        var rivalHeader = new ReplicaRpcHeader(group, leader.TopologyFingerprint, leader.ConfigurationGeneration, leader.CurrentTerm, rival, rival);
        var leaderHeader = new ReplicaRpcHeader(group, leader.TopologyFingerprint, leader.ConfigurationGeneration, leader.CurrentTerm, leaderId, leaderId);

        var refused = await cluster[rival].GetRequiredService<IReplicaVoteGateway>()
            .RequestVoteAsync(voterId, rivalHeader, leader.LastLogIndex, leader.LastLogTerm, cancellationToken);
        var granted = await cluster[leaderId].GetRequiredService<IReplicaVoteGateway>()
            .RequestVoteAsync(voterId, leaderHeader, leader.LastLogIndex, leader.LastLogTerm, cancellationToken);

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
