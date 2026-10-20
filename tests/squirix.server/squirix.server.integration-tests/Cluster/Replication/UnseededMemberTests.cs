using System;
using System.Threading;
using System.Threading.Tasks;
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
/// Members of a running group that come back on an empty disk hold unseeded copies, which grant no vote and take no entries, until a leader that
/// read the term ceiling from a majority without them repairs them (#1126).
/// </summary>
[ExecutionPriority(Priority.High)]
public sealed class UnseededMemberTests : NodeIntegrationTestBase
{
    private const string Follower = "node-c";
    private const string OtherFollower = "node-b";

    private static readonly string[] Survivors = [WipedCluster.OwnerId, OtherFollower];

    /// <summary>
    /// A follower restarted on an empty disk is repaired by the owner without an operator: its copy ends complete at the owner's term with its
    /// vote for the owner, holds the value written before the loss, and takes the writes that follow.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WipedFollowerIsRepaired(CancellationToken cancellationToken)
    {
        await using var cluster = await StartAsync("unseeded-repaired", cancellationToken);
        var owner = cluster[WipedCluster.OwnerId];
        var key = owner.FindKeyOwnedBy(WipedCluster.CacheName, WipedCluster.OwnerId);
        var cache = owner.GetCache<object?>(WipedCluster.CacheName);
        await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), WipedCluster.CacheName, key, new NodeCacheEntry<object?> { Value = "before", Version = 1 }, cancellationToken);

        var wiped = await cluster.RestartWipedNodeAsync(Follower, null, cancellationToken);
        var copy = await WipedCluster.AwaitRepairedCopyAsync(wiped, WipedCluster.OwnerId, WipedCluster.OwnerId, cancellationToken);
        var leader = await WipedCluster.Log(owner, WipedCluster.OwnerId).GetStatusAsync(cancellationToken);
        var local = wiped.GetRequiredService<PhysicalCache<object?>>();
        var cacheKey = new CacheKey(WipedCluster.CacheName, key);
        var repaired = (await local.GetValueAsync(cacheKey, cancellationToken)).Value;
        await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), WipedCluster.CacheName, key, new NodeCacheEntry<object?> { Value = "after", Version = 2 }, cancellationToken);

        _ = await Assert.That(copy.CurrentTerm).IsEqualTo(leader.CurrentTerm);
        _ = await Assert.That(repaired).IsEqualTo("before");
        await (Local: local, Key: cacheKey).WaitUntilValueAsync(
            static async (state, token) => string.Equals((await state.Local.GetValueAsync(state.Key, token)).Value as string, "after", StringComparison.Ordinal),
            WipedCluster.Bound,
            TimeProvider.System,
            cancellationToken);
    }

    /// <summary>
    /// A follower wiped while the owner is down gives no vote: the remaining member cannot elect a leader with it. Once the owner returns, a leader
    /// is elected among the members that kept their copies, and it repairs the wiped one with its vote.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnseededFollowerGivesNoVote(CancellationToken cancellationToken)
    {
        const string scope = "unseeded-no-vote";
        await using var cluster = await StartAsync(scope, cancellationToken);
        var ledger = new GroupAuthorityLedger<IntegrationStartOptions>(cluster, WipedCluster.OwnerId, WipedCluster.Bound);
        await cluster.WipeNodeAsync(Follower);
        await cluster.StopNodeAsync(WipedCluster.OwnerId);
        var wiped = await cluster.StartWipedNodeAsync(Follower, null, cancellationToken);

        await ledger.HoldsAsync(() => ledger.Observe().Term == 0UL, WipedCluster.Timing.Round * 2, "no node leads the group without the vote of the unseeded copy", cancellationToken);
        _ = await cluster.StartNodeAsync(WipedCluster.OwnerId, WipedCluster.Options(scope, true, null, false), cancellationToken);
        var (leader, term) = await ledger.LeaderAsync(Survivors, 0UL, "the returned owner and its peer elect a leader", cancellationToken);

        _ = await Assert.That(term).IsGreaterThan(0UL);
        _ = await Assert.That(leader).IsNotEqualTo(Follower);
        _ = await WipedCluster.AwaitRepairedCopyAsync(wiped, WipedCluster.OwnerId, leader, cancellationToken);
    }

    /// <summary>
    /// Two members of three wiped: the group has no leader and refuses writes, the surviving copy stays complete, and the wiped copies stay unseeded
    /// and refuse votes, since no member that kept its copy is left to give the term ceiling.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TwoWipedMembersLeaveNoLeader(CancellationToken cancellationToken)
    {
        await using var cluster = await StartAsync("unseeded-two-wiped", cancellationToken);
        var ledger = new GroupAuthorityLedger<IntegrationStartOptions>(cluster, WipedCluster.OwnerId, WipedCluster.Bound);

        // Both are wiped before either comes back, so the owner never reads a term ceiling from a member that kept its copy.
        await cluster.WipeNodeAsync(OtherFollower);
        await cluster.WipeNodeAsync(Follower);
        var second = await cluster.StartWipedNodeAsync(OtherFollower, null, cancellationToken);
        var third = await cluster.StartWipedNodeAsync(Follower, null, cancellationToken);
        await ledger.UntilAsync(() => ledger.Observe().Term == 0UL, "the owner loses authority without a majority", cancellationToken);
        await ledger.HoldsAsync(() => ledger.Observe().Term == 0UL, WipedCluster.Timing.Round * 2, "no node leads a group of two unseeded copies", cancellationToken);

        await WipedCluster.AssertRefusesWriteAsync(cluster[WipedCluster.OwnerId], WipedCluster.OwnerId, cancellationToken);
        await WipedCluster.AssertCompleteCopyAsync(cluster[WipedCluster.OwnerId], WipedCluster.OwnerId, cancellationToken);
        await WipedCluster.AssertUnseededCopyAsync(second, WipedCluster.OwnerId, cancellationToken);
        await WipedCluster.AssertUnseededCopyAsync(third, WipedCluster.OwnerId, cancellationToken);
        await WipedCluster.AssertUnseededAsync(second, cancellationToken);
        await WipedCluster.AssertUnseededAsync(third, cancellationToken);
        await AssertVoteRefusedAsync(cluster[WipedCluster.OwnerId], cancellationToken);
    }

    /// <summary>Sends a pre-vote and a vote of the leader of the group to the unseeded copy of the follower.</summary>
    /// <param name="candidate">The node that leads the group.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once both were refused as unseeded.</returns>
    private static async Task AssertVoteRefusedAsync(ITestNodeHost candidate, CancellationToken cancellationToken)
    {
        var own = await WipedCluster.Log(candidate, WipedCluster.OwnerId).GetStatusAsync(cancellationToken);
        var header = new ReplicaRpcHeader(WipedCluster.OwnerId, own.TopologyFingerprint, own.ConfigurationGeneration, own.CurrentTerm + 2UL, WipedCluster.OwnerId, WipedCluster.OwnerId);
        var gateway = candidate.GetRequiredService<IReplicaVoteGateway>();

        var preVote = await gateway.PreVoteAsync(Follower, header, own.LastLogIndex, own.LastLogTerm, cancellationToken);
        var vote = await gateway.RequestVoteAsync(Follower, header, own.LastLogIndex, own.LastLogTerm, cancellationToken);

        _ = await Assert.That(preVote).IsEqualTo(new FollowerLogVoteResult(false, RefusalCodes.Unseeded, 0UL));
        _ = await Assert.That(vote).IsEqualTo(new FollowerLogVoteResult(false, RefusalCodes.Unseeded, 0UL));
    }

    /// <summary>Starts the three-node cluster with automatic failover and waits for the owner to lead its group.</summary>
    /// <param name="scope">The scope of the data directories.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The started cluster.</returns>
    private async ValueTask<TestCluster<IntegrationStartOptions>> StartAsync(string scope, CancellationToken cancellationToken)
    {
        var cluster = await StartClusterAsync(WipedCluster.Nodes[0], WipedCluster.Nodes[1], WipedCluster.Nodes[2], WipedCluster.Options(scope, true), cancellationToken, scope);
        try
        {
            await WipedCluster.AwaitOwnerLeadsAsync(cluster, cancellationToken);
            return cluster;
        }
        catch
        {
            await cluster.DisposeAsync();
            throw;
        }
    }
}
