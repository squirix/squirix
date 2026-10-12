using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Members of a running group that come back on an empty disk hold unseeded copies: they grant no vote and take no entries.</summary>
public sealed class UnseededMemberTests : NodeIntegrationTestBase
{
    private const string Follower = "node-c";
    private const string OtherFollower = "node-b";

    private static readonly string[] Survivors = [WipedCluster.OwnerId, OtherFollower];

    /// <summary>A follower restarted on an empty disk is not ready, holds an unseeded copy at term zero, refuses a vote and a pre-vote, and the group keeps committing on the other two.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WipedFollowerStaysUnseeded(CancellationToken cancellationToken)
    {
        await using var cluster = await StartAsync("unseeded-follower", cancellationToken);
        var owner = cluster[WipedCluster.OwnerId];
        await WipedCluster.WriteAndReadAsync(owner, WipedCluster.OwnerId, cancellationToken);

        var wiped = await cluster.RestartWipedNodeAsync(Follower, null, cancellationToken);

        await WipedCluster.AssertUnseededAsync(wiped, cancellationToken);
        await WipedCluster.AssertUnseededCopyAsync(wiped, WipedCluster.OwnerId, cancellationToken);
        await AssertVoteRefusedAsync(owner, cancellationToken);
        await WipedCluster.WriteAndReadAsync(owner, WipedCluster.OwnerId, cancellationToken);
        await WipedCluster.AssertUnseededCopyAsync(wiped, WipedCluster.OwnerId, cancellationToken);
    }

    /// <summary>With a follower still unseeded, stopping the leader leaves no leader; the group has a leader again once the stopped leader returns.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnseededFollowerGivesNoVote(CancellationToken cancellationToken)
    {
        await using var cluster = await StartAsync("unseeded-no-vote", cancellationToken);
        var ledger = new GroupAuthorityLedger<IntegrationStartOptions>(cluster, WipedCluster.OwnerId, WipedCluster.Bound);
        _ = await cluster.RestartWipedNodeAsync(Follower, null, cancellationToken);
        await cluster.StopNodeAsync(WipedCluster.OwnerId);

        await ledger.HoldsAsync(() => ledger.Observe().Term == 0UL, WipedCluster.Timing.Round * 2, "no node leads the group without the vote of the unseeded copy", cancellationToken);
        _ = await cluster.StartNodeAsync(WipedCluster.OwnerId, null, cancellationToken);
        var (leader, term) = await ledger.LeaderAsync(Survivors, 0UL, "the returned owner and its peer elect a leader", cancellationToken);

        _ = await Assert.That(term).IsGreaterThan(0UL);
        _ = await Assert.That(leader).IsNotEqualTo(Follower);
        await WipedCluster.AssertUnseededCopyAsync(cluster[Follower], WipedCluster.OwnerId, cancellationToken);
    }

    /// <summary>Two members of three wiped: the group has no leader and refuses writes, the surviving copy stays complete, and the wiped copies stay unseeded.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TwoWipedMembersLeaveNoLeader(CancellationToken cancellationToken)
    {
        await using var cluster = await StartAsync("unseeded-two-wiped", cancellationToken);
        var ledger = new GroupAuthorityLedger<IntegrationStartOptions>(cluster, WipedCluster.OwnerId, WipedCluster.Bound);

        var second = await cluster.RestartWipedNodeAsync(OtherFollower, null, cancellationToken);
        var third = await cluster.RestartWipedNodeAsync(Follower, null, cancellationToken);
        await ledger.UntilAsync(() => ledger.Observe().Term == 0UL, "the owner loses authority without a majority", cancellationToken);
        await ledger.HoldsAsync(() => ledger.Observe().Term == 0UL, WipedCluster.Timing.Round * 2, "no node leads a group of two unseeded copies", cancellationToken);

        await WipedCluster.AssertRefusesWriteAsync(cluster[WipedCluster.OwnerId], WipedCluster.OwnerId, cancellationToken);
        await WipedCluster.AssertCompleteCopyAsync(cluster[WipedCluster.OwnerId], WipedCluster.OwnerId, cancellationToken);
        await WipedCluster.AssertUnseededCopyAsync(second, WipedCluster.OwnerId, cancellationToken);
        await WipedCluster.AssertUnseededCopyAsync(third, WipedCluster.OwnerId, cancellationToken);
        await WipedCluster.AssertUnseededAsync(second, cancellationToken);
        await WipedCluster.AssertUnseededAsync(third, cancellationToken);
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
