using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>The owner of a group that comes back on an empty disk while the other members hold complete copies does not start the group again.</summary>
public sealed class WipedOwnerTests : NodeIntegrationTestBase
{
    private static readonly string[] Others = [WipedCluster.Nodes[1], WipedCluster.Nodes[2]];

    /// <summary>
    /// With automatic failover the other two members elect a leader and the group keeps working; the owner offers nobody, and the elected leader
    /// repairs its copy, which records the elected term and a vote for that leader (#1126).
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OwnerWipedGroupKeepsWorkingByElection(CancellationToken cancellationToken)
    {
        await using var cluster = await StartAsync("wiped-owner-elected", true, cancellationToken);
        var ledger = new GroupAuthorityLedger<IntegrationStartOptions>(cluster, WipedCluster.OwnerId, WipedCluster.Bound);

        var owner = await cluster.RestartWipedNodeAsync(WipedCluster.OwnerId, null, cancellationToken);
        var (leader, term) = await ledger.LeaderAsync(Others, 0UL, "the other members elect a leader of the group", cancellationToken);
        await CommitAsync(cluster[leader], cancellationToken);

        var copy = await WipedCluster.AwaitRepairedCopyAsync(owner, WipedCluster.OwnerId, leader, cancellationToken);

        // Once the leader repaired the copy, the first start of the owner may end, finding the copy complete; it never offered the group again:
        // the copy holds the elected term, the owner never led, and the other members keep their complete copies.
        _ = await Assert.That(term).IsGreaterThan(0UL);
        _ = await Assert.That(copy.CurrentTerm).IsEqualTo(term);
        _ = await Assert.That(ledger.Observe([WipedCluster.OwnerId]).Term).IsEqualTo(0UL);
        foreach (var node in Others)
            await WipedCluster.AssertCompleteCopyAsync(cluster[node], WipedCluster.OwnerId, cancellationToken);
    }

    /// <summary>Without automatic failover the owner refuses the keys of its group as unavailable, and the groups of the other nodes are not affected.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OwnerWipedRefusesKeysWithoutFailover(CancellationToken cancellationToken)
    {
        await using var cluster = await StartAsync("wiped-owner-static", false, cancellationToken);
        await WipedCluster.AwaitReadyAsync(cluster, WipedCluster.Nodes, cancellationToken);

        var owner = await cluster.RestartWipedNodeAsync(WipedCluster.OwnerId, null, cancellationToken);

        await WipedCluster.AssertRefusesWriteAsync(owner, WipedCluster.OwnerId, cancellationToken);
        await WipedCluster.AssertUnseededAsync(owner, cancellationToken);
        _ = await Assert.That(FirstStartAsync(owner).IsCompleted).IsFalse();
        foreach (var node in Others)
        {
            await WipedCluster.WriteAndReadAsync(cluster[node], node, cancellationToken);
            await WipedCluster.AssertCompleteCopyAsync(cluster[node], node, cancellationToken);
        }
    }

    /// <summary>Commits a write of the group under test through the committer of the node that leads it.</summary>
    /// <param name="leader">The elected leader of the group.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the write is committed.</returns>
    private static Task CommitAsync(ITestNodeHost leader, CancellationToken cancellationToken)
    {
        var key = leader.FindKeyOwnedBy(WipedCluster.CacheName, WipedCluster.OwnerId);
        var entry = new NodeCacheEntry<object?> { Value = "elected", Version = 1 };
        return leader.GetRequiredService<ReplicaGroupCommitters>().For(WipedCluster.OwnerId).CommitSetAsync(Guid.NewGuid().ToString("N"), WipedCluster.CacheName, key, entry, cancellationToken);
    }

    /// <summary>Gets the first start of the group a node owns.</summary>
    /// <param name="host">The node.</param>
    /// <returns>The task of the first start, which ends once the owner completed its copy.</returns>
    /// <exception cref="InvalidOperationException">The node runs no first start.</exception>
    private static Task FirstStartAsync(ITestNodeHost host)
    {
        foreach (var service in host.GetRequiredService<IEnumerable<IHostedService>>())
        {
            if (service is ReplicaGroupBootstrapService { ExecuteTask: { } start })
                return start;
        }

        throw new InvalidOperationException("The node runs no first start of the group it owns.");
    }

    /// <summary>Starts the three-node cluster and waits for the owner of the group under test to lead it.</summary>
    /// <param name="scope">The scope of the data directories.</param>
    /// <param name="failover">Whether automatic failover elects the leaders of the groups.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The started cluster.</returns>
    private async ValueTask<TestCluster<IntegrationStartOptions>> StartAsync(string scope, bool failover, CancellationToken cancellationToken)
    {
        var cluster = await StartClusterAsync(WipedCluster.Nodes[0], WipedCluster.Nodes[1], WipedCluster.Nodes[2], WipedCluster.Options(scope, failover), cancellationToken, scope);
        try
        {
            if (failover)
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
