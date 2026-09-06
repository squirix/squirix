using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Xunit;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Activated topology agreement across restarts: matching identity stays ready, changes are refused.</summary>
public sealed class TopologyAgreementTests : NodeIntegrationTestBase
{
    /// <summary>A restart with the same activated identity starts and its group logs stay ready.</summary>
    [Fact(DisplayName = "TopologyAgreementTests.RestartWithMatchingMajorityAllowsReadiness")]
    public async Task RestartMatchingAllowsReadiness()
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("n1", uriA), ("n2", uriB)]);
        var options = new NodeStartOptions { ReplicaCount = 2, UsePersistence = true, ExtraScope = "topology-restart" };
        var nodeA = await StartNodeAsync(uriA, peers, options);
        await using var nodeB = await StartNodeAsync(uriB, peers, options);
        await nodeA.DisposeAsync();

        await using var restarted = await StartNodeAsync(
            uriA,
            peers,
            new NodeStartOptions { ReplicaCount = 2, UsePersistence = true, CleanTestDir = false, ExtraScope = "topology-restart" });
        var registry = restarted.Services.GetRequiredService<ReplicaGroupRegistry>();

        Assert.True(registry.TryGetLog("n1", out var log) && log != null);
        var status = await log.GetStatusAsync(DefaultCancellationToken);
        Assert.Equal(FollowerLogReadiness.Ready, status.Readiness);
    }

    /// <summary>A restart with a new generation and no offline bootstrap is refused at startup.</summary>
    [Fact(DisplayName = "TopologyAgreementTests.GenerationChangeWithoutBootstrapIsRejected")]
    public async Task GenerationChangeRejectedWithoutBootstrap()
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("n1", uriA), ("n2", uriB)]);
        var options = new NodeStartOptions { ReplicaCount = 2, UsePersistence = true, ExtraScope = "topology-generation" };
        var nodeA = await StartNodeAsync(uriA, peers, options);
        await using var nodeB = await StartNodeAsync(uriB, peers, options);
        await nodeA.DisposeAsync();

        var exception = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, TestNodeHost>(
            StartNodeAsync(
                uriA,
                peers,
                new NodeStartOptions { ReplicaCount = 2, UsePersistence = true, CleanTestDir = false, ExtraScope = "topology-generation", ConfigurationGeneration = 2 }));

        Assert.Contains("offline bootstrap", exception.Message, System.StringComparison.Ordinal);
    }
}
