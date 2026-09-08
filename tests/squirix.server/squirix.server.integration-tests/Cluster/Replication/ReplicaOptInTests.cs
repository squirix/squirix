using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Xunit;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Explicit replication opt-in: RF&gt;1 refuses to start up without the switch.</summary>
public sealed class ReplicaOptInTests : NodeIntegrationTestBase
{
    /// <summary>RF=2 with persistence and mTLS but without the opt-in refuses startup to name the switch.</summary>
    [Fact]
    public async Task RfTwoRefusesWithoutOptIn()
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("n1", uriA), ("n2", uriB)]);
        var task = StartNodeAsync(uriA, peers, new NodeStartOptions { ReplicaCount = 2, UsePersistence = true, EnableReplication = false, ExtraScope = "rf2-optin-refused" });
        var exception = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, TestNodeHost>(task);
        Assert.Contains(ReplicationActivationGuard.OptInRequired, exception.Message, StringComparison.Ordinal);
    }

    /// <summary>RF=2 with persistence, mTLS, and the opt-in activates network replication.</summary>
    [Fact]
    public async Task RfTwoActivatesWithOptIn()
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("n1", uriA), ("n2", uriB)]);
        var options = new NodeStartOptions { ReplicaCount = 2, UsePersistence = true, EnableReplication = true, ExtraScope = "rf2-optin-active" };
        await using var host = await StartNodeAsync(uriA, peers, options);
        var featureState = host.Services.GetRequiredService<FeatureState>();
        Assert.True(featureState.NetworkReplicationEnabled);
        _ = host.Services.GetRequiredService<IReplicaGroupLocator>();
    }

    /// <summary>RF=1 starts without the opt-in and ignores it when set.</summary>
    [Fact]
    public async Task RfOneNeedsNoOptIn()
    {
        await using var plain = await StartNodeAsync(GetNextHttpUri(), "n1", new NodeStartOptions { EnableReplication = false });
        Assert.False(plain.Services.GetRequiredService<FeatureState>().NetworkReplicationEnabled);

        await using var opted = await StartNodeAsync(GetNextHttpUri(), "n1", new NodeStartOptions { EnableReplication = true });
        Assert.False(opted.Services.GetRequiredService<FeatureState>().NetworkReplicationEnabled);
    }
}
