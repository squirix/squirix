using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Explicit replication opt-in: RF&gt;1 refuses to start up without the switch.</summary>
public sealed class ReplicaOptInTests : NodeIntegrationTestBase
{
    /// <summary>RF=1 starts without the opt-in and ignores it when set.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfOneNeedsNoOptIn(CancellationToken cancellationToken)
    {
        await using var plainCluster = await StartClusterAsync("n1", new IntegrationStartOptions { EnableReplication = false }, cancellationToken);
        _ = await Assert.That(plainCluster["n1"].GetRequiredService<FeatureState>().NetworkReplicationEnabled).IsFalse();

        await using var optedCluster = await StartClusterAsync("n1", new IntegrationStartOptions { EnableReplication = true }, cancellationToken);
        _ = await Assert.That(optedCluster["n1"].GetRequiredService<FeatureState>().NetworkReplicationEnabled).IsFalse();
    }

    /// <summary>RF=2 with persistence, mTLS, and the opt-in activates network replication.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfTwoActivatesWithOptIn(CancellationToken cancellationToken)
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([new ClusterNode("n1", uriA), new ClusterNode("n2", uriB)]);
        var options = new IntegrationStartOptions { ReplicaCount = 2, UsePersistence = true, EnableReplication = true, ExtraScope = "rf2-optin-active" };
        await using var host = await StartClusterNodeAsync(uriA, peers, options, cancellationToken);
        var featureState = host.GetRequiredService<FeatureState>();
        _ = await Assert.That(featureState.NetworkReplicationEnabled).IsTrue();
        _ = host.GetRequiredService<IReplicaGroupLocator>();
    }

    /// <summary>RF=2 with persistence and mTLS but without the opt-in refuses startup to name the switch.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfTwoRefusesWithoutOptIn(CancellationToken cancellationToken)
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([new ClusterNode("n1", uriA), new ClusterNode("n2", uriB)]);
        var task = StartClusterNodeAsync(
            uriA,
            peers,
            new IntegrationStartOptions { ReplicaCount = 2, UsePersistence = true, EnableReplication = false, ExtraScope = "rf2-optin-refused" },
            cancellationToken);
        var exception = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ITestNodeHost>(task);
        _ = await Assert.That(exception.Message).Contains(ReplicationActivationGuard.OptInRequired, StringComparison.Ordinal);
    }
}
