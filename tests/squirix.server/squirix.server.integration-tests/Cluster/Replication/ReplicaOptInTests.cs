using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
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
        await using var plain = await StartNodeAsync(GetNextHttpUri(), "n1", new NodeStartOptions { EnableReplication = false }, cancellationToken);
        _ = await Assert.That(plain.Services.GetRequiredService<FeatureState>().NetworkReplicationEnabled).IsFalse();

        await using var opted = await StartNodeAsync(GetNextHttpUri(), "n1", new NodeStartOptions { EnableReplication = true }, cancellationToken);
        _ = await Assert.That(opted.Services.GetRequiredService<FeatureState>().NetworkReplicationEnabled).IsFalse();
    }

    /// <summary>RF=2 with persistence, mTLS, and the opt-in activates network replication.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfTwoActivatesWithOptIn(CancellationToken cancellationToken)
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("n1", uriA), ("n2", uriB)]);
        var options = new NodeStartOptions { ReplicaCount = 2, UsePersistence = true, EnableReplication = true, ExtraScope = "rf2-optin-active" };
        await using var host = await StartNodeAsync(uriA, peers, options, cancellationToken);
        var featureState = host.Services.GetRequiredService<FeatureState>();
        _ = await Assert.That(featureState.NetworkReplicationEnabled).IsTrue();
        _ = host.Services.GetRequiredService<IReplicaGroupLocator>();
    }

    /// <summary>RF=2 with persistence and mTLS but without the opt-in refuses startup to name the switch.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfTwoRefusesWithoutOptIn(CancellationToken cancellationToken)
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("n1", uriA), ("n2", uriB)]);
        var task = StartNodeAsync(
            uriA,
            peers,
            new NodeStartOptions { ReplicaCount = 2, UsePersistence = true, EnableReplication = false, ExtraScope = "rf2-optin-refused" },
            cancellationToken);
        var exception = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, TestNodeHost>(task);
        _ = await Assert.That(exception.Message).Contains(ReplicationActivationGuard.OptInRequired, StringComparison.Ordinal);
    }
}
