using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Node startup coverage for ReplicaCount activation guards.</summary>
public sealed class ReplicaConfigurationStartupTests : NodeIntegrationTestBase
{
    /// <summary>RF=1 starts with planning services and network replication disabled.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfOneStartsWithoutReplicationServices(CancellationToken cancellationToken)
    {
        var uri = GetNextHttpUri();
        await using var host = await StartNodeAsync(uri, "n1", cancellationToken: cancellationToken);
        var featureState = host.Services.GetRequiredService<FeatureState>();
        _ = await Assert.That(featureState.NetworkReplicationEnabled).IsFalse();
        _ = host.Services.GetRequiredService<IReplicaGroupLocator>();
    }

    /// <summary>RF=2 without persistence reports the persistence prerequisite first.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfTwoReportsMissingPersistenceFirst(CancellationToken cancellationToken)
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("n1", uriA), ("n2", uriB)]);
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, TestNodeHost>(
            StartNodeAsync(uriA, peers, new NodeStartOptions { ReplicaCount = 2 }, cancellationToken));
        _ = await Assert.That(ex.Message).Contains(ReplicationActivationGuard.PersistenceRequired, StringComparison.Ordinal);
    }

    /// <summary>RF=2 with persistence but without mTLS reports the mTLS prerequisite.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfTwoRequiresMtlsBeforeActivation(CancellationToken cancellationToken)
    {
        var uri = GetNextHttpUri();
        var peers = new[]
        {
            new ServerPeer { NodeId = "n1", Uri = uri },
            new ServerPeer { NodeId = "n2", Uri = GetNextHttpUri() },
        };
        using var dataDir = new TempDirectory("squirix-rf2-mtls");
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(
            NodeHost.StartAsync(
                new TopologyOptions(peers)
                {
                    ClusterId = "c1",
                    NodeId = "n1",
                    Uri = uri,
                    ReplicaCount = 2,
                },
                new NodeHostStartOptions
                {
                    PersistenceOptions = new PersistenceOptions { DataDir = dataDir.Path },
                    MtlsOptions = new MtlsOptions(),
                },
                cancellationToken));
        _ = await Assert.That(ex.Message).Contains(ReplicationActivationGuard.MtlsRequired, StringComparison.Ordinal);
    }

    /// <summary>RF=2 starts with network replication activated when persistence and mTLS are present.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfTwoStartsWithPrerequisites(CancellationToken cancellationToken)
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("n1", uriA), ("n2", uriB)]);
        await using var host = await StartNodeAsync(
            uriA,
            peers,
            new NodeStartOptions
            {
                ReplicaCount = 2,
                UsePersistence = true,
                ExtraScope = "rf2-activation",
            },
            cancellationToken);
        var featureState = host.Services.GetRequiredService<FeatureState>();
        _ = await Assert.That(featureState.NetworkReplicationEnabled).IsTrue();
        _ = host.Services.GetRequiredService<IReplicaGroupLocator>();
    }

    /// <summary>Settings JSON round-trips ReplicaCount and ConfigurationGeneration.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SettingsRoundTripReplicaCountGeneration(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-rf-settings-roundtrip");
        var path = Path.Join(dir.Path, "Squirix.settings.json");
        const string json =
            "{\"Squirix\":{\"Cluster\":{\"ClusterId\":\"c1\",\"NodeId\":\"n1\",\"Uri\":\"https://localhost:6001\",\"ReplicaCount\":1,\"ConfigurationGeneration\":7,\"Peers\":[{\"NodeId\":\"n1\",\"Uri\":\"https://localhost:6001\"},{\"NodeId\":\"n2\",\"Uri\":\"https://localhost:6002\"},{\"NodeId\":\"n3\",\"Uri\":\"https://localhost:6003\"}]}}}";
        await File.WriteAllTextAsync(path, json, cancellationToken);

        var options = await Configurator.LoadAsync(path, cancellationToken);
        _ = await Assert.That(options.ReplicaCount).IsEqualTo(1);
        _ = await Assert.That(options.ConfigurationGeneration).IsEqualTo(7u);
    }
}
