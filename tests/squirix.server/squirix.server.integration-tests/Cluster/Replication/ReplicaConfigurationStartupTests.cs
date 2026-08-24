using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Node startup coverage for ReplicaCount activation guards.</summary>
public sealed class ReplicaConfigurationStartupTests : NodeIntegrationTestBase
{
    /// <summary>RF=1 starts with planning services and network replication disabled.</summary>
    [Fact]
    public async Task RfOneStartsWithoutReplicationServices()
    {
        var uri = GetNextHttpUri();
        await using var host = await StartNodeAsync(uri, "n1");
        var featureState = host.Services.GetRequiredService<FeatureState>();
        Assert.False(featureState.NetworkReplicationEnabled);
        _ = host.Services.GetRequiredService<IReplicaGroupLocator>();
    }

    /// <summary>RF=2 fails before replication activation even with persistence and mTLS.</summary>
    [Fact]
    public async Task RfTwoFailsBeforeReplicationActivation()
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("n1", uriA), ("n2", uriB)]);
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, TestNodeHost>(
            StartNodeAsync(
                uriA,
                peers,
                new NodeStartOptions
                {
                    ReplicaCount = 2,
                    UsePersistence = true,
                    ExtraScope = "rf2-activation",
                }));
        Assert.Contains(ReplicationActivationGuard.NotActivated, ex.Message, StringComparison.Ordinal);
    }

    /// <summary>RF=2 without persistence reports the persistence prerequisite first.</summary>
    [Fact]
    public async Task RfTwoReportsMissingPersistenceFirst()
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("n1", uriA), ("n2", uriB)]);
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, TestNodeHost>(StartNodeAsync(uriA, peers, new NodeStartOptions { ReplicaCount = 2 }));
        Assert.Contains(ReplicationActivationGuard.PersistenceRequired, ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(ReplicationActivationGuard.NotActivated, ex.Message, StringComparison.Ordinal);
    }

    /// <summary>RF=2 with persistence but without mTLS reports mTLS before activation refusal.</summary>
    [Fact]
    public async Task RfTwoRequiresMtlsBeforeActivation()
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
                    PersistenceOptions = new Storage.PersistenceOptions { DataDir = dataDir.Path },
                    MtlsOptions = new MtlsOptions(),
                },
                DefaultCancellationToken));
        Assert.Contains(ReplicationActivationGuard.MtlsRequired, ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(ReplicationActivationGuard.NotActivated, ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Settings JSON round-trips ReplicaCount and ConfigurationGeneration.</summary>
    [Fact]
    public async Task SettingsRoundTripReplicaCountGeneration()
    {
        using var dir = new TempDirectory("squirix-rf-settings-roundtrip");
        var path = Path.Join(dir.Path, "Squirix.settings.json");
        const string json =
            "{\"Squirix\":{\"Cluster\":{\"ClusterId\":\"c1\",\"NodeId\":\"n1\",\"Uri\":\"https://localhost:6001\",\"ReplicaCount\":1,\"ConfigurationGeneration\":7,\"Peers\":[{\"NodeId\":\"n1\",\"Uri\":\"https://localhost:6001\"},{\"NodeId\":\"n2\",\"Uri\":\"https://localhost:6002\"},{\"NodeId\":\"n3\",\"Uri\":\"https://localhost:6003\"}]}}}";
        await File.WriteAllTextAsync(path, json, DefaultCancellationToken);

        var options = await Configurator.LoadFromFileAsync(path, DefaultCancellationToken);
        Assert.Equal(1, options.ReplicaCount);
        Assert.Equal(7u, options.ConfigurationGeneration);
    }
}
