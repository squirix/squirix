using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
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
    /// <summary>Bounds every election wait; the timing below elects within seconds, the rest absorbs a loaded machine.</summary>
    private static readonly TimeSpan ElectionBound = TimeSpan.FromSeconds(90);

    private static readonly TestElectionTiming ElectionTiming = new() { JitterSeed = 11UL };

    /// <summary>A node started with automatic failover on and quorum reads off refuses to start and names the missing switch.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailoverPairRefusedAtStartup(CancellationToken cancellationToken)
    {
        await using var cluster = CreateCluster([new ClusterNode("n1", GetNextHttpUri()), new ClusterNode("n2", GetNextHttpUri()), new ClusterNode("n3", GetNextHttpUri())]);
        var ex = await NodeAsyncAssert.ThrowsAsync<OptionsValidationException, ITestNodeHost>(
            cluster.StartNodeAsync(
                "n1",
                new IntegrationStartOptions { ReplicaCount = 3, UsePersistence = true, AutomaticFailoverEnabled = true, ExtraScope = "failover-pair" },
                cancellationToken));
        _ = await Assert.That(ex.Message).Contains(TopologyValidator.AutomaticFailoverRequiresQuorumReads, StringComparison.Ordinal);
    }

    /// <summary>
    /// Three nodes configured through the server options with both switches on elect a new leader when the leader of a group stops; the
    /// node configuration is the cluster configuration the hosting path maps from those options.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublicSwitchesElectLeader(CancellationToken cancellationToken)
    {
        ClusterNode[] nodes = [new("node-a", GetNextHttpUri()), new("node-b", GetNextHttpUri()), new("node-c", GetNextHttpUri())];
        string[] ids = [nodes[0].NodeId, nodes[1].NodeId, nodes[2].NodeId];
        var peers = new SquirixServerPeerOptions[nodes.Length];
        for (var i = 0; i < nodes.Length; i++)
            peers[i] = new SquirixServerPeerOptions { NodeId = nodes[i].NodeId, Uri = nodes[i].Uri };
        var serverOptions = new SquirixServerOptions
        {
            NodeId = nodes[0].NodeId,
            Uri = nodes[0].Uri,
            Peers = peers,
            ReplicaCount = 3,
            ReplicationEnabled = true,
            AutomaticFailoverEnabled = true,
            QuorumReadsEnabled = true,
        };
        serverOptions.UsePersistence();
        var topology = Configurator.ToClusterConfig(serverOptions);
        var startOptions = new IntegrationStartOptions
        {
            ReplicaCount = topology.ReplicaCount,
            UsePersistence = true,
            ExtraScope = "public-switches",
            AutomaticFailoverEnabled = topology.AutomaticFailoverEnabled,
            QuorumReadsEnabled = topology.QuorumReadsEnabled,
            ElectionTiming = ElectionTiming,
        };

        await using var cluster = await StartClusterAsync(nodes, startOptions, cancellationToken);
        var probe = new ClusterLeaderProbe<IntegrationStartOptions>(cluster);
        var (former, formerTerm) = await probe.WaitForStableLeaderAsync(ids[0], ids, ElectionBound, cancellationToken);
        await cluster.StopNodeAsync(former);
        var (next, nextTerm) = await probe.WaitForNewLeaderAsync(ids[0], formerTerm, ElectionBound, cancellationToken);

        _ = await Assert.That(next).IsNotEqualTo(former);
        _ = await Assert.That(nextTerm).IsGreaterThanOrEqualTo(2UL);
    }

    /// <summary>RF=1 starts with planning services and network replication disabled.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfOneStartsWithoutReplicationServices(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("n1", cancellationToken: cancellationToken);
        var host = cluster["n1"];
        var featureState = host.GetRequiredService<FeatureState>();
        _ = await Assert.That(featureState.NetworkReplicationEnabled).IsFalse();
        _ = host.GetRequiredService<IReplicaGroupLocator>();
    }

    /// <summary>RF=2 without persistence reports the persistence prerequisite first.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfTwoReportsMissingPersistenceFirst(CancellationToken cancellationToken)
    {
        await using var cluster = CreateCluster([new ClusterNode("n1", GetNextHttpUri()), new ClusterNode("n2", GetNextHttpUri())]);
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ITestNodeHost>(
            cluster.StartNodeAsync("n1", new IntegrationStartOptions { ReplicaCount = 2 }, cancellationToken));
        _ = await Assert.That(ex.Message).Contains(ReplicationActivationGuard.PersistenceRequired, StringComparison.Ordinal);
    }

    /// <summary>RF=2 with persistence but without mTLS reports the mTLS prerequisite.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfTwoRequiresMtlsBeforeActivation(CancellationToken cancellationToken)
    {
        await using var cluster = CreateCluster([new ClusterNode("n1", GetNextHttpUri()), new ClusterNode("n2", GetNextHttpUri())]);
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ITestNodeHost>(
            cluster.StartNodeAsync(
                "n1",
                new IntegrationStartOptions { ReplicaCount = 2, UsePersistence = true, OmitClusterMtls = true, ExtraScope = "rf2-mtls" },
                cancellationToken));
        _ = await Assert.That(ex.Message).Contains(ReplicationActivationGuard.MtlsRequired, StringComparison.Ordinal);
    }

    /// <summary>RF=2 starts with network replication activated when persistence and mTLS are present.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfTwoStartsWithPrerequisites(CancellationToken cancellationToken)
    {
        await using var cluster = CreateCluster([new ClusterNode("n1", GetNextHttpUri()), new ClusterNode("n2", GetNextHttpUri())]);
        var host = await cluster.StartNodeAsync(
            "n1",
            new IntegrationStartOptions
            {
                ReplicaCount = 2,
                UsePersistence = true,
                ExtraScope = "rf2-activation",
            },
            cancellationToken);
        var featureState = host.GetRequiredService<FeatureState>();
        _ = await Assert.That(featureState.NetworkReplicationEnabled).IsTrue();
        _ = host.GetRequiredService<IReplicaGroupLocator>();
    }

    /// <summary>Settings JSON round-trips the automatic failover and quorum read switches into the cluster configuration.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SettingsRoundTripFailoverSwitches(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-failover-settings-roundtrip");
        var path = Path.Join(dir, "Squirix.settings.json");
        const string json =
            "{\"Squirix\":{\"Cluster\":{\"ClusterId\":\"c1\",\"NodeId\":\"n1\",\"Uri\":\"https://localhost:6001\",\"ReplicaCount\":3,\"PersistenceEnabled\":true," +
            "\"AutomaticFailoverEnabled\":true,\"QuorumReadsEnabled\":true," +
            "\"Peers\":[{\"NodeId\":\"n1\",\"Uri\":\"https://localhost:6001\"},{\"NodeId\":\"n2\",\"Uri\":\"https://localhost:6002\"},{\"NodeId\":\"n3\",\"Uri\":\"https://localhost:6003\"}]}}}";
        await File.WriteAllTextAsync(path, json, cancellationToken);

        var options = await Configurator.LoadAsync(path, cancellationToken);
        var copy = new SquirixServerOptions();
        Configurator.CopyOptions(options, copy);
        var topology = Configurator.ToClusterConfig(copy);

        _ = await Assert.That((options.AutomaticFailoverEnabled, options.QuorumReadsEnabled)).IsEqualTo((true, true));
        _ = await Assert.That((topology.AutomaticFailoverEnabled, topology.QuorumReadsEnabled)).IsEqualTo((true, true));
    }

    /// <summary>Settings JSON round-trips ReplicaCount and ConfigurationGeneration.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SettingsRoundTripReplicaCountGeneration(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-rf-settings-roundtrip");
        var path = Path.Join(dir, "Squirix.settings.json");
        const string json =
            "{\"Squirix\":{\"Cluster\":{\"ClusterId\":\"c1\",\"NodeId\":\"n1\",\"Uri\":\"https://localhost:6001\",\"ReplicaCount\":1,\"ConfigurationGeneration\":7,\"Peers\":[{\"NodeId\":\"n1\",\"Uri\":\"https://localhost:6001\"},{\"NodeId\":\"n2\",\"Uri\":\"https://localhost:6002\"},{\"NodeId\":\"n3\",\"Uri\":\"https://localhost:6003\"}]}}}";
        await File.WriteAllTextAsync(path, json, cancellationToken);

        var options = await Configurator.LoadAsync(path, cancellationToken);
        _ = await Assert.That(options.ReplicaCount).IsEqualTo(1);
        _ = await Assert.That(options.ConfigurationGeneration).IsEqualTo(7u);
    }
}
