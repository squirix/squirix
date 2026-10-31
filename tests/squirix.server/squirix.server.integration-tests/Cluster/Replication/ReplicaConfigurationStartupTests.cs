using System;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Grpc.AspNetCore.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Mtls;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Node startup coverage for ReplicaCount activation guards.</summary>
public sealed class ReplicaConfigurationStartupTests : NodeIntegrationTestBase
{
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
    /// The public hosting path maps both switches from the server options into the node topology, and with them on registers one more
    /// hosted service, the election service; the host is built but not started, so storage stays closed. The cluster mTLS material comes from the testkit instead of process environment variables; nothing else differs from
    /// <see cref="AspNetCoreExtensions.AddSquirixServerAsync" />.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HostPathRegistersElection(CancellationToken cancellationToken)
    {
        // The host is built but never started, so nothing binds these URIs and no listen port is held for them.
        ClusterNode[] nodes =
        [
            new("node-a", new Uri("https://localhost:6001")),
            new("node-b", new Uri("https://localhost:6002")),
            new("node-c", new Uri("https://localhost:6003")),
        ];

        var (onTopology, onHosted) = await BuildHostAsync(nodes, true, cancellationToken);
        var (offTopology, offHosted) = await BuildHostAsync(nodes, false, cancellationToken);

        _ = await Assert.That((onTopology.AutomaticFailoverEnabled, onTopology.QuorumReadsEnabled)).IsEqualTo((true, true));
        _ = await Assert.That((offTopology.AutomaticFailoverEnabled, offTopology.QuorumReadsEnabled)).IsEqualTo((false, false));
        _ = await Assert.That(onHosted - offHosted).IsEqualTo(1);
    }

    /// <summary>RF=1 starts with planning services and network replication disabled.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfOneStartsWithoutReplicationServices(CancellationToken cancellationToken)
    {
        await using var cluster = CreateCluster([new ClusterNode("n1", GetNextHttpUri()), new ClusterNode("n2", GetNextHttpUri())]);
        var host = await cluster.StartNodeAsync("n1", new IntegrationStartOptions(), cancellationToken);
        var featureState = host.GetRequiredService<FeatureState>();
        _ = await Assert.That(featureState.NetworkReplicationEnabled).IsFalse();
        _ = await Assert.That(host.GetRequiredService<MtlsCertificate>().Enabled).IsTrue();
        _ = await Assert.That(host.GetRequiredService<MtlsOptions>().InternalListenPort > 0).IsTrue();
        _ = await Assert.That(host.Services.GetService<SquirixReplicationServiceAdapter>()).IsNull();
        _ = await Assert.That(HasReplicationEndpoint(host)).IsFalse();
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
        _ = await Assert.That(host.Services.GetService<SquirixReplicationServiceAdapter>()).IsNotNull();
        _ = await Assert.That(HasReplicationEndpoint(host)).IsTrue();
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

    /// <summary>The public RF1 hosting path cannot activate the test-only replication mode.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublicHostDisablesFoundationMode(CancellationToken cancellationToken)
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions { Args = [], ApplicationName = "Squirix.Server" });
        _ = await builder.AddSquirixServerAsync(
            static options =>
            {
                options.NodeId = "node-a";
                options.Uri = new Uri("https://localhost:6001");
                options.ReplicaCount = 1;
            },
            loadDiscoveredSettings: false,
            cancellationToken: cancellationToken);
        await using var app = builder.Build();
        var features = app.Services.GetRequiredService<FeatureState>();
        _ = await Assert.That(features.FoundationOnly).IsFalse();
        _ = await Assert.That(features.NetworkReplicationEnabled).IsFalse();
        _ = await Assert.That(app.Services.GetService<SquirixReplicationServiceAdapter>()).IsNull();
    }

    /// <summary>RF1 persists and replays a mutation without opening a replica group log.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfOneWritesWithoutGroupWal(CancellationToken cancellationToken)
    {
        await using var cluster = CreateCluster([new ClusterNode("n1", GetNextHttpUri())]);
        var host = await cluster.StartNodeAsync("n1", new IntegrationStartOptions { UsePersistence = true, ExtraScope = "rf1-no-group-wal" }, cancellationToken);
        var cache = host.GetCache<object?>("default");
        await cache.SetEntryAsync("rf1-operation", "default", "key", new NodeCacheEntry<object?> { Value = "persisted", Version = 1 }, cancellationToken);
        _ = await Assert.That(ReadText((await cache.GetValueAsync("default", "key", cancellationToken)).Value)).IsEqualTo("persisted");
        AssertNoGroupLog(host);
        await cluster.StopNodeAsync("n1");

        var restarted = await cluster.StartNodeAsync("n1", new IntegrationStartOptions { UsePersistence = true, ExtraScope = "rf1-no-group-wal", CleanTestDir = false }, cancellationToken);
        _ = await Assert.That(ReadText((await restarted.GetCache<object?>("default").GetValueAsync("default", "key", cancellationToken)).Value)).IsEqualTo("persisted");
        AssertNoGroupLog(restarted);
    }

    private static string? ReadText(object? value) => value is JsonElement json ? json.GetString() : value as string;

    private static void AssertNoGroupLog(ITestNodeHost host)
    {
        var registry = host.Services.GetService<ReplicaGroupRegistry>();
        if (registry?.TryGetLog("n1", out _) == true)
            throw new InvalidOperationException("RF1 must not open a replica group log.");
    }

    private static bool HasReplicationEndpoint(ITestNodeHost host)
    {
        foreach (var endpoint in host.GetRequiredService<EndpointDataSource>().Endpoints)
        {
            var grpc = endpoint.Metadata.GetMetadata<GrpcMethodMetadata>();
            if (grpc != null && string.Equals(grpc.Method.ServiceName, SquirixReplicationService.Descriptor.FullName, StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    /// <summary>Builds, without starting, a node of the given three-node topology through the public hosting path.</summary>
    /// <param name="nodes">The topology.</param>
    /// <param name="switches">The value of both failover switches.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The topology the built host resolves, and the number of hosted services the host registers.</returns>
    private static async Task<(TopologyOptions Topology, int HostedServices)> BuildHostAsync(ClusterNode[] nodes, bool switches, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-failover-host-path");
        var peers = new SquirixServerPeerOptions[nodes.Length];
        for (var i = 0; i < nodes.Length; i++)
            peers[i] = new SquirixServerPeerOptions { NodeId = nodes[i].NodeId, Uri = nodes[i].Uri };

        var (identity, mtlsOptions, certificate) = await ClusterIdentity.ResolveForNodeAsync(
            null,
            new TopologyOptions([new ServerPeer { NodeId = nodes[0].NodeId, Uri = nodes[0].Uri }, new ServerPeer { NodeId = nodes[1].NodeId, Uri = nodes[1].Uri }])
            {
                NodeId = nodes[0].NodeId,
                Uri = nodes[0].Uri,
            },
            cancellationToken);
        using var identityScope = identity;
        using var material = certificate;
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions { Args = [], ApplicationName = "Squirix.Server" });

        _ = await AspNetCoreExtensions.ConfigureSquirixServerBuilderAsync(
            builder,
            options =>
            {
                options.NodeId = nodes[0].NodeId;
                options.Uri = nodes[0].Uri;
                options.Peers = peers;
                options.ReplicaCount = 3;
                options.ReplicationEnabled = true;
                options.AutomaticFailoverEnabled = switches;
                options.QuorumReadsEnabled = switches;
                options.UsePersistence(dir);
            },
            null,
            false,
            null,
            args =>
            {
                args.MtlsOptions = mtlsOptions;
                args.Certificate = certificate;
            },
            cancellationToken);
        var hostedServices = 0;
        foreach (var descriptor in builder.Services)
        {
            if (descriptor.ServiceType == typeof(IHostedService))
                hostedServices++;
        }

        await using var app = builder.Build();
        return (app.Services.GetRequiredService<TopologyOptions>(), hostedServices);
    }
}
