using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>REQ-CFG-001 / REQ-CFG-002 coverage for ReplicaCount bounds and activation prerequisites.</summary>
[Immutable]
public sealed class ReplicaCountValidationTests : ServerUnitTestBase
{
    /// <summary>Default ReplicaCount is one.</summary>
    [Fact]
    public void DefaultReplicaCountIsOne()
    {
        Assert.Equal(1, new SquirixServerOptions().ReplicaCount);
        var topology = new TopologyOptions(CreatePeers(1))
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = PeerUri(1),
        };
        Assert.Equal(1, topology.ReplicaCount);
    }

    /// <summary>ReplicaCount may equal the distinct peer count.</summary>
    [Fact]
    public void AcceptsReplicaCountEqualPeerCount()
    {
        var peers = CreatePeers(3);
        var topology = new TopologyOptions(peers)
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = peers[0].Uri,
            ReplicaCount = peers.Length,
        };

        Assert.True(TopologyValidator.TryValidate(topology, out _));
    }

    /// <summary>ReplicaCount must be positive.</summary>
    [Fact]
    public void RejectsReplicaCountZero()
    {
        var peers = CreatePeers(2);
        var topology = new TopologyOptions(peers)
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = peers[0].Uri,
            ReplicaCount = 0,
        };

        Assert.False(TopologyValidator.TryValidate(topology, out var errors));
        Assert.Contains("ReplicaCount must be greater than zero.", errors, StringComparer.Ordinal);
    }

    /// <summary>ReplicaCount cannot exceed distinct peers.</summary>
    [Fact]
    public void RejectsReplicaCountAbovePeerCount()
    {
        var peers = CreatePeers(2);
        var topology = new TopologyOptions(peers)
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = peers[0].Uri,
            ReplicaCount = 3,
        };

        Assert.False(TopologyValidator.TryValidate(topology, out var errors));
        Assert.Contains("ReplicaCount cannot exceed the number of configured peers.", errors, StringComparer.Ordinal);
    }

    /// <summary>ReplicaCount may reach MaxReplicaCount when peers allow it.</summary>
    [Fact]
    public void AcceptsReplicaCountAtProtocolMaximum()
    {
        var peers = CreatePeers(TopologyConstraints.MaxReplicaCount);
        var topology = new TopologyOptions(peers)
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = peers[0].Uri,
            ReplicaCount = TopologyConstraints.MaxReplicaCount,
        };

        Assert.True(TopologyValidator.TryValidate(topology, out _));
    }

    /// <summary>ReplicaCount cannot exceed MaxReplicaCount.</summary>
    [Fact]
    public void RejectsReplicaCountAboveProtocolMaximum()
    {
        var peers = CreatePeers(TopologyConstraints.MaxReplicaCount + 1);
        var topology = new TopologyOptions(peers)
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = peers[0].Uri,
            ReplicaCount = TopologyConstraints.MaxReplicaCount + 1,
        };

        Assert.False(TopologyValidator.TryValidate(topology, out var errors));
        Assert.Contains($"ReplicaCount cannot exceed MaxReplicaCount ({TopologyConstraints.MaxReplicaCount}).", errors, StringComparer.Ordinal);
    }

    /// <summary>RF&gt;1 requires persistence before activation refusal.</summary>
    [Fact]
    public void RfTwoRequiresPersistence()
    {
        var options = CreateServerOptions(2);
        Assert.False(options.TryValidate(out var errors));
        Assert.Contains(ReplicationActivationGuard.PersistenceRequired, errors, StringComparer.Ordinal);
    }

    /// <summary>RF&gt;1 requires mTLS material when evaluated at hosting time.</summary>
    [Fact]
    public void RfTwoRequiresMtls()
    {
        var failures = new List<string>();
        ReplicationActivationGuard.CollectFailures(failures, 2, true, false);
        Assert.Contains(ReplicationActivationGuard.MtlsRequired, failures, StringComparer.Ordinal);
    }

    /// <summary>RF&gt;1 remains refused even when persistence and mTLS prerequisites are present.</summary>
    [Fact]
    public void RfTwoRemainsDisabledBeforeActivation()
    {
        var failures = new List<string>();
        ReplicationActivationGuard.CollectFailures(failures, 2, true, true);
        Assert.Equal([ReplicationActivationGuard.NotActivated], failures);
    }

    /// <summary>Configurator copies replica placement fields.</summary>
    [Fact]
    public void ConfiguratorCopiesReplicaCountAndGeneration()
    {
        var source = CreateServerOptions(3);
        source.ConfigurationGeneration = 9;
        var target = new SquirixServerOptions();
        Configurator.CopyOptions(source, target);
        Assert.Equal(3, target.ReplicaCount);
        Assert.Equal(9u, target.ConfigurationGeneration);
    }

    /// <summary>JSON settings load ReplicaCount and ConfigurationGeneration.</summary>
    [Fact]
    public async Task SettingsJsonLoadsReplicaCountAndGeneration()
    {
        using var dir = new TempDirectory("squirix-rf-settings");
        var path = Path.Join(dir.Path, "Squirix.settings.json");
        const string json =
            "{\"Squirix\":{\"Cluster\":{\"ClusterId\":\"c1\",\"NodeId\":\"n1\",\"Uri\":\"https://localhost:6001\",\"ReplicaCount\":1,\"ConfigurationGeneration\":4,\"Peers\":[{\"NodeId\":\"n1\",\"Uri\":\"https://localhost:6001\"},{\"NodeId\":\"n2\",\"Uri\":\"https://localhost:6002\"}]}}}";
        await File.WriteAllTextAsync(path, json, DefaultCancellationToken);

        var options = await Configurator.LoadFromFileAsync(path, DefaultCancellationToken);
        Assert.Equal(1, options.ReplicaCount);
        Assert.Equal(4u, options.ConfigurationGeneration);
    }

    private static SquirixServerOptions CreateServerOptions(int replicaCount)
    {
        var peers = new SquirixServerPeerOptions[Math.Max(replicaCount, 1)];
        for (var i = 0; i < peers.Length; i++)
        {
            peers[i] = new SquirixServerPeerOptions
            {
                NodeId = "n" + (i + 1).ToString(CultureInfo.InvariantCulture),
                Uri = PeerUri(i + 1),
            };
        }

        return new SquirixServerOptions
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = peers[0].Uri,
            ReplicaCount = replicaCount,
            Peers = peers,
        };
    }

    private static ServerPeer[] CreatePeers(int count)
    {
        var peers = new ServerPeer[count];
        for (var i = 0; i < count; i++)
            peers[i] = new ServerPeer { NodeId = "n" + (i + 1).ToString(CultureInfo.InvariantCulture), Uri = PeerUri(i + 1) };

        return peers;
    }

    private static Uri PeerUri(int index) => new("https://localhost:" + (6000 + index).ToString(CultureInfo.InvariantCulture));
}
