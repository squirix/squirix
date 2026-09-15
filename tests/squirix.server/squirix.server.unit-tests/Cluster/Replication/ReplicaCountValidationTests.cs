using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>REQ-CFG-001 / REQ-CFG-002 coverage for ReplicaCount bounds and activation prerequisites.</summary>
[Immutable]
public sealed class ReplicaCountValidationTests : IsolatedStorageTestBase
{
    /// <summary>ReplicaCount may reach MaxReplicaCount when peers allow it.</summary>
    [Test]
    public async Task AcceptsReplicaCountAtProtocolMaximum()
    {
        var peers = CreatePeers(TopologyConstraints.MaxReplicaCount);
        var topology = new TopologyOptions(peers)
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = peers[0].Uri,
            ReplicaCount = TopologyConstraints.MaxReplicaCount,
        };

        _ = await Assert.That(TopologyValidator.TryValidate(topology, out _)).IsTrue();
    }

    /// <summary>ReplicaCount may equal the distinct peer count.</summary>
    [Test]
    public async Task AcceptsReplicaCountEqualPeerCount()
    {
        var peers = CreatePeers(3);
        var topology = new TopologyOptions(peers)
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = peers[0].Uri,
            ReplicaCount = peers.Length,
        };

        _ = await Assert.That(TopologyValidator.TryValidate(topology, out _)).IsTrue();
    }

    /// <summary>Configurator copies replica placement fields.</summary>
    [Test]
    public async Task ConfiguratorCopiesCountAndGeneration()
    {
        var source = CreateServerOptions(3);
        source.ConfigurationGeneration = 9;
        var target = new SquirixServerOptions();
        Configurator.CopyOptions(source, target);
        _ = await Assert.That(target.ReplicaCount).IsEqualTo(3);
        _ = await Assert.That(target.ConfigurationGeneration).IsEqualTo(9u);
    }

    /// <summary>Default ReplicaCount is one.</summary>
    [Test]
    public async Task DefaultReplicaCountIsOne()
    {
        _ = await Assert.That(new SquirixServerOptions().ReplicaCount).IsEqualTo(1);
        var topology = new TopologyOptions(CreatePeers(1))
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = PeerUri(1),
        };
        _ = await Assert.That(topology.ReplicaCount).IsEqualTo(1);
    }

    /// <summary>ReplicaCount cannot exceed distinct peers.</summary>
    [Test]
    public async Task RejectsReplicaCountAbovePeerCount()
    {
        var peers = CreatePeers(2);
        var topology = new TopologyOptions(peers)
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = peers[0].Uri,
            ReplicaCount = 3,
        };

        _ = await Assert.That(TopologyValidator.TryValidate(topology, out var errors)).IsFalse();
        _ = await Assert.That(errors).Contains("ReplicaCount cannot exceed the number of configured peers.", StringComparer.Ordinal);
    }

    /// <summary>ReplicaCount cannot exceed MaxReplicaCount.</summary>
    [Test]
    public async Task RejectsReplicaCountAboveProtocolMaximum()
    {
        var peers = CreatePeers(TopologyConstraints.MaxReplicaCount + 1);
        var topology = new TopologyOptions(peers)
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = peers[0].Uri,
            ReplicaCount = TopologyConstraints.MaxReplicaCount + 1,
        };

        _ = await Assert.That(TopologyValidator.TryValidate(topology, out var errors)).IsFalse();
        _ = await Assert.That(errors).Contains($"ReplicaCount cannot exceed MaxReplicaCount ({TopologyConstraints.MaxReplicaCount}).", StringComparer.Ordinal);
    }

    /// <summary>ReplicaCount must be positive.</summary>
    [Test]
    public async Task RejectsReplicaCountZero()
    {
        var peers = CreatePeers(2);
        var topology = new TopologyOptions(peers)
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = peers[0].Uri,
            ReplicaCount = 0,
        };

        _ = await Assert.That(TopologyValidator.TryValidate(topology, out var errors)).IsFalse();
        _ = await Assert.That(errors).Contains("ReplicaCount must be greater than zero.", StringComparer.Ordinal);
    }

    /// <summary>RF&gt;1 activates when persistence and mTLS prerequisites are present.</summary>
    [Test]
    public async Task RfTwoActivatesWithPrerequisites()
    {
        var failures = new List<string>();
        ReplicationActivationGuard.CollectFailures(failures, 2, true, true, true);
        _ = await Assert.That(failures).IsEmpty();
    }

    /// <summary>RF&gt;1 requires mTLS material when evaluated at hosting time.</summary>
    [Test]
    public async Task RfTwoRequiresMtls()
    {
        var failures = new List<string>();
        ReplicationActivationGuard.CollectFailures(failures, 2, true, false, true);
        _ = await Assert.That(failures).Contains(ReplicationActivationGuard.MtlsRequired, StringComparer.Ordinal);
    }

    /// <summary>RF&gt;1 requires persistence before activation refusal.</summary>
    [Test]
    public async Task RfTwoRequiresPersistence()
    {
        var options = CreateServerOptions(2);
        _ = await Assert.That(options.TryValidate(out var errors)).IsFalse();
        _ = await Assert.That(errors).Contains(ReplicationActivationGuard.PersistenceRequired, StringComparer.Ordinal);
    }

    /// <summary>JSON settings load ReplicaCount and ConfigurationGeneration.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SettingsJsonLoadsCountAndGeneration(CancellationToken cancellationToken)
    {
        var path = Path.Join(Dir.Path, "Squirix.settings.json");
        const string json =
            "{\"Squirix\":{\"Cluster\":{\"ClusterId\":\"c1\",\"NodeId\":\"n1\",\"Uri\":\"https://localhost:6001\",\"ReplicaCount\":1,\"ConfigurationGeneration\":4,\"Peers\":[{\"NodeId\":\"n1\",\"Uri\":\"https://localhost:6001\"},{\"NodeId\":\"n2\",\"Uri\":\"https://localhost:6002\"}]}}}";
        await File.WriteAllTextAsync(path, json, cancellationToken);

        var options = await Configurator.LoadAsync(path, cancellationToken);
        _ = await Assert.That(options.ReplicaCount).IsEqualTo(1);
        _ = await Assert.That(options.ConfigurationGeneration).IsEqualTo(4u);
    }

    private static ServerPeer[] CreatePeers(int count)
    {
        var peers = new ServerPeer[count];
        for (var i = 0; i < count; i++)
            peers[i] = new ServerPeer { NodeId = "n" + (i + 1).ToString(CultureInfo.InvariantCulture), Uri = PeerUri(i + 1) };

        return peers;
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

    private static Uri PeerUri(int index) => new("https://localhost:" + (6000 + index).ToString(CultureInfo.InvariantCulture));
}
