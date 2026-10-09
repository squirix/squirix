using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>
/// The automatic failover and quorum read switches of the server options: both default off, each requires the other, failover requires
/// three replicas, and the options, the settings file and the cluster configuration carry both values.
/// </summary>
[Immutable]
public sealed class FailoverOptionsValidationTests : IsolatedStorageTestBase
{
    /// <summary>A topology with every failover failure lists each of them.</summary>
    [Test]
    public async Task AllFailuresAreListed()
    {
        _ = await Assert.That(TopologyValidator.TryValidate(Topology(2, true, false), out var errors)).IsFalse();

        _ = await Assert.That(errors).Contains(TopologyValidator.AutomaticFailoverRequiresThreeReplicas, StringComparer.Ordinal);
        _ = await Assert.That(errors).Contains(TopologyValidator.AutomaticFailoverRequiresQuorumReads, StringComparer.Ordinal);
    }

    /// <summary>The cluster configuration built from the server options carries both switches.</summary>
    [Test]
    public async Task ConfiguratorMapsFailoverSwitches()
    {
        var topology = Configurator.ToClusterConfig(ServerOptions(3, true, true));

        _ = await Assert.That(topology.AutomaticFailoverEnabled).IsTrue();
        _ = await Assert.That(topology.QuorumReadsEnabled).IsTrue();
    }

    /// <summary>Copying server options copies both switches.</summary>
    [Test]
    public async Task CopyOptionsCopiesFailoverSwitches()
    {
        var target = new SquirixServerOptions();

        Configurator.CopyOptions(ServerOptions(3, true, true), target);

        _ = await Assert.That(target.AutomaticFailoverEnabled).IsTrue();
        _ = await Assert.That(target.QuorumReadsEnabled).IsTrue();
    }

    /// <summary>Both switches are off by default, and the default cluster configuration carries them off.</summary>
    [Test]
    public async Task DefaultsAreOff()
    {
        var options = new SquirixServerOptions();
        var topology = Configurator.ToClusterConfig(options);

        _ = await Assert.That(options.AutomaticFailoverEnabled).IsFalse();
        _ = await Assert.That(options.QuorumReadsEnabled).IsFalse();
        _ = await Assert.That(topology.AutomaticFailoverEnabled).IsFalse();
        _ = await Assert.That(topology.QuorumReadsEnabled).IsFalse();
    }

    /// <summary>Both switches on three or more replicas validate.</summary>
    /// <param name="replicaCount">The replica count.</param>
    [Test]
    [Arguments(3)]
    [Arguments(5)]
    public async Task FailoverOnThreeReplicasValidates(int replicaCount)
    {
        _ = await Assert.That(TopologyValidator.TryValidate(Topology(replicaCount, true, true), out _)).IsTrue();
        _ = await Assert.That(ServerOptions(replicaCount, true, true).TryValidate(out _)).IsTrue();
    }

    /// <summary>Automatic failover without quorum reads is refused.</summary>
    [Test]
    public async Task FailoverRequiresQuorumReads()
    {
        _ = await Assert.That(TopologyValidator.TryValidate(Topology(3, true, false), out var errors)).IsFalse();

        _ = await Assert.That((errors.Count, errors[0])).IsEqualTo((1, TopologyValidator.AutomaticFailoverRequiresQuorumReads));
    }

    /// <summary>Automatic failover on one or two replicas is refused.</summary>
    /// <param name="replicaCount">The replica count.</param>
    [Test]
    [Arguments(1)]
    [Arguments(2)]
    public async Task FailoverRequiresThreeReplicas(int replicaCount)
    {
        _ = await Assert.That(TopologyValidator.TryValidate(Topology(replicaCount, true, true), out var errors)).IsFalse();

        _ = await Assert.That((errors.Count, errors[0])).IsEqualTo((1, TopologyValidator.AutomaticFailoverRequiresThreeReplicas));
    }

    /// <summary>The public options refuse half of the switch pair with the validation message.</summary>
    [Test]
    public async Task PublicOptionsRefuseFailoverPair()
    {
        _ = await Assert.That(ServerOptions(3, true, false).TryValidate(out var failover)).IsFalse();
        _ = await Assert.That(ServerOptions(3, false, true).TryValidate(out var quorum)).IsFalse();
        var thrown = NodeExceptionAssert.For<ArgumentException>().Throws(ServerOptions(3, false, true), static options => options.Validate());

        _ = await Assert.That(failover).Contains(TopologyValidator.AutomaticFailoverRequiresQuorumReads, StringComparer.Ordinal);
        _ = await Assert.That(quorum).Contains(TopologyValidator.QuorumReadsRequireAutomaticFailover, StringComparer.Ordinal);
        _ = await Assert.That(thrown.Message).StartsWith(TopologyValidator.QuorumReadsRequireAutomaticFailover, StringComparison.Ordinal);
    }

    /// <summary>Quorum reads without automatic failover are refused.</summary>
    [Test]
    public async Task QuorumReadsRequireFailover()
    {
        _ = await Assert.That(TopologyValidator.TryValidate(Topology(3, false, true), out var errors)).IsFalse();

        _ = await Assert.That((errors.Count, errors[0])).IsEqualTo((1, TopologyValidator.QuorumReadsRequireAutomaticFailover));
    }

    /// <summary>A settings file loads both switches from the cluster section.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SettingsFileLoadsFailoverSwitches(CancellationToken cancellationToken)
    {
        var path = await WriteSettingsAsync("failover-on.json", "true", "true", cancellationToken);

        var (success, options, error) = await Configurator.LoadFromFileAsync(path, cancellationToken);

        _ = await Assert.That(error).IsNull();
        _ = await Assert.That(success).IsTrue();
        _ = await Assert.That(options!.AutomaticFailoverEnabled).IsTrue();
        _ = await Assert.That(options.QuorumReadsEnabled).IsTrue();
    }

    /// <summary>A settings file with failover on and quorum reads off fails to load with the validation message.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SettingsFileRefusesHalfPair(CancellationToken cancellationToken)
    {
        var path = await WriteSettingsAsync("failover-half.json", "true", "false", cancellationToken);

        var (success, _, error) = await Configurator.LoadFromFileAsync(path, cancellationToken);

        _ = await Assert.That(success).IsFalse();
        _ = await Assert.That(error).Contains(TopologyValidator.AutomaticFailoverRequiresQuorumReads, StringComparison.Ordinal);
    }

    private static Uri PeerUri(int index) => new("https://localhost:" + (6000 + index).ToString(CultureInfo.InvariantCulture));

    private static SquirixServerOptions ServerOptions(int replicaCount, bool failover, bool quorumReads)
    {
        var peers = new SquirixServerPeerOptions[Math.Max(replicaCount, 3)];
        for (var i = 0; i < peers.Length; i++)
            peers[i] = new SquirixServerPeerOptions { NodeId = "n" + (i + 1).ToString(CultureInfo.InvariantCulture), Uri = PeerUri(i + 1) };

        var options = new SquirixServerOptions
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = peers[0].Uri,
            ReplicaCount = replicaCount,
            Peers = peers,
            AutomaticFailoverEnabled = failover,
            QuorumReadsEnabled = quorumReads,
        };
        options.UsePersistence();
        return options;
    }

    private static TopologyOptions Topology(int replicaCount, bool failover, bool quorumReads)
    {
        var peers = new List<ServerPeer>(Math.Max(replicaCount, 3));
        for (var i = 0; i < Math.Max(replicaCount, 3); i++)
            peers.Add(new ServerPeer { NodeId = "n" + (i + 1).ToString(CultureInfo.InvariantCulture), Uri = PeerUri(i + 1) });

        return new TopologyOptions(peers)
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = peers[0].Uri,
            ReplicaCount = replicaCount,
            AutomaticFailoverEnabled = failover,
            QuorumReadsEnabled = quorumReads,
        };
    }

    private async Task<string> WriteSettingsAsync(string fileName, string failover, string quorumReads, CancellationToken cancellationToken)
    {
        var path = Path.Join(Dir, fileName);
        var json =
            "{\"Squirix\":{\"Cluster\":{\"ClusterId\":\"c1\",\"NodeId\":\"n1\",\"Uri\":\"https://localhost:6001\",\"ReplicaCount\":3,\"PersistenceEnabled\":true," +
            $"\"AutomaticFailoverEnabled\":{failover},\"QuorumReadsEnabled\":{quorumReads}," +
            "\"Peers\":[{\"NodeId\":\"n1\",\"Uri\":\"https://localhost:6001\"},{\"NodeId\":\"n2\",\"Uri\":\"https://localhost:6002\"},{\"NodeId\":\"n3\",\"Uri\":\"https://localhost:6003\"}]}}}";
        await File.WriteAllTextAsync(path, json, cancellationToken);
        return path;
    }
}
