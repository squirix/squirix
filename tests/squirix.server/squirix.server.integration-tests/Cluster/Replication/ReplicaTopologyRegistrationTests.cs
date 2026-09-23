using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>DI registration coverage for RF=1 planning vs network replication transport.</summary>
public sealed class ReplicaTopologyRegistrationTests : NodeIntegrationTestBase
{
    /// <summary>Peer order does not change the topology fingerprint.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PeerOrderDoesNotChangeFingerprint(CancellationToken cancellationToken)
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peersAb = BuildClusterPeers([new ClusterNode("n1", uriA), new ClusterNode("n2", uriB)]);
        var peersBa = BuildClusterPeers([new ClusterNode("n2", uriB), new ClusterNode("n1", uriA)]);

        string fingerprintAb;
        await using (var hostAb = await StartClusterAsync(uriA, peersAb, cancellationToken: cancellationToken))
            fingerprintAb = hostAb.GetRequiredService<TopologyFingerprint>().ToString();

        string fingerprintBa;
        await using (var hostBa = await StartClusterAsync(uriA, peersBa, cancellationToken: cancellationToken))
            fingerprintBa = hostBa.GetRequiredService<TopologyFingerprint>().ToString();

        _ = await Assert.That(fingerprintBa).IsEqualTo(fingerprintAb);
    }

    /// <summary>RF=1 keeps network replication disabled and does not enable internode mTLS on a standalone node.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfOneSkipsReplicationTransport(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("n1", cancellationToken: cancellationToken);
        var host = cluster["n1"];
        var featureState = host.GetRequiredService<FeatureState>();
        _ = await Assert.That(featureState.NetworkReplicationEnabled).IsFalse();
        _ = await Assert.That(host.HasInterNodeMtlsListener).IsFalse();
        var material = host.GetRequiredService<MtlsCertificate>();
        _ = await Assert.That(material.Enabled).IsFalse();
    }
}
