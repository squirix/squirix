using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.IntegrationTests.Support;
using Xunit;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>DI registration coverage for RF=1 planning vs network replication transport.</summary>
public sealed class ReplicaTopologyRegistrationTests : NodeIntegrationTestBase
{
    /// <summary>RF=1 keeps network replication disabled and does not enable inter-node mTLS on a standalone node.</summary>
    [Fact]
    public async Task RfOneSkipsReplicationTransport()
    {
        var uri = GetNextHttpUri();
        await using var host = await StartNodeAsync(uri, "n1");
        var featureState = host.Services.GetRequiredService<FeatureState>();
        Assert.False(featureState.NetworkReplicationEnabled);
        Assert.False(host.HasInterNodeMtlsListener);
        var material = host.Services.GetRequiredService<MtlsCertificateMaterial>();
        Assert.False(material.Enabled);
    }

    /// <summary>Peer order does not change the topology fingerprint.</summary>
    [Fact]
    public async Task DifferentPeerOrderProducesSameFingerprint()
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peersAb = BuildClusterPeers([("n1", uriA), ("n2", uriB)]);
        var peersBa = BuildClusterPeers([("n2", uriB), ("n1", uriA)]);

        string fingerprintAb;
        await using (var hostAb = await StartNodeAsync(uriA, peersAb))
            fingerprintAb = hostAb.Services.GetRequiredService<TopologyFingerprint>().ToString();

        string fingerprintBa;
        await using (var hostBa = await StartNodeAsync(uriA, peersBa))
            fingerprintBa = hostBa.Services.GetRequiredService<TopologyFingerprint>().ToString();

        Assert.Equal(fingerprintAb, fingerprintBa);
    }
}
