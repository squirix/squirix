using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.UnitTests.Support;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Shared factory helpers for <see cref="SquirixReplicationServiceAdapterTests" />.</summary>
internal static class SquirixReplicationAdapterTestHelpers
{
    internal static async Task<SquirixReplicationServiceAdapterTests.AdapterFixture> CreateAdapterAsync(CancellationToken cancellationToken)
    {
        var topology = CreateTopology();
        var mtls = new MtlsOptions { InternalListenPort = 6001 };
        var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        var peerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-a");
        var material = MtlsCertificateMaterial.Create(peerCertificate, bundle.Ca);
        return new SquirixReplicationServiceAdapterTests.AdapterFixture(new SquirixReplicationServiceAdapter(topology, mtls, material), mtls, bundle, peerCertificate, material);
    }

    internal static TopologyOptions CreateTopology() => new(new ServerPeer { NodeId = "node-a", Uri = new Uri("https://localhost:6001") });

    internal static ReplicationEnvelopeHeader CreateValidHeader(string? senderNodeId = "node-a") => new()
    {
        SchemaVersion = EnvelopeCodec.SchemaVersion,
        SenderNodeId = senderNodeId,
        LeaderNodeId = senderNodeId,
        Term = 7,
    };
}
