using System;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>
/// Unit tests for <see cref="SquirixReplicationServiceAdapter"/>.
/// </summary>
public sealed class SquirixReplicationServiceAdapterTests
{
    /// <summary>Verifies that GetReplicaStatus returns the node's current topology fingerprint and configuration generation.</summary>
    [Fact]
    public async Task GetReplicaStatusReturnsLocalTopologyAsync()
    {
        var peer = new ServerPeer { NodeId = "node-a", Uri = new Uri("https://localhost:6001") };
        var topology = new TopologyOptions(peer);
        var mtls = new MtlsOptions { InternalListenPort = 6001 };
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(TestContext.Current.CancellationToken);
        using var peerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, peer.NodeId);
        using var mtlsMaterial = MtlsCertificateMaterial.Create(peerCertificate, bundle.Ca);

        var adapter = new SquirixReplicationServiceAdapter(topology, mtls, mtlsMaterial);

        var header = new ReplicationEnvelopeHeader
        {
            SchemaVersion = EnvelopeCodec.SchemaVersion,
            SenderNodeId = peer.NodeId,
            TopologyFingerprint = ByteString.CopyFrom(1, 2, 3),
            ConfigurationGeneration = 999UL,
        };

        var httpContext = new DefaultHttpContext
        {
            Connection =
            {
                LocalPort = mtls.InternalListenPort,
                ClientCertificate = peerCertificate,
            },
        };
        var response = adapter.GetReplicaStatus(new GetReplicaStatusRequest { Header = header }, new TestServerCallContext(null, httpContext));
        var result = await response;

        // The adapter should report the node's own topology fingerprint and configuration generation,
        // not the caller-supplied header values.
        var expectedFingerprint = TopologyFingerprint.CreateFromTopology(topology, mtls).ToString();
        var actualFingerprint = Convert.ToHexString(result.TopologyFingerprint.ToByteArray());
        Assert.Equal(expectedFingerprint, actualFingerprint, StringComparer.Ordinal);
        Assert.Equal(topology.ConfigurationGeneration, result.ConfigurationGeneration);
        Assert.Equal(RefusalCodes.NotReady, result.RefusalCode);
    }

    /// <summary>Verifies that AppendReplicaEntries returns failure and a zero last log index when the node refuses.</summary>
    [Fact]
    public async Task AppendReplicaEntriesOnRefusalReturnsZeroAsync()
    {
        var peer = new ServerPeer { NodeId = "node-a", Uri = new Uri("https://localhost:6001") };
        var topology = new TopologyOptions(peer);
        var mtls = new MtlsOptions { InternalListenPort = 6001 };
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(TestContext.Current.CancellationToken);
        using var peerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, peer.NodeId);
        using var mtlsMaterial = MtlsCertificateMaterial.Create(peerCertificate, bundle.Ca);

        var adapter = new SquirixReplicationServiceAdapter(topology, mtls, mtlsMaterial);

        var header = new ReplicationEnvelopeHeader
        {
            SchemaVersion = EnvelopeCodec.SchemaVersion,
            SenderNodeId = peer.NodeId,
            LeaderNodeId = peer.NodeId,
        };

        var request = new AppendReplicaEntriesRequest
        {
            Header = header,
            PrevLogIndex = 42,
        };

        var httpContext = new DefaultHttpContext
        {
            Connection =
            {
                LocalPort = mtls.InternalListenPort,
                ClientCertificate = peerCertificate,
            },
        };
        var response = adapter.AppendReplicaEntries(request, new TestServerCallContext(null, httpContext));
        var result = await response;

        Assert.False(result.Success);
        Assert.Equal(0UL, result.LastLogIndex);
        Assert.Equal(RefusalCodes.NotReady, result.RefusalCode);
    }

    /// <summary>Verifies that a leader-authorized RPC with a LeaderNodeId that does not match the peer certificate is rejected.</summary>
    [Fact]
    public async Task ForeignLeaderNodeIdIsRejectedAsync()
    {
        var peer = new ServerPeer { NodeId = "node-a", Uri = new Uri("https://localhost:6001") };
        var topology = new TopologyOptions(peer);
        var mtls = new MtlsOptions { InternalListenPort = 6001 };
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(TestContext.Current.CancellationToken);
        using var peerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, peer.NodeId);
        using var mtlsMaterial = MtlsCertificateMaterial.Create(peerCertificate, bundle.Ca);

        var adapter = new SquirixReplicationServiceAdapter(topology, mtls, mtlsMaterial);

        var header = new ReplicationEnvelopeHeader
        {
            SchemaVersion = EnvelopeCodec.SchemaVersion,
            SenderNodeId = peer.NodeId,
            LeaderNodeId = "node-b",
        };

        var request = new AppendReplicaEntriesRequest
        {
            Header = header,
            PrevLogIndex = 42,
        };

        var httpContext = new DefaultHttpContext
        {
            Connection =
            {
                LocalPort = mtls.InternalListenPort,
                ClientCertificate = peerCertificate,
            },
        };
        var ex = await Assert.ThrowsAsync<RpcException>(() => adapter.AppendReplicaEntries(request, new TestServerCallContext(null, httpContext)));

        Assert.Equal(StatusCode.PermissionDenied, ex.StatusCode);
    }
}
