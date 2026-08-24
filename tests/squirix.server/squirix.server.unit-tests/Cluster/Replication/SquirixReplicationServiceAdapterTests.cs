using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>
/// Unit tests for <see cref="SquirixReplicationServiceAdapter"/>.
/// </summary>
[Immutable]
public sealed class SquirixReplicationServiceAdapterTests : ServerUnitTestBase
{
    /// <summary>Verifies that AdvanceReplicaCommit refuses while echoing the term and a zero commit index.</summary>
    [Fact]
    public async Task AdvanceReplicaCommitReturnsNotReadyAsync()
    {
        using var fixture = await CreateAdapterAsync(DefaultCancellationToken);
        var request = new AdvanceReplicaCommitRequest { Header = CreateValidHeader(), CommitIndex = 42 };

        var response = await fixture.Adapter.AdvanceReplicaCommit(request, new TestServerCallContext(null, fixture.CreateHttpContext()));

        Assert.Equal(7UL, response.Term);
        Assert.Equal(0UL, response.CommitIndex);
        Assert.False(response.Success);
        Assert.Equal(RefusalCodes.NotReady, response.RefusalCode);
    }

    /// <summary>Verifies that AppendReplicaEntries returns failure and a zero-last log index when the node refuses.</summary>
    [Fact]
    public async Task EntriesAppendOnRefusalReturnsZeroAsync()
    {
        var peer = new ServerPeer { NodeId = "node-a", Uri = new Uri("https://localhost:6001") };
        var topology = new TopologyOptions(peer);
        var mtls = new MtlsOptions { InternalListenPort = 6001 };
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(DefaultCancellationToken);
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

    /// <summary>Verifies that the adapter constructor requires a cluster topology.</summary>
    [Fact]
    public void ConstructorRequiresCluster()
    {
        var material = MtlsCertificateMaterial.Load(new MtlsOptions(), null, false);
        TopologyOptions? cluster = null;

        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(material, cluster, static (m, c) => _ = new SquirixReplicationServiceAdapter(c!, new MtlsOptions(), m));
    }

    /// <summary>Verifies that the adapter constructor requires mTLS certificate material.</summary>
    [Fact]
    public void ConstructorRequiresMtlsMaterial()
    {
        MtlsCertificateMaterial? material = null;

        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(
            material,
            static m => _ = new SquirixReplicationServiceAdapter(CreateTopology(), new MtlsOptions { InternalListenPort = 6001 }, m!));
    }

    /// <summary>Verifies that the adapter constructor requires mTLS options.</summary>
    [Fact]
    public void ConstructorRequiresMtlsOptions()
    {
        var material = MtlsCertificateMaterial.Load(new MtlsOptions(), null, false);
        MtlsOptions? mtlsOptions = null;

        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(
            material,
            mtlsOptions,
            static (m, mtls) => _ = new SquirixReplicationServiceAdapter(CreateTopology(), mtls!, m));
    }

    /// <summary>Verifies that a disabled mTLS material makes the internal listener unavailable.</summary>
    [Fact]
    public void DisabledMtlsMaterialIsRejected()
    {
        var adapter = new SquirixReplicationServiceAdapter(
            CreateTopology(),
            new MtlsOptions { InternalListenPort = 6001 },
            MtlsCertificateMaterial.Load(new MtlsOptions(), null, false));
        var request = new GetReplicaStatusRequest { Header = CreateValidHeader() };

        var ex = NodeExceptionAssert.For<RpcException>().Throws(adapter, request, new TestServerCallContext(), static (a, req, context) => _ = a.GetReplicaStatus(req, context));

        Assert.Equal(StatusCode.Unavailable, ex.StatusCode);
    }

    /// <summary>Verifies that GetReplicaStatus returns the node's current topology fingerprint and configuration generation.</summary>
    [Fact]
    public async Task ReplicaStatusReturnsLocalTopologyAsync()
    {
        var peer = new ServerPeer { NodeId = "node-a", Uri = new Uri("https://localhost:6001") };
        var topology = new TopologyOptions(peer);
        var mtls = new MtlsOptions { InternalListenPort = 6001 };
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(DefaultCancellationToken);
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

    /// <summary>Verifies that InstallReplicaSnapshot rejects a later chunk with a different sender node id.</summary>
    [Fact]
    public async Task SnapshotRejectsMismatchedSenderAsync()
    {
        using var fixture = await CreateAdapterAsync(DefaultCancellationToken);
        var stream = new TestAsyncStreamReader<InstallReplicaSnapshotRequest>(
            [new InstallReplicaSnapshotRequest { Header = CreateValidHeader() }, new InstallReplicaSnapshotRequest { Header = CreateValidHeader("node-b") }]);

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(fixture.Adapter.InstallReplicaSnapshot(stream, new TestServerCallContext(null, fixture.CreateHttpContext())));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
    }

    /// <summary>Verifies that InstallReplicaSnapshot requires at least one chunk.</summary>
    [Fact]
    public async Task SnapshotInstallRequiresFirstChunkAsync()
    {
        using var fixture = await CreateAdapterAsync(DefaultCancellationToken);
        var stream = new TestAsyncStreamReader<InstallReplicaSnapshotRequest>([]);

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(fixture.Adapter.InstallReplicaSnapshot(stream, new TestServerCallContext(null, fixture.CreateHttpContext())));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
    }

    /// <summary>Verifies that InstallReplicaSnapshot refuses a single-chunk stream.</summary>
    [Fact]
    public async Task SnapshotInstallReportsNotReadyAsync()
    {
        using var fixture = await CreateAdapterAsync(DefaultCancellationToken);
        var stream = new TestAsyncStreamReader<InstallReplicaSnapshotRequest>([new InstallReplicaSnapshotRequest { Header = CreateValidHeader(), LastIncludedIndex = 9 }]);

        var response = await fixture.Adapter.InstallReplicaSnapshot(stream, new TestServerCallContext(null, fixture.CreateHttpContext()));

        Assert.Equal(7UL, response.Term);
        Assert.False(response.Success);
        Assert.Equal(RefusalCodes.NotReady, response.RefusalCode);
    }

    /// <summary>Verifies that InstallReplicaSnapshot skips later chunks without a header.</summary>
    [Fact]
    public async Task InstallSkipsHeaderlessChunksAsync()
    {
        using var fixture = await CreateAdapterAsync(DefaultCancellationToken);
        var stream = new TestAsyncStreamReader<InstallReplicaSnapshotRequest>(
            [new InstallReplicaSnapshotRequest { Header = CreateValidHeader() }, new InstallReplicaSnapshotRequest()]);

        var response = await fixture.Adapter.InstallReplicaSnapshot(stream, new TestServerCallContext(null, fixture.CreateHttpContext()));

        Assert.False(response.Success);
        Assert.Equal(RefusalCodes.NotReady, response.RefusalCode);
    }

    /// <summary>Verifies that a request without a client certificate is unauthenticated.</summary>
    [Fact]
    public async Task MissingClientCertificateIsRejectedAsync()
    {
        using var fixture = await CreateAdapterAsync(DefaultCancellationToken);
        var request = new GetReplicaStatusRequest { Header = CreateValidHeader() };
        var httpContext = new DefaultHttpContext
        {
            Connection =
            {
                LocalPort = fixture.Mtls.InternalListenPort,
            },
        };

        var ex = NodeExceptionAssert.For<RpcException>().Throws(
            fixture.Adapter,
            request,
            new TestServerCallContext(null, httpContext),
            static (adapter, req, context) => _ = adapter.GetReplicaStatus(req, context));

        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }

    /// <summary>Verifies that a missing envelope header is rejected before peer authentication.</summary>
    [Fact]
    public async Task MissingHeaderIsRejectedAsync()
    {
        using var fixture = await CreateAdapterAsync(DefaultCancellationToken);

        var ex = NodeExceptionAssert.For<RpcException>().Throws(
            fixture.Adapter,
            new GetReplicaStatusRequest(),
            new TestServerCallContext(null, fixture.CreateHttpContext()),
            static (adapter, request, context) => _ = adapter.GetReplicaStatus(request, context));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
    }

    /// <summary>Verifies that a header with an empty sender node id is rejected.</summary>
    [Fact]
    public async Task MissingSenderNodeIdIsRejectedAsync()
    {
        using var fixture = await CreateAdapterAsync(DefaultCancellationToken);
        var request = new GetReplicaStatusRequest
        {
            Header = new ReplicationEnvelopeHeader { SchemaVersion = EnvelopeCodec.SchemaVersion, SenderNodeId = " " },
        };

        var ex = NodeExceptionAssert.For<RpcException>().Throws(
            fixture.Adapter,
            request,
            new TestServerCallContext(null, fixture.CreateHttpContext()),
            static (adapter, req, context) => _ = adapter.GetReplicaStatus(req, context));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
    }

    /// <summary>Verifies that a claimed sender node id differing from the certificate is unauthenticated.</summary>
    [Fact]
    public async Task SenderNodeIdMismatchIsRejectedAsync()
    {
        using var fixture = await CreateAdapterAsync(DefaultCancellationToken);
        var request = new GetReplicaStatusRequest { Header = CreateValidHeader("node-b") };

        var ex = NodeExceptionAssert.For<RpcException>().Throws(
            fixture.Adapter,
            request,
            new TestServerCallContext(null, fixture.CreateHttpContext()),
            static (adapter, req, context) => _ = adapter.GetReplicaStatus(req, context));

        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }

    /// <summary>Verifies that a certificate not bound to a configured peer is unauthenticated.</summary>
    [Fact]
    public async Task UnknownPeerCertificateIsRejectedAsync()
    {
        var peer = new ServerPeer { NodeId = "node-a", Uri = new Uri("https://localhost:6001") };
        var topology = new TopologyOptions(peer);
        var mtls = new MtlsOptions { InternalListenPort = 6001 };
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(DefaultCancellationToken);
        using var unknownCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-unknown");
        using var material = MtlsCertificateMaterial.Create(unknownCertificate, bundle.Ca);
        var adapter = new SquirixReplicationServiceAdapter(topology, mtls, material);
        var request = new GetReplicaStatusRequest { Header = CreateValidHeader("node-unknown") };
        var httpContext = new DefaultHttpContext
        {
            Connection =
            {
                LocalPort = mtls.InternalListenPort,
                ClientCertificate = unknownCertificate,
            },
        };

        var ex = NodeExceptionAssert.For<RpcException>().Throws(
            adapter,
            request,
            new TestServerCallContext(null, httpContext),
            static (a, req, context) => _ = a.GetReplicaStatus(req, context));

        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }

    /// <summary>Verifies that an unsupported envelope schema version is rejected.</summary>
    [Fact]
    public async Task UnsupportedSchemaVersionIsRejectedAsync()
    {
        using var fixture = await CreateAdapterAsync(DefaultCancellationToken);
        var request = new GetReplicaStatusRequest
        {
            Header = new ReplicationEnvelopeHeader { SchemaVersion = 99, SenderNodeId = "node-a" },
        };

        var ex = NodeExceptionAssert.For<RpcException>().Throws(
            fixture.Adapter,
            request,
            new TestServerCallContext(null, fixture.CreateHttpContext()),
            static (adapter, req, context) => _ = adapter.GetReplicaStatus(req, context));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
    }

    /// <summary>Verifies that a call not arriving on the internal listener is denied.</summary>
    [Fact]
    public async Task WrongLocalPortIsRejectedAsync()
    {
        using var fixture = await CreateAdapterAsync(DefaultCancellationToken);
        var request = new GetReplicaStatusRequest { Header = CreateValidHeader() };
        var httpContext = new DefaultHttpContext
        {
            Connection =
            {
                LocalPort = 0,
                ClientCertificate = fixture.NodeCertificate,
            },
        };

        var ex = NodeExceptionAssert.For<RpcException>().Throws(
            fixture.Adapter,
            request,
            new TestServerCallContext(null, httpContext),
            static (adapter, req, context) => _ = adapter.GetReplicaStatus(req, context));

        Assert.Equal(StatusCode.PermissionDenied, ex.StatusCode);
    }

    private static async Task<AdapterFixture> CreateAdapterAsync(CancellationToken cancellationToken)
    {
        var topology = CreateTopology();
        var mtls = new MtlsOptions { InternalListenPort = 6001 };
        var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        var peerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-a");
        var material = MtlsCertificateMaterial.Create(peerCertificate, bundle.Ca);
        return new AdapterFixture(new SquirixReplicationServiceAdapter(topology, mtls, material), mtls, bundle, peerCertificate, material);
    }

    private static TopologyOptions CreateTopology() => new(new ServerPeer { NodeId = "node-a", Uri = new Uri("https://localhost:6001") });

    private static ReplicationEnvelopeHeader CreateValidHeader(string? senderNodeId = "node-a") => new()
    {
        SchemaVersion = EnvelopeCodec.SchemaVersion,
        SenderNodeId = senderNodeId,
        LeaderNodeId = senderNodeId,
        Term = 7,
    };

    [Immutable]
    private sealed class AdapterFixture : IDisposable
    {
        private readonly MtlsTestCertificateBundle _bundle;
        private readonly IDisposable _material;

        internal AdapterFixture(
            SquirixReplicationServiceAdapter adapter,
            MtlsOptions mtls,
            MtlsTestCertificateBundle bundle,
            X509Certificate2 peerCertificate,
            MtlsCertificateMaterial material)
        {
            Adapter = adapter;
            Mtls = mtls;
            NodeCertificate = peerCertificate;
            _bundle = bundle;
            _material = material;
        }

        internal SquirixReplicationServiceAdapter Adapter { get; }

        internal MtlsOptions Mtls { get; }

        internal X509Certificate2? NodeCertificate { get; }

        public void Dispose()
        {
            _material.Dispose();
            NodeCertificate?.Dispose();
            _bundle.Dispose();
        }

        internal DefaultHttpContext CreateHttpContext() => new()
        {
            Connection =
            {
                LocalPort = Mtls.InternalListenPort,
                ClientCertificate = NodeCertificate,
            },
        };
    }

    /// <summary>In-memory <see cref="IAsyncStreamReader{T}" /> backed by a fixed item sequence.</summary>
    /// <typeparam name="T">The streamed message type.</typeparam>
    [Immutable]
    private sealed class TestAsyncStreamReader<T> : IAsyncStreamReader<T>
    {
        private readonly IEnumerator<T> _items;

        /// <summary>Initializes a new instance of the <see cref="TestAsyncStreamReader{T}" /> class.</summary>
        /// <param name="items">The item sequence to stream.</param>
        internal TestAsyncStreamReader(IEnumerable<T> items)
        {
            _items = items.GetEnumerator();
        }

        /// <inheritdoc />
        public T Current => _items.Current;

        /// <inheritdoc />
        public Task<bool> MoveNext(CancellationToken cancellationToken) => Task.FromResult(_items.MoveNext());
    }
}
