using System;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Adapters.Grpc.Replication.SquirixReplicationAdapterTestHelpers;

namespace Squirix.Server.UnitTests.Adapters.Grpc.Replication;

/// <summary>Unit tests for <see cref="SquirixReplicationServiceAdapter" />.</summary>
[Immutable]
public sealed class SquirixReplicationServiceAdapterTests : ServerUnitTestBase
{
    /// <summary>Verifies that the adapter constructor requires a cluster topology.</summary>
    [Test]
    public void ConstructorRequiresCluster()
    {
        var material = MtlsCertificate.Load(new MtlsOptions(), null, false);
        TopologyOptions? cluster = null;

        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(material, cluster, static (m, c) => _ = new SquirixReplicationServiceAdapter(c!, new MtlsOptions(), m, TimeProvider.System));
    }

    /// <summary>Verifies that the adapter constructor requires the replica sets whenever it is given a group registry.</summary>
    /// <returns>A task that completes when the registry is disposed.</returns>
    [Test]
    public async Task ConstructorRequiresMembersWithGroups()
    {
        using var dir = new TempDirectory("squirix-replication-adapter-members");
        await using var registry = new ReplicaGroupRegistry(dir, ["node-a"], 1, ReadOnlyMemory<byte>.Of(9), 1UL, NullLoggerFactory.Instance, new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System }) { ElectionClock = TimeProvider.System };

        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(
            registry,
            static r => _ = new SquirixReplicationServiceAdapter(
                CreateTopology(),
                new MtlsOptions { InternalListenPort = 6001 },
                MtlsCertificate.Load(new MtlsOptions(), null, false),
                TimeProvider.System,
                r));
    }

    /// <summary>Verifies that the adapter constructor requires mTLS certificate material.</summary>
    [Test]
    public void ConstructorRequiresMtlsMaterial()
    {
        MtlsCertificate? certificate = null;

        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(
            certificate,
            static m => _ = new SquirixReplicationServiceAdapter(CreateTopology(), new MtlsOptions { InternalListenPort = 6001 }, m!, TimeProvider.System));
    }

    /// <summary>Verifies that the adapter constructor requires mTLS options.</summary>
    [Test]
    public void ConstructorRequiresMtlsOptions()
    {
        var material = MtlsCertificate.Load(new MtlsOptions(), null, false);
        MtlsOptions? mtlsOptions = null;

        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(
            material,
            mtlsOptions,
            static (m, mtls) => _ = new SquirixReplicationServiceAdapter(CreateTopology(), mtls!, m, TimeProvider.System));
    }

    /// <summary>Verifies that a disabled mTLS material makes the internal listener unavailable.</summary>
    [Test]
    public async Task DisabledMtlsMaterialIsRejected()
    {
        var adapter = new SquirixReplicationServiceAdapter(CreateTopology(), new MtlsOptions { InternalListenPort = 6001 }, MtlsCertificate.Load(new MtlsOptions(), null, false), TimeProvider.System);
        var request = new GetReplicaStatusRequest { Header = CreateValidHeader() };

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(adapter.GetReplicaStatus(request, new TestServerCallContext()));

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unavailable);
    }

    /// <summary>Verifies that AppendReplicaEntries returns failure and a zero-last log index when the node refuses.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EntriesAppendOnRefusalReturnsZeroAsync(CancellationToken cancellationToken)
    {
        var peer = new ServerPeer { NodeId = "node-a", Uri = new Uri("https://localhost:6001") };
        var topology = new TopologyOptions(peer);
        var mtls = new MtlsOptions { InternalListenPort = 6001 };
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var peerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, peer.NodeId);
        using var mtlsMaterial = MtlsCertificate.Create(peerCertificate, bundle.Ca);

        var adapter = new SquirixReplicationServiceAdapter(topology, mtls, mtlsMaterial, TimeProvider.System);

        var header = new ReplicationEnvelopeHeader
        {
            SchemaVersion = EnvelopeSchema.Version,
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

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.LastLogIndex).IsEqualTo(0UL);
        _ = await Assert.That(result.RefusalCode).IsEqualTo(RefusalCodes.NotReady);
    }

    /// <summary>Verifies that a call on an open connection whose certificate expired since its first call is unauthenticated.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredCertificateIsRejectedAsync(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(TimeProvider.System.GetUtcNow());
        var mtls = new MtlsOptions { InternalListenPort = 6001 };
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var peerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-a", clock.GetUtcNow().AddHours(-1), clock.GetUtcNow().AddHours(1));
        using var material = MtlsCertificate.Create(peerCertificate, bundle.Ca);
        var adapter = new SquirixReplicationServiceAdapter(CreateTopology(), mtls, material, clock);
        var request = new GetReplicaStatusRequest { Header = CreateValidHeader() };
        var httpContext = new DefaultHttpContext { Connection = { LocalPort = mtls.InternalListenPort, ClientCertificate = peerCertificate } };

        _ = await adapter.GetReplicaStatus(request, new TestServerCallContext(null, httpContext));
        clock.Advance(TimeSpan.FromHours(2));
        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(adapter.GetReplicaStatus(request, new TestServerCallContext(null, httpContext)));

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unauthenticated);
    }

    /// <summary>Verifies that a request without a client certificate is unauthenticated.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MissingClientCertificateIsRejectedAsync(CancellationToken cancellationToken)
    {
        using var fixture = await CreateAdapterAsync(cancellationToken);
        var request = new GetReplicaStatusRequest { Header = CreateValidHeader() };
        var httpContext = new DefaultHttpContext
        {
            Connection =
            {
                LocalPort = fixture.Mtls.InternalListenPort,
            },
        };

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(fixture.Adapter.GetReplicaStatus(request, new TestServerCallContext(null, httpContext)));

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unauthenticated);
    }

    /// <summary>Verifies that a missing envelope header is rejected before peer authentication.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MissingHeaderIsRejectedAsync(CancellationToken cancellationToken)
    {
        using var fixture = await CreateAdapterAsync(cancellationToken);

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            fixture.Adapter.GetReplicaStatus(new GetReplicaStatusRequest(), new TestServerCallContext(null, fixture.CreateHttpContext())));

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.InvalidArgument);
    }

    /// <summary>Verifies that a header with an empty sender node id is rejected.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MissingSenderNodeIdIsRejectedAsync(CancellationToken cancellationToken)
    {
        using var fixture = await CreateAdapterAsync(cancellationToken);
        var request = new GetReplicaStatusRequest
        {
            Header = new ReplicationEnvelopeHeader { SchemaVersion = EnvelopeSchema.Version, SenderNodeId = " " },
        };

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(fixture.Adapter.GetReplicaStatus(request, new TestServerCallContext(null, fixture.CreateHttpContext())));

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.InvalidArgument);
    }

    /// <summary>Verifies that GetReplicaStatus returns the node's current topology fingerprint and configuration generation.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReplicaStatusReturnsLocalTopologyAsync(CancellationToken cancellationToken)
    {
        var peer = new ServerPeer { NodeId = "node-a", Uri = new Uri("https://localhost:6001") };
        var topology = new TopologyOptions(peer);
        var mtls = new MtlsOptions { InternalListenPort = 6001 };
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var peerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, peer.NodeId);
        using var mtlsMaterial = MtlsCertificate.Create(peerCertificate, bundle.Ca);

        var adapter = new SquirixReplicationServiceAdapter(topology, mtls, mtlsMaterial, TimeProvider.System);

        var header = new ReplicationEnvelopeHeader
        {
            SchemaVersion = EnvelopeSchema.Version,
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
        // Golden oracle: precomputed SHA-256 over the documented canonical layout for the
        // single-peer topology above (node-a, https://localhost:6001, generation 1).
        // Must stay a constant: recomputing it via TopologyFingerprint here would mask
        // a broken fingerprint implementation identically on both sides.
        const string expectedFingerprint = "425BB7DE0321D8A16345697266147C6E1BD9D9F175592F680E758D6BDA295DD2";
        var actualFingerprint = Convert.ToHexString(result.TopologyFingerprint.ToByteArray());
        _ = await Assert.That(actualFingerprint).IsEqualTo(expectedFingerprint, StringComparer.Ordinal);
        _ = await Assert.That(result.ConfigurationGeneration).IsEqualTo(topology.ConfigurationGeneration);
        _ = await Assert.That(result.RefusalCode).IsEqualTo(RefusalCodes.NotReady);
    }

    /// <summary>Verifies that a claimed sender node id differing from the certificate is unauthenticated.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SenderNodeIdMismatchIsRejectedAsync(CancellationToken cancellationToken)
    {
        using var fixture = await CreateAdapterAsync(cancellationToken);
        var request = new GetReplicaStatusRequest { Header = CreateValidHeader("node-b") };

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(fixture.Adapter.GetReplicaStatus(request, new TestServerCallContext(null, fixture.CreateHttpContext())));

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unauthenticated);
    }

    /// <summary>Verifies that a certificate not bound to a configured peer is unauthenticated.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnknownPeerCertificateIsRejectedAsync(CancellationToken cancellationToken)
    {
        var peer = new ServerPeer { NodeId = "node-a", Uri = new Uri("https://localhost:6001") };
        var topology = new TopologyOptions(peer);
        var mtls = new MtlsOptions { InternalListenPort = 6001 };
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var unknownCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-unknown");
        using var material = MtlsCertificate.Create(unknownCertificate, bundle.Ca);
        var adapter = new SquirixReplicationServiceAdapter(topology, mtls, material, TimeProvider.System);
        var request = new GetReplicaStatusRequest { Header = CreateValidHeader("node-unknown") };
        var httpContext = new DefaultHttpContext
        {
            Connection =
            {
                LocalPort = mtls.InternalListenPort,
                ClientCertificate = unknownCertificate,
            },
        };

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(adapter.GetReplicaStatus(request, new TestServerCallContext(null, httpContext)));

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unauthenticated);
    }

    /// <summary>Verifies that an unsupported envelope schema version is rejected.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnsupportedSchemaVersionIsRejectedAsync(CancellationToken cancellationToken)
    {
        using var fixture = await CreateAdapterAsync(cancellationToken);
        var request = new GetReplicaStatusRequest
        {
            Header = new ReplicationEnvelopeHeader { SchemaVersion = 99, SenderNodeId = "node-a" },
        };

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(fixture.Adapter.GetReplicaStatus(request, new TestServerCallContext(null, fixture.CreateHttpContext())));

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.InvalidArgument);
    }

    /// <summary>Verifies that a later call on a verified connection still binds the claimed sender to the certificate NodeId.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VerifiedPeerKeepsSenderBindingAsync(CancellationToken cancellationToken)
    {
        using var fixture = await CreateAdapterAsync(cancellationToken);
        var httpContext = fixture.CreateHttpContext();
        _ = await fixture.Adapter.GetReplicaStatus(new GetReplicaStatusRequest { Header = CreateValidHeader() }, new TestServerCallContext(null, httpContext));
        var request = new GetReplicaStatusRequest { Header = CreateValidHeader("node-c") };

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(fixture.Adapter.GetReplicaStatus(request, new TestServerCallContext(null, httpContext)));

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unauthenticated);
    }

    /// <summary>Verifies that a call not arriving on the internal listener is denied.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WrongLocalPortIsRejectedAsync(CancellationToken cancellationToken)
    {
        using var fixture = await CreateAdapterAsync(cancellationToken);
        var request = new GetReplicaStatusRequest { Header = CreateValidHeader() };
        var httpContext = new DefaultHttpContext
        {
            Connection =
            {
                LocalPort = 0,
                ClientCertificate = fixture.NodeCertificate,
            },
        };

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(fixture.Adapter.GetReplicaStatus(request, new TestServerCallContext(null, httpContext)));

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.PermissionDenied);
    }

    [Immutable]
    internal sealed class AdapterFixture : IDisposable
    {
        private readonly MtlsTestCertificateBundle _bundle;
        private readonly IDisposable _material;

        internal AdapterFixture(
            SquirixReplicationServiceAdapter adapter,
            MtlsOptions mtls,
            MtlsTestCertificateBundle bundle,
            X509Certificate2 peerCertificate,
            MtlsCertificate material)
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
}
