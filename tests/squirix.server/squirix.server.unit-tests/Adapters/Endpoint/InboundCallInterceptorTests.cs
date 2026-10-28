using System;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Runtime.Invocation;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Endpoint;

/// <summary>Covers the peer checks of the inbound call interceptor for internal owner calls.</summary>
[Immutable]
public sealed class InboundCallInterceptorTests : ServerUnitTestBase
{
    private const int InternalPort = 6001;
    private static readonly RingFingerprint LocalRing = RingFingerprint.Create("cluster", ["n1", "n2"], 128);

    /// <summary>Verifies a call without the internal owner header is not checked against the peers.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CallWithoutInternalHeaderSkipsPeerCheck(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-unknown");
        var chains = new PeerChainCache(bundle.Ca, PeerChainCache.DefaultCapacity, PeerChainCache.DefaultMaxAge);
        var peers = new VerifiedPeerCertificates(chains, ["node-b"], TimeProvider.System, NullLogger.Instance);
        var interceptor = InboundCallInterceptors.Create(new MtlsOptions { InternalListenPort = InternalPort }, peers, RingAgreements.Create());

        var response = await interceptor.UnaryServerHandler("request", CreateContext(certificate, false, null), static (_, _) => Task.FromResult("ok"));

        _ = await Assert.That(response).IsEqualTo("ok");
        _ = await Assert.That(chains.ChainBuilds).IsEqualTo(0L);
    }

    /// <summary>Verifies a trusted peer is served from one chain build per certificate and each certificate reports its own NodeId.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EachPeerCertificateReportsItsOwnNodeId(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var first = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");
        using var second = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-c");
        var chains = new PeerChainCache(bundle.Ca, PeerChainCache.DefaultCapacity, PeerChainCache.DefaultMaxAge);
        var peers = new VerifiedPeerCertificates(chains, ["node-b", "node-c"], TimeProvider.System, NullLogger.Instance);
        var calls = new (X509Certificate2 Certificate, string NodeId)[] { (first, "node-b"), (second, "node-c"), (first, "node-b"), (second, "node-c") };

        foreach (var (certificate, expected) in calls)
        {
            var ring = RingAgreements.Create();
            var interceptor = InboundCallInterceptors.Create(new MtlsOptions { InternalListenPort = InternalPort }, peers, ring);

            _ = await NodeAsyncAssert.ThrowsAsync<RpcException>(
                interceptor.UnaryServerHandler("request", CreateContext(certificate, true, "other-ring"), static (_, _) => Task.FromResult("ok")));

            var mismatch = await Assert.That(ring.FirstMismatch).IsNotNull();
            _ = await Assert.That(mismatch.PeerNodeId).IsEqualTo(expected);
        }

        _ = await Assert.That(chains.ChainBuilds).IsEqualTo(2L);
    }

    /// <summary>Verifies the internal owner header on a connection that did not arrive on the internal listener is refused.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OffListenerInternalHeaderIsRefused(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");
        var interceptor = InboundCallInterceptors.Create(new MtlsOptions { InternalListenPort = InternalPort }, CreatePeers(bundle, "node-b"), RingAgreements.Create());
        var httpContext = new DefaultHttpContext();
        httpContext.Connection.ClientCertificate = certificate;
        httpContext.Connection.LocalPort = InternalPort;
        var context = new TestServerCallContext(CreateHeaders(true, LocalRing.Value), httpContext);

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(interceptor.UnaryServerHandler("request", context, static (_, _) => Task.FromResult("ok")));

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unauthenticated);
    }

    /// <summary>Verifies a trusted peer with the same ring passes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TrustedPeerWithSameRingPasses(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");
        var interceptor = InboundCallInterceptors.Create(new MtlsOptions { InternalListenPort = InternalPort }, CreatePeers(bundle, "node-b"), RingAgreements.Create());

        var response = await interceptor.UnaryServerHandler("request", CreateContext(certificate, true, LocalRing.Value), static (_, _) => Task.FromResult("ok"));

        _ = await Assert.That(response).IsEqualTo("ok");
    }

    /// <summary>Verifies the internal owner header from an untrusted certificate is refused as unauthenticated.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UntrustedPeerInternalHeaderIsRefused(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-unknown");
        var interceptor = InboundCallInterceptors.Create(new MtlsOptions { InternalListenPort = InternalPort }, CreatePeers(bundle, "node-b"), RingAgreements.Create());

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            interceptor.UnaryServerHandler("request", CreateContext(certificate, true, LocalRing.Value), static (_, _) => Task.FromResult("ok")));

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unauthenticated);
    }

    private static TestServerCallContext CreateContext(X509Certificate2 certificate, bool internalOwner, string? ringFingerprint)
    {
        var httpContext = new DefaultHttpContext();
        httpContext.Connection.ClientCertificate = certificate;
        httpContext.Connection.LocalPort = InternalPort;
        httpContext.Features.Set(InternalListenerMarker.Instance);
        return new TestServerCallContext(CreateHeaders(internalOwner, ringFingerprint), httpContext);
    }

    private static Metadata CreateHeaders(bool internalOwner, string? ringFingerprint)
    {
        var headers = new Metadata();
        if (internalOwner)
            headers.Add(RemoteInvocationContract.InternalOwnerRpcHeaderName, RemoteInvocationContract.InternalOwnerRpcHeaderValue);

        if (ringFingerprint != null)
            headers.Add(RemoteInvocationContract.RingFingerprintHeaderName, ringFingerprint);

        return headers;
    }

    private static VerifiedPeerCertificates CreatePeers(MtlsTestCertificateBundle bundle, string nodeId) =>
        new(new PeerChainCache(bundle.Ca, PeerChainCache.DefaultCapacity, PeerChainCache.DefaultMaxAge), [nodeId], TimeProvider.System, NullLogger.Instance);
}
