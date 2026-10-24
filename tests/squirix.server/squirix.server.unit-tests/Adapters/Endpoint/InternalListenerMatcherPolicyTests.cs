using System;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing.Matching;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Endpoint;

/// <summary>Unit tests for <see cref="InternalListenerMatcherPolicy" />.</summary>
[Immutable]
public sealed class InternalListenerMatcherPolicyTests : ServerUnitTestBase
{
    private const int InternalPort = 7443;

    /// <summary>Verifies a connection on the internal listener with a verified peer certificate is served by the internal endpoint only.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VerifiedPeerSelectsInternal(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");

        var (internalValid, twinValid) = await ApplyAsync(CreatePeers(bundle), InternalPort, true, InternalPort, certificate);

        _ = await Assert.That(internalValid).IsTrue();
        _ = await Assert.That(twinValid).IsFalse();
    }

    /// <summary>Verifies an internal listener connection without a client certificate falls to the externally authorized endpoint.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NoCertificateSelectsTwin(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);

        var (internalValid, twinValid) = await ApplyAsync(CreatePeers(bundle), InternalPort, true, InternalPort, null);

        _ = await Assert.That(internalValid).IsFalse();
        _ = await Assert.That(twinValid).IsTrue();
    }

    /// <summary>Verifies a public listener connection with a certificate falls to the externally authorized endpoint.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublicPortSelectsTwin(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");

        var (internalValid, twinValid) = await ApplyAsync(CreatePeers(bundle), InternalPort + 1, false, InternalPort, certificate);

        _ = await Assert.That(internalValid).IsFalse();
        _ = await Assert.That(twinValid).IsTrue();
    }

    /// <summary>Verifies a connection on the internal port that the internal listener did not mark, or with no local port, falls to the twin.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnmarkedConnectionSelectsTwin(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");
        var peers = CreatePeers(bundle);

        var (unmarkedInternal, unmarkedTwin) = await ApplyAsync(peers, InternalPort, false, InternalPort, certificate);
        var (noPortInternal, noPortTwin) = await ApplyAsync(peers, 0, true, InternalPort, certificate);

        _ = await Assert.That(unmarkedInternal).IsFalse();
        _ = await Assert.That(unmarkedTwin).IsTrue();
        _ = await Assert.That(noPortInternal).IsFalse();
        _ = await Assert.That(noPortTwin).IsTrue();
    }

    /// <summary>Verifies endpoints carrying a zero internal port never select the internal endpoint.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ZeroInternalPortSelectsTwin(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");

        var (internalValid, twinValid) = await ApplyAsync(CreatePeers(bundle), 0, true, 0, certificate);

        _ = await Assert.That(internalValid).IsFalse();
        _ = await Assert.That(twinValid).IsTrue();
    }

    /// <summary>Verifies a certificate of a peer outside the configured set, or a policy without peer trust, falls to the twin.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnverifiedPeerSelectsTwin(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var removed = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-removed");
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");

        var (removedInternal, removedTwin) = await ApplyAsync(CreatePeers(bundle), InternalPort, true, InternalPort, removed);
        var (noPeersInternal, noPeersTwin) = await ApplyAsync(null, InternalPort, true, InternalPort, certificate);

        _ = await Assert.That(removedInternal).IsFalse();
        _ = await Assert.That(removedTwin).IsTrue();
        _ = await Assert.That(noPeersInternal).IsFalse();
        _ = await Assert.That(noPeersTwin).IsTrue();
    }

    private static async Task<(bool InternalValid, bool TwinValid)> ApplyAsync(
        VerifiedPeerCertificates? peers,
        int localPort,
        bool marked,
        int metadataPort,
        X509Certificate2? certificate)
    {
        var httpContext = new DefaultHttpContext();
        httpContext.Connection.LocalPort = localPort;
        httpContext.Connection.ClientCertificate = certificate;
        if (marked)
            httpContext.Features.Set(InternalListenerMarker.Instance);

        Microsoft.AspNetCore.Http.Endpoint[] endpoints =
        [
            new(null, new EndpointMetadataCollection(new InternalListenerEndpointMetadata(metadataPort, true)), "internal"),
            new(null, new EndpointMetadataCollection(new InternalListenerEndpointMetadata(metadataPort, false)), "twin"),
        ];
        var candidates = new CandidateSet(endpoints, [[], []], [0, 0]);

        await new InternalListenerMatcherPolicy(peers).ApplyAsync(httpContext, candidates);

        return (candidates.IsValidCandidate(0), candidates.IsValidCandidate(1));
    }

    private static VerifiedPeerCertificates CreatePeers(MtlsTestCertificateBundle bundle)
        => new(new PeerChainCache(bundle.Ca, PeerChainCache.DefaultCapacity, PeerChainCache.DefaultMaxAge), ["node-b"], TimeProvider.System);
}
