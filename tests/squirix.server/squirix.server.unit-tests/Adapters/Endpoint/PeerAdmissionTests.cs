using System;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Adapters.Wire.Replication;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Binary;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Endpoint;

/// <summary>Unit tests for native peer listener admission.</summary>
[Immutable]
public sealed class PeerAdmissionTests : ServerUnitTestBase
{
    private const int InternalPort = 7443;

    /// <summary>Verifies a connection on the internal listener with a verified peer certificate is served by the internal endpoint only.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VerifiedPeerIsAdmitted(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");

        var internalValid = Admit(CreatePeers(bundle), InternalPort, true, InternalPort, certificate);

        _ = await Assert.That(internalValid).IsEqualTo(FrameStatus.Ok);
    }

    /// <summary>Verifies an internal listener connection without a client certificate falls to the externally authorized endpoint.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NoCertificateIsRejected(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);

        var internalValid = Admit(CreatePeers(bundle), InternalPort, true, InternalPort, null);

        _ = await Assert.That(internalValid).IsEqualTo(FrameStatus.Unauthenticated);
    }

    /// <summary>Verifies a public listener connection with a certificate falls to the externally authorized endpoint.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublicPortIsRejected(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");

        var internalValid = Admit(CreatePeers(bundle), InternalPort + 1, false, InternalPort, certificate);

        _ = await Assert.That(internalValid).IsEqualTo(FrameStatus.PermissionDenied);
    }

    /// <summary>Verifies a connection on the internal port that the internal listener did not mark, or with no local port, falls to the twin.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnmarkedConnectionIsRejected(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");
        var peers = CreatePeers(bundle);

        var unmarkedInternal = Admit(peers, InternalPort, false, InternalPort, certificate);
        var noPortInternal = Admit(peers, 0, true, InternalPort, certificate);

        _ = await Assert.That(unmarkedInternal).IsEqualTo(FrameStatus.PermissionDenied);
        _ = await Assert.That(noPortInternal).IsEqualTo(FrameStatus.PermissionDenied);
    }

    /// <summary>Verifies endpoints carrying a zero internal port never select the internal endpoint.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ZeroInternalPortIsRejected(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");

        var internalValid = Admit(CreatePeers(bundle), 0, true, 0, certificate);

        _ = await Assert.That(internalValid).IsEqualTo(FrameStatus.Unavailable);
    }

    /// <summary>Verifies a certificate of a peer outside the configured set, or a policy without peer trust, falls to the twin.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnverifiedPeerIsRejected(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var removed = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-removed");
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");

        var removedInternal = Admit(CreatePeers(bundle), InternalPort, true, InternalPort, removed);
        var noPeersInternal = Admit(null, InternalPort, true, InternalPort, certificate);

        _ = await Assert.That(removedInternal).IsEqualTo(FrameStatus.Unauthenticated);
        _ = await Assert.That(noPeersInternal).IsEqualTo(FrameStatus.Unavailable);
    }

    private static FrameStatus Admit(VerifiedPeerCertificates? peers, int localPort, bool marked, int metadataPort, X509Certificate2? certificate)
    {
        var context = new PeerCallContext { LocalPort = localPort, InternalListener = marked, ClientCertificate = certificate };
        try
        {
            _ = SquirixReplicationServiceAdapter.PeerAuth.EnsureTrustedPeer(context, new MtlsOptions { InternalListenPort = metadataPort }, peers, "node-b");
            return FrameStatus.Ok;
        }
        catch (ServerCallException exception)
        {
            return exception.StatusCode;
        }
    }

    private static VerifiedPeerCertificates CreatePeers(MtlsTestCertificateBundle bundle)
        => new(new PeerChainCache(bundle.Ca, PeerChainCache.DefaultCapacity, PeerChainCache.DefaultMaxAge), ["node-b"], TimeProvider.System, NullLogger.Instance);
}
