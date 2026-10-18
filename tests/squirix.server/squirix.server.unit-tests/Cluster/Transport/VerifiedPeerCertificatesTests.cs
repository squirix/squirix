using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>Unit tests for <see cref="VerifiedPeerCertificates" />.</summary>
[Immutable]
public sealed class VerifiedPeerCertificatesTests : ServerUnitTestBase
{
    /// <summary>Verifies a certificate that expires while its connection is open is refused from its expiry on, without another chain build.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CertificateExpiringWhileOpenIsRefused(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(TimeProvider.System.GetUtcNow());
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b", clock.GetUtcNow().AddHours(-1), clock.GetUtcNow().AddHours(1));
        var peers = new VerifiedPeerCertificates(bundle.Ca, ["node-b"], clock);

        _ = await Assert.That(peers.TryVerify(certificate, out _)).IsTrue();
        clock.Advance(TimeSpan.FromHours(2));

        _ = await Assert.That(peers.TryVerify(certificate, out _)).IsFalse();
        _ = await Assert.That(peers.ChainBuilds).IsEqualTo(1L);
    }

    /// <summary>Verifies a certificate of an unconfigured peer is refused.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnconfiguredPeerIsRefused(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-unknown");
        var peers = new VerifiedPeerCertificates(bundle.Ca, ["node-b"], TimeProvider.System);

        _ = await Assert.That(peers.TryVerify(certificate, out _)).IsFalse();
    }

    /// <summary>Verifies a certificate issued by another authority is refused.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UntrustedCertificateIsRefused(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var other = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(other.Ca, "node-b");
        var peers = new VerifiedPeerCertificates(bundle.Ca, ["node-b"], TimeProvider.System);

        _ = await Assert.That(peers.TryVerify(certificate, out _)).IsFalse();
    }

    /// <summary>Verifies later calls with the certificate of a verified connection build no chain and report its NodeId.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VerifiedCertificateBuildsOneChain(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");
        var peers = new VerifiedPeerCertificates(bundle.Ca, ["node-b", "node-c"], TimeProvider.System);

        for (var i = 0; i < 3; i++)
        {
            _ = await Assert.That(peers.TryVerify(certificate, out var nodeId)).IsTrue();
            _ = await Assert.That(nodeId).IsEqualTo("node-b");
        }

        _ = await Assert.That(peers.ChainBuilds).IsEqualTo(1L);
    }
}
