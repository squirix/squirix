using System;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>Unit tests for <see cref="VerifiedPeerCertificates" />.</summary>
[Immutable]
public sealed class VerifiedPeerCertificatesTests : ServerUnitTestBase
{
    private const int ExpiredEventId = 5012;

    /// <summary>Verifies a certificate that expires while its connection is open is refused from its expiry on, without another chain build.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CertificateExpiringWhileOpenIsRefused(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(TimeProvider.System.GetUtcNow());
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b", clock.GetUtcNow().AddHours(-1), clock.GetUtcNow().AddHours(1));
        var chains = CreateChains(bundle);
        var peers = new VerifiedPeerCertificates(chains, ["node-b"], clock, NullLogger.Instance);

        _ = await Assert.That(peers.TryVerify(certificate, out _)).IsTrue();
        clock.Advance(TimeSpan.FromHours(2));

        _ = await Assert.That(peers.TryVerify(certificate, out _)).IsFalse();
        _ = await Assert.That(chains.ChainBuilds).IsEqualTo(1L);
    }

    /// <summary>Verifies a certificate that expires while its connection is open is logged once with its NodeId, however many calls are refused.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiryOnOpenConnectionIsLoggedOnce(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(TimeProvider.System.GetUtcNow());
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b", clock.GetUtcNow().AddHours(-1), clock.GetUtcNow().AddHours(1));
        var log = new EventRecordingLogger();
        var peers = new VerifiedPeerCertificates(CreateChains(bundle), ["node-b"], clock, log);

        _ = await Assert.That(peers.TryVerify(certificate, out _)).IsTrue();
        _ = await Assert.That(log.Count(ExpiredEventId)).IsEqualTo(0);
        clock.Advance(TimeSpan.FromHours(2));

        for (var call = 0; call < 3; call++)
            _ = await Assert.That(peers.TryVerify(certificate, out _)).IsFalse();

        _ = await Assert.That(log.Count(ExpiredEventId)).IsEqualTo(1);
        _ = await Assert.That(log.Count(ExpiredEventId, "node-b")).IsEqualTo(1);
        _ = await Assert.That(log.Count(ExpiredEventId, "expired while its connection was open")).IsEqualTo(1);
        var (level, _) = await Assert.That(log.Find(ExpiredEventId)).IsNotNull();
        _ = await Assert.That(level).IsEqualTo(LogLevel.Warning);
    }

    /// <summary>Verifies each connection certificate reports its own expiry once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiryIsLoggedPerConnectionCertificate(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(TimeProvider.System.GetUtcNow());
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var first = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b", clock.GetUtcNow().AddHours(-1), clock.GetUtcNow().AddHours(1));
        using var second = X509CertificateLoader.LoadCertificate(first.RawData);
        var log = new EventRecordingLogger();
        var peers = new VerifiedPeerCertificates(CreateChains(bundle), ["node-b"], clock, log);

        _ = await Assert.That(peers.TryVerify(first, out _)).IsTrue();
        _ = await Assert.That(peers.TryVerify(second, out _)).IsTrue();
        clock.Advance(TimeSpan.FromHours(2));
        _ = await Assert.That(peers.TryVerify(first, out _)).IsFalse();
        _ = await Assert.That(peers.TryVerify(second, out _)).IsFalse();
        _ = await Assert.That(peers.TryVerify(first, out _)).IsFalse();

        _ = await Assert.That(log.Count(ExpiredEventId)).IsEqualTo(2);
    }

    /// <summary>Verifies the factory requires the topology and the mTLS material.</summary>
    [Test]
    public void FactoryRequiresArguments()
    {
        var material = MtlsCertificate.Load(new MtlsOptions(), null, false);
        TopologyOptions? cluster = null;
        MtlsCertificate? missing = null;

        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(
            material,
            cluster,
            static (m, c) => _ = VerifiedPeerCertificates.CreateIfEnabled(c!, m, TimeProvider.System, NullLogger.Instance));
        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(
            missing,
            static m => _ = CreateWithTopology(m));
    }

    /// <summary>Verifies a certificate checked before its validity starts is refused at the clock's time and not remembered as refused.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NotYetValidCertificateIsRetried(CancellationToken cancellationToken)
    {
        var now = TimeProvider.System.GetUtcNow();
        var clock = new FakeTimeProvider(now.AddHours(-3));
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b", now.AddHours(-1), now.AddHours(1));
        var chains = CreateChains(bundle);
        var log = new EventRecordingLogger();
        var peers = new VerifiedPeerCertificates(chains, ["node-b"], clock, log);

        _ = await Assert.That(peers.TryVerify(certificate, out _)).IsFalse();
        clock.Advance(TimeSpan.FromHours(3));

        _ = await Assert.That(peers.TryVerify(certificate, out _)).IsTrue();
        _ = await Assert.That(chains.ChainBuilds).IsEqualTo(2L);
        _ = await Assert.That(log.Count(ExpiredEventId)).IsEqualTo(0);
    }

    /// <summary>Verifies a certificate of an unconfigured peer is refused.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnconfiguredPeerIsRefused(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-unknown");
        var chains = CreateChains(bundle);
        var peers = new VerifiedPeerCertificates(chains, ["node-b"], TimeProvider.System, NullLogger.Instance);
        var otherPeers = new VerifiedPeerCertificates(chains, ["node-unknown"], TimeProvider.System, NullLogger.Instance);

        _ = await Assert.That(otherPeers.TryVerify(certificate, out _)).IsTrue();
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
        var chains = CreateChains(bundle);
        var peers = new VerifiedPeerCertificates(chains, ["node-b"], TimeProvider.System, NullLogger.Instance);

        _ = await Assert.That(peers.TryVerify(certificate, out _)).IsFalse();
    }

    /// <summary>Verifies later calls with the certificate of a verified connection build no chain and report its NodeId.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VerifiedCertificateBuildsOneChain(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");
        var chains = CreateChains(bundle);
        var peers = new VerifiedPeerCertificates(chains, ["node-b", "node-c"], TimeProvider.System, NullLogger.Instance);

        for (var i = 0; i < 3; i++)
        {
            _ = await Assert.That(peers.TryVerify(certificate, out var nodeId)).IsTrue();
            _ = await Assert.That(nodeId).IsEqualTo("node-b");
        }

        _ = await Assert.That(chains.ChainBuilds).IsEqualTo(1L);
    }

    private static VerifiedPeerCertificates? CreateWithTopology(MtlsCertificate? material) => VerifiedPeerCertificates.CreateIfEnabled(
        new TopologyOptions(new ServerPeer { NodeId = "node-b", Uri = new Uri("https://localhost:6001") }),
        material!,
        TimeProvider.System,
        NullLogger.Instance);

    private static PeerChainCache CreateChains(MtlsTestCertificateBundle bundle) => new(bundle.Ca, PeerChainCache.DefaultCapacity, PeerChainCache.DefaultMaxAge);
}
