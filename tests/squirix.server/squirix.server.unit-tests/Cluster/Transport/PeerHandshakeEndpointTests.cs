using System;
using System.Net;
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

/// <summary>Unit tests for the remote endpoint and the refusal reuse of <see cref="PeerHandshakeVerifier" />.</summary>
[Immutable]
public sealed class PeerHandshakeEndpointTests : ServerUnitTestBase
{
    private const int InboundEventId = 5009;
    private static readonly TimeSpan MaxAge = TimeSpan.FromMinutes(10);
    private static readonly TimeSpan RelogInterval = TimeSpan.FromMinutes(30);
    private static readonly string[] ConfiguredPeers = ["node-b"];

    /// <summary>Verifies the inbound refusal names the remote endpoint of the connection, and says it is unknown when none is given.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InboundRefusalNamesRemoteEndpoint(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-x");
        using var unknown = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-y");
        var log = new EventRecordingLogger();
        var verifier = Create(bundle, log, new FakeTimeProvider(TimeProvider.System.GetUtcNow()));

        _ = await Assert.That(verifier.VerifyConfiguredPeer(certificate, ConfiguredPeers, new IPEndPoint(IPAddress.Parse("203.0.113.7"), 51234))).IsFalse();
        _ = await Assert.That(verifier.VerifyConfiguredPeer(unknown, ConfiguredPeers)).IsFalse();

        _ = await Assert.That(log.Count(InboundEventId, "from 203.0.113.7:51234:")).IsEqualTo(1);
        _ = await Assert.That(log.Count(InboundEventId, "from unknown:")).IsEqualTo(1);
    }

    /// <summary>Verifies the remote endpoint is not part of the refusal identity, so a retry from another port is not logged again.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetryFromAnotherPortIsLoggedOnce(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-x");
        var log = new EventRecordingLogger();
        var verifier = Create(bundle, log, new FakeTimeProvider(TimeProvider.System.GetUtcNow()));

        _ = await Assert.That(verifier.VerifyConfiguredPeer(certificate, ConfiguredPeers, new IPEndPoint(IPAddress.Parse("203.0.113.7"), 51234))).IsFalse();
        _ = await Assert.That(verifier.VerifyConfiguredPeer(certificate, ConfiguredPeers, new IPEndPoint(IPAddress.Parse("203.0.113.7"), 51235))).IsFalse();

        _ = await Assert.That(log.Count(InboundEventId)).IsEqualTo(1);
    }

    /// <summary>Verifies control characters and excess length of the remote endpoint text are neutralized before logging.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoteEndpointIsSanitized(CancellationToken cancellationToken)
    {
        var endpoint = "10.0.0.1:1\r\nFORGED" + new string('a', PeerHandshakeVerifier.MaxLoggedNameLength * 2);
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-x");
        var log = new EventRecordingLogger();
        var verifier = Create(bundle, log, new FakeTimeProvider(TimeProvider.System.GetUtcNow()));

        _ = await Assert.That(verifier.VerifyConfiguredPeer(certificate, ConfiguredPeers, new TextEndPoint(endpoint))).IsFalse();

        var message = await Assert.That(log.FindMessage(InboundEventId)).IsNotNull();
        _ = await Assert.That(message.Contains('\r', StringComparison.Ordinal)).IsFalse();
        _ = await Assert.That(message.Contains('\n', StringComparison.Ordinal)).IsFalse();
        _ = await Assert.That(message.Contains("10.0.0.1:1??FORGED", StringComparison.Ordinal)).IsTrue();
        _ = await Assert.That(message.Contains(endpoint, StringComparison.Ordinal)).IsFalse();
    }

    /// <summary>Verifies a retried refusal builds one chain and a not yet valid certificate is accepted once it is valid.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetriedRefusalBuildsOneChain(CancellationToken cancellationToken)
    {
        var now = TimeProvider.System.GetUtcNow();
        var clock = new FakeTimeProvider(now.AddHours(-3));
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var other = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var untrusted = MtlsTestCertificateFactory.CreatePeerCertificate(other.Ca, "node-b");
        using var delayed = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b", now.AddHours(-1), now.AddHours(1));
        var chains = new PeerChainCache(bundle.Ca, PeerChainCache.DefaultCapacity, MaxAge);
        var verifier = new PeerHandshakeVerifier(chains, clock, new EventRecordingLogger(), PeerHandshakeVerifier.DefaultCapacity, RelogInterval);

        for (var attempt = 0; attempt < 3; attempt++)
            _ = await Assert.That(verifier.VerifyConfiguredPeer(untrusted, ConfiguredPeers)).IsFalse();

        _ = await Assert.That(chains.ChainBuilds).IsEqualTo(1L);
        _ = await Assert.That(verifier.VerifyConfiguredPeer(delayed, ConfiguredPeers)).IsFalse();
        clock.Advance(TimeSpan.FromHours(3));

        _ = await Assert.That(verifier.VerifyConfiguredPeer(delayed, ConfiguredPeers)).IsTrue();
    }

    private static PeerHandshakeVerifier Create(MtlsTestCertificateBundle bundle, EventRecordingLogger log, TimeProvider clock) =>
        new(new PeerChainCache(bundle.Ca, PeerChainCache.DefaultCapacity, MaxAge), clock, log, PeerHandshakeVerifier.DefaultCapacity, RelogInterval);

    /// <summary>An endpoint whose text is chosen by the test.</summary>
    [Immutable]
    private sealed class TextEndPoint : EndPoint
    {
        private readonly string _text;

        internal TextEndPoint(string text)
        {
            _text = text;
        }

        public override string ToString() => _text;
    }
}
