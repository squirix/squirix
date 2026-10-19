using System;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>Unit tests for <see cref="PeerHandshakeVerifier" />.</summary>
[Immutable]
public sealed class PeerHandshakeVerifierTests : ServerUnitTestBase
{
    private const int InboundEventId = 5009;
    private const int OutboundEventId = 5010;
    private const int SuppressedEventId = 5011;
    private const int ConcurrentCalls = 16;
    private static readonly TimeSpan MaxAge = TimeSpan.FromMinutes(10);
    private static readonly TimeSpan RelogInterval = TimeSpan.FromMinutes(30);
    private static readonly string[] ConfiguredPeers = ["node-b"];

    /// <summary>Verifies a refused certificate is logged once across retries.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusedCertificateIsLoggedOnce(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var other = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(other.Ca, "node-b");
        var log = new EventRecordingLogger();
        var verifier = Create(bundle, log, new FakeTimeProvider(TimeProvider.System.GetUtcNow()));

        for (var attempt = 0; attempt < 3; attempt++)
            _ = await Assert.That(verifier.VerifyConfiguredPeer(certificate, ConfiguredPeers)).IsFalse();

        _ = await Assert.That(log.Count(InboundEventId)).IsEqualTo(1);
    }

    /// <summary>Verifies the message names the reason and the thumbprint and the entry is a warning.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MessageNamesReasonAndThumbprint(CancellationToken cancellationToken)
    {
        var now = TimeProvider.System.GetUtcNow();
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b", now.AddHours(-2), now.AddHours(-1));
        var log = new EventRecordingLogger();
        var verifier = Create(bundle, log, new FakeTimeProvider(now));

        _ = await Assert.That(verifier.VerifyConfiguredPeer(certificate, ConfiguredPeers)).IsFalse();

        var message = await Assert.That(log.FindMessage(InboundEventId)).IsNotNull();
        _ = await Assert.That(message.Contains("cluster trust root", StringComparison.Ordinal)).IsTrue();
        _ = await Assert.That(message.Contains("valid from", StringComparison.Ordinal)).IsTrue();
        _ = await Assert.That(message.Contains(certificate.GetCertHashString(HashAlgorithmName.SHA256), StringComparison.Ordinal)).IsTrue();
        var (level, _) = await Assert.That(log.Find(InboundEventId)).IsNotNull();
        _ = await Assert.That(level).IsEqualTo(LogLevel.Warning);
    }

    /// <summary>Verifies a trusted certificate that names an unconfigured node is logged with the presented NodeId.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnconfiguredNodeIdIsLogged(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-x");
        var log = new EventRecordingLogger();
        var verifier = Create(bundle, log, new FakeTimeProvider(TimeProvider.System.GetUtcNow()));

        _ = await Assert.That(verifier.VerifyConfiguredPeer(certificate, ConfiguredPeers)).IsFalse();

        var message = await Assert.That(log.FindMessage(InboundEventId)).IsNotNull();
        _ = await Assert.That(message.Contains("'node-x'", StringComparison.Ordinal)).IsTrue();
        _ = await Assert.That(message.Contains("not a configured peer", StringComparison.Ordinal)).IsTrue();
    }

    /// <summary>Verifies an outbound certificate of the wrong node logs the outbound event naming the expected peer.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnexpectedPeerIsLoggedWithPeerName(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-x");
        var log = new EventRecordingLogger();
        var verifier = Create(bundle, log, new FakeTimeProvider(TimeProvider.System.GetUtcNow()));

        _ = await Assert.That(verifier.VerifyExpectedPeer(certificate, "node-b")).IsFalse();

        var message = await Assert.That(log.FindMessage(OutboundEventId)).IsNotNull();
        _ = await Assert.That(message.Contains("peer node-b", StringComparison.Ordinal)).IsTrue();
        _ = await Assert.That(message.Contains("'node-x'", StringComparison.Ordinal)).IsTrue();
        _ = await Assert.That(log.Count(InboundEventId)).IsEqualTo(0);
    }

    /// <summary>Verifies a refusal for another expected peer is logged again.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ChangedExpectedPeerIsLoggedAgain(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-x");
        var log = new EventRecordingLogger();
        var verifier = Create(bundle, log, new FakeTimeProvider(TimeProvider.System.GetUtcNow()));

        _ = await Assert.That(verifier.VerifyExpectedPeer(certificate, "node-b")).IsFalse();
        _ = await Assert.That(verifier.VerifyExpectedPeer(certificate, "node-c")).IsFalse();
        _ = await Assert.That(verifier.VerifyConfiguredPeer(certificate, ConfiguredPeers)).IsFalse();

        _ = await Assert.That(log.Count(OutboundEventId)).IsEqualTo(2);
        _ = await Assert.That(log.Count(InboundEventId)).IsEqualTo(1);
    }

    /// <summary>Verifies a refusal of the same certificate and direction whose reason changed is logged again before the relog interval.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ChangedReasonIsLoggedAgain(CancellationToken cancellationToken)
    {
        var now = TimeProvider.System.GetUtcNow();
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-x", now.AddHours(-1), now.AddMinutes(1));
        var log = new EventRecordingLogger();
        var clock = new FakeTimeProvider(now);
        var verifier = Create(bundle, log, clock);

        _ = await Assert.That(verifier.VerifyConfiguredPeer(certificate, ConfiguredPeers)).IsFalse();
        _ = await Assert.That(log.Count(InboundEventId, "not a configured peer")).IsEqualTo(1);

        clock.Advance(TimeSpan.FromMinutes(2));
        _ = await Assert.That(verifier.VerifyConfiguredPeer(certificate, ConfiguredPeers)).IsFalse();

        _ = await Assert.That(log.Count(InboundEventId)).IsEqualTo(2);
        _ = await Assert.That(log.Count(InboundEventId, "NotTimeValid")).IsEqualTo(1);
    }

    /// <summary>Verifies the refusal of a serverAuth-only certificate names the usage hint.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ServerAuthOnlyCertificateLogsUsageHint(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificateWithPurposes(bundle.Ca, "node-b", [MtlsCertificateIdentity.ServerAuthOid]);
        var log = new EventRecordingLogger();
        var verifier = Create(bundle, log, new FakeTimeProvider(TimeProvider.System.GetUtcNow()));

        _ = await Assert.That(verifier.VerifyConfiguredPeer(certificate, ConfiguredPeers)).IsFalse();

        var message = await Assert.That(log.FindMessage(InboundEventId)).IsNotNull();
        _ = await Assert.That(message.Contains("NotValidForUsage", StringComparison.Ordinal)).IsTrue();
        _ = await Assert.That(message.Contains("must list both serverAuth and clientAuth", StringComparison.Ordinal)).IsTrue();
    }

    /// <summary>Verifies the refusal of a clientAuth-only certificate names the usage hint.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ClientAuthOnlyCertificateLogsUsageHint(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificateWithPurposes(bundle.Ca, "node-b", [MtlsCertificateIdentity.ClientAuthOid]);
        var log = new EventRecordingLogger();
        var verifier = Create(bundle, log, new FakeTimeProvider(TimeProvider.System.GetUtcNow()));

        _ = await Assert.That(verifier.VerifyExpectedPeer(certificate, "node-b")).IsFalse();

        var message = await Assert.That(log.FindMessage(OutboundEventId)).IsNotNull();
        _ = await Assert.That(message.Contains("NotValidForUsage", StringComparison.Ordinal)).IsTrue();
        _ = await Assert.That(message.Contains("must list both serverAuth and clientAuth", StringComparison.Ordinal)).IsTrue();
    }

    /// <summary>Verifies refusals beyond the per-interval cap are not logged and the next interval reports their count once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusalsBeyondCapAreSummarized(CancellationToken cancellationToken)
    {
        const int cap = 2;
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var first = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-x");
        using var second = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-y");
        using var third = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-z");
        using var fourth = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-w");
        var log = new EventRecordingLogger();
        var clock = new FakeTimeProvider(TimeProvider.System.GetUtcNow());
        var verifier = new PeerHandshakeVerifier(
            new PeerChainCache(bundle.Ca, PeerChainCache.DefaultCapacity, MaxAge),
            clock,
            log,
            PeerHandshakeVerifier.DefaultCapacity,
            RelogInterval,
            cap);

        _ = verifier.VerifyConfiguredPeer(first, ConfiguredPeers);
        _ = verifier.VerifyConfiguredPeer(second, ConfiguredPeers);
        _ = verifier.VerifyConfiguredPeer(third, ConfiguredPeers);
        _ = verifier.VerifyConfiguredPeer(fourth, ConfiguredPeers);

        _ = await Assert.That(log.Count(InboundEventId)).IsEqualTo(cap);
        _ = await Assert.That(log.Count(SuppressedEventId)).IsEqualTo(0);

        clock.Advance(RelogInterval);
        _ = verifier.VerifyConfiguredPeer(third, ConfiguredPeers);
        _ = verifier.VerifyConfiguredPeer(fourth, ConfiguredPeers);

        _ = await Assert.That(log.Count(SuppressedEventId)).IsEqualTo(1);
        _ = await Assert.That(log.Count(SuppressedEventId, "Suppressed 2 further")).IsEqualTo(1);
        _ = await Assert.That(log.Count(InboundEventId)).IsEqualTo(cap + cap);
        var (level, _) = await Assert.That(log.Find(SuppressedEventId)).IsNotNull();
        _ = await Assert.That(level).IsEqualTo(LogLevel.Warning);
    }

    /// <summary>Verifies control characters of a subject are replaced before logging.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SubjectControlCharactersAreReplaced(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-x\r\nFORGED");
        var log = new EventRecordingLogger();
        var verifier = Create(bundle, log, new FakeTimeProvider(TimeProvider.System.GetUtcNow()));

        _ = await Assert.That(verifier.VerifyConfiguredPeer(certificate, ConfiguredPeers)).IsFalse();

        var message = await Assert.That(log.FindMessage(InboundEventId)).IsNotNull();
        _ = await Assert.That(message.Contains('\r', StringComparison.Ordinal)).IsFalse();
        _ = await Assert.That(message.Contains('\n', StringComparison.Ordinal)).IsFalse();
        _ = await Assert.That(message.Contains("node-x??FORGED", StringComparison.Ordinal)).IsTrue();
    }

    /// <summary>Verifies a long subject is cut and marked.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LongSubjectIsTruncated(CancellationToken cancellationToken)
    {
        var longName = new string('a', PeerHandshakeVerifier.MaxLoggedNameLength * 2);
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, longName);
        var log = new EventRecordingLogger();
        var verifier = Create(bundle, log, new FakeTimeProvider(TimeProvider.System.GetUtcNow()));

        _ = await Assert.That(verifier.VerifyConfiguredPeer(certificate, ConfiguredPeers)).IsFalse();

        var message = await Assert.That(log.FindMessage(InboundEventId)).IsNotNull();
        _ = await Assert.That(message.Contains(longName, StringComparison.Ordinal)).IsFalse();
        _ = await Assert.That(message.Contains("...", StringComparison.Ordinal)).IsTrue();
    }

    /// <summary>Verifies a refusal is logged again exactly when the relog interval has passed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusalIsLoggedAgainAtInterval(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-x");
        var log = new EventRecordingLogger();
        var clock = new FakeTimeProvider(TimeProvider.System.GetUtcNow());
        var verifier = Create(bundle, log, clock);

        _ = await Assert.That(verifier.VerifyConfiguredPeer(certificate, ConfiguredPeers)).IsFalse();
        clock.Advance(RelogInterval - TimeSpan.FromTicks(1));
        _ = await Assert.That(verifier.VerifyConfiguredPeer(certificate, ConfiguredPeers)).IsFalse();
        _ = await Assert.That(log.Count(InboundEventId)).IsEqualTo(1);

        clock.Advance(TimeSpan.FromTicks(1));
        _ = await Assert.That(verifier.VerifyConfiguredPeer(certificate, ConfiguredPeers)).IsFalse();

        _ = await Assert.That(log.Count(InboundEventId)).IsEqualTo(2);
    }

    /// <summary>Verifies a refusal is logged again when the clock steps back.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ClockStepBackLogsAgain(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-x");
        var log = new EventRecordingLogger();
        var clock = new SteppableClock(TimeProvider.System.GetUtcNow());
        var verifier = Create(bundle, log, clock);

        _ = await Assert.That(verifier.VerifyConfiguredPeer(certificate, ConfiguredPeers)).IsFalse();
        clock.Now -= TimeSpan.FromMinutes(1);
        _ = await Assert.That(verifier.VerifyConfiguredPeer(certificate, ConfiguredPeers)).IsFalse();

        _ = await Assert.That(log.Count(InboundEventId)).IsEqualTo(2);
    }

    /// <summary>Verifies accepted certificates log nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AcceptedCertificateLogsNothing(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");
        var log = new EventRecordingLogger();
        var verifier = Create(bundle, log, new FakeTimeProvider(TimeProvider.System.GetUtcNow()));

        _ = await Assert.That(verifier.VerifyConfiguredPeer(certificate, ConfiguredPeers)).IsTrue();
        _ = await Assert.That(verifier.VerifyExpectedPeer(certificate, "node-b")).IsTrue();

        _ = await Assert.That(log.Count(InboundEventId)).IsEqualTo(0);
        _ = await Assert.That(log.Count(OutboundEventId)).IsEqualTo(0);
    }

    /// <summary>Verifies concurrent refusals of one certificate log once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentRefusalsLogOnce(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-x");
        var log = new EventRecordingLogger();
        var verifier = Create(bundle, log, new FakeTimeProvider(TimeProvider.System.GetUtcNow()));
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tasks = new Task<bool>[ConcurrentCalls];
        for (var i = 0; i < tasks.Length; i++)
            tasks[i] = VerifyAfterAsync(gate.Task, verifier, certificate.RawData);

        _ = gate.TrySetResult();
        var results = await Task.WhenAll(tasks);

        foreach (var result in results)
            _ = await Assert.That(result).IsFalse();

        _ = await Assert.That(log.Count(InboundEventId)).IsEqualTo(1);
    }

    /// <summary>Verifies a full table starts over, so an evicted refusal is logged again.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FullTableStartsOver(CancellationToken cancellationToken)
    {
        const int capacity = 2;
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var first = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-x");
        using var second = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-y");
        using var third = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-z");
        var log = new EventRecordingLogger();
        var verifier = new PeerHandshakeVerifier(
            new PeerChainCache(bundle.Ca, PeerChainCache.DefaultCapacity, MaxAge),
            new FakeTimeProvider(TimeProvider.System.GetUtcNow()),
            log,
            capacity,
            RelogInterval);

        _ = verifier.VerifyConfiguredPeer(first, ConfiguredPeers);
        _ = verifier.VerifyConfiguredPeer(second, ConfiguredPeers);
        _ = verifier.VerifyConfiguredPeer(third, ConfiguredPeers);
        _ = verifier.VerifyConfiguredPeer(first, ConfiguredPeers);

        _ = await Assert.That(log.Count(InboundEventId)).IsEqualTo(4);
    }

    private static async Task<bool> VerifyAfterAsync(Task gate, PeerHandshakeVerifier verifier, byte[] rawCertificate)
    {
        await gate;
        using var certificate = X509CertificateLoader.LoadCertificate(rawCertificate);
        return verifier.VerifyConfiguredPeer(certificate, ConfiguredPeers);
    }

    private static PeerHandshakeVerifier Create(MtlsTestCertificateBundle bundle, EventRecordingLogger log, TimeProvider clock) =>
        new(new PeerChainCache(bundle.Ca, PeerChainCache.DefaultCapacity, MaxAge), clock, log, PeerHandshakeVerifier.DefaultCapacity, RelogInterval);

    /// <summary>A clock that can be set to any time, including an earlier one.</summary>
    [Mutable]
    private sealed class SteppableClock : TimeProvider
    {
        internal SteppableClock(DateTimeOffset now)
        {
            Now = now;
        }

        internal DateTimeOffset Now { get; set; }

        public override DateTimeOffset GetUtcNow() => Now;
    }
}
