using System;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>Unit tests for <see cref="PeerChainCache" />.</summary>
[Immutable]
public sealed class PeerChainCacheTests : ServerUnitTestBase
{
    private const int BackdateDays = 1;
    private const string CodeSigningOid = "1.3.6.1.5.5.7.3.3";
    private const int ValidityDays = 30;

    private static readonly TimeSpan MaxAge = TimeSpan.FromMinutes(10);

    /// <summary>Verifies an accepted certificate reports no refusal, also from the cache.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AcceptedCertificateReportsNoRefusal(CancellationToken cancellationToken)
    {
        var now = TimeProvider.System.GetUtcNow();
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");
        var cache = new PeerChainCache(bundle.Ca, PeerChainCache.DefaultCapacity, MaxAge);

        _ = await Assert.That(cache.TryVerify(certificate, now, out _, out var built)).IsTrue();
        _ = await Assert.That(cache.TryVerify(certificate, now, out _, out var cached)).IsTrue();

        _ = await Assert.That(built).IsEqualTo(default);
        _ = await Assert.That(cached).IsEqualTo(default);
    }

    /// <summary>Verifies a certificate with both roles is refused when its authority allows only one of them.</summary>
    [Test]
    public async Task AuthorityWithoutBothRolesIsRefused()
    {
        using var authority = MtlsTestCertificateFactory.CreateAuthorityWithPurposes("CN=Squirix Server Only CA", [MtlsCertificateIdentity.ServerAuthOid]);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificateWithPurposes(
            authority,
            "node-b",
            [MtlsCertificateIdentity.ServerAuthOid, MtlsCertificateIdentity.ClientAuthOid]);
        var cache = new PeerChainCache(authority, PeerChainCache.DefaultCapacity, MaxAge);

        _ = await Assert.That(cache.TryVerify(certificate, TimeProvider.System.GetUtcNow(), out _)).IsFalse();
    }

    /// <summary>Verifies the number of remembered certificates never exceeds the capacity and an evicted certificate is built again.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CapacityBoundsEntries(CancellationToken cancellationToken)
    {
        const int capacity = 2;
        var now = TimeProvider.System.GetUtcNow();
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var first = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");
        using var second = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-c");
        using var third = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-d");
        var cache = new PeerChainCache(bundle.Ca, capacity, MaxAge);

        _ = await Assert.That(cache.TryVerify(first, now, out _)).IsTrue();
        _ = await Assert.That(cache.TryVerify(second, now, out _)).IsTrue();
        _ = await Assert.That(cache.TryVerify(third, now, out _)).IsTrue();
        _ = await Assert.That(cache.Count).IsLessThanOrEqualTo(capacity);
        _ = await Assert.That(cache.TryVerify(first, now, out _)).IsTrue();

        _ = await Assert.That(cache.ChainBuilds).IsEqualTo(4L);
    }

    /// <summary>Verifies a certificate of another authority is refused, also when the cache of that authority has accepted it before.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CertificateOfOtherAuthorityIsRefused(CancellationToken cancellationToken)
    {
        var now = TimeProvider.System.GetUtcNow();
        using var bundleA = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var bundleB = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificateA = MtlsTestCertificateFactory.CreatePeerCertificate(bundleA.Ca, "node-b");
        using var certificateB = MtlsTestCertificateFactory.CreatePeerCertificate(bundleB.Ca, "node-b");
        var cacheA = new PeerChainCache(bundleA.Ca, PeerChainCache.DefaultCapacity, MaxAge);
        var cacheB = new PeerChainCache(bundleB.Ca, PeerChainCache.DefaultCapacity, MaxAge);

        _ = await Assert.That(cacheA.TryVerify(certificateA, now, out _)).IsTrue();
        _ = await Assert.That(cacheA.TryVerify(certificateB, now, out _)).IsFalse();
        _ = await Assert.That(cacheB.TryVerify(certificateA, now, out _)).IsFalse();
        _ = await Assert.That(cacheB.TryVerify(certificateB, now, out _)).IsTrue();
    }

    /// <summary>Verifies a certificate listing both server and client authentication is accepted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CertificateWithBothRolesIsAccepted(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificateWithPurposes(bundle.Ca, "node-b", [MtlsCertificateIdentity.ServerAuthOid, MtlsCertificateIdentity.ClientAuthOid]);
        var cache = new PeerChainCache(bundle.Ca, PeerChainCache.DefaultCapacity, MaxAge);

        _ = await Assert.That(cache.TryVerify(certificate, TimeProvider.System.GetUtcNow(), out _)).IsTrue();
    }

    /// <summary>Verifies a certificate that lists both roles besides another purpose is accepted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CertificateWithExtraPurposeIsAccepted(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificateWithPurposes(
            bundle.Ca,
            "node-b",
            [MtlsCertificateIdentity.ServerAuthOid, MtlsCertificateIdentity.ClientAuthOid, CodeSigningOid]);
        var cache = new PeerChainCache(bundle.Ca, PeerChainCache.DefaultCapacity, MaxAge);

        _ = await Assert.That(cache.TryVerify(certificate, TimeProvider.System.GetUtcNow(), out _)).IsTrue();
    }

    /// <summary>Verifies a certificate that names its key usage without both roles is refused and not remembered.</summary>
    /// <param name="purpose">The only extended key usage of the certificate.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(MtlsCertificateIdentity.ServerAuthOid)]
    [Arguments(MtlsCertificateIdentity.ClientAuthOid)]
    [Arguments(CodeSigningOid)]
    public async Task CertificateWithoutBothRolesIsRefused(string purpose, CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificateWithPurposes(bundle.Ca, "node-b", [purpose]);
        var cache = new PeerChainCache(bundle.Ca, PeerChainCache.DefaultCapacity, MaxAge);

        _ = await Assert.That(cache.TryVerify(certificate, TimeProvider.System.GetUtcNow(), out _)).IsFalse();
        _ = await Assert.That(cache.Count).IsEqualTo(0);
    }

    /// <summary>Verifies a certificate without an extended key usage extension, which is valid for any purpose, is accepted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CertificateWithoutKeyUsageIsAccepted(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificateWithPurposes(bundle.Ca, "node-b", null);
        var cache = new PeerChainCache(bundle.Ca, PeerChainCache.DefaultCapacity, MaxAge);

        _ = await Assert.That(cache.TryVerify(certificate, TimeProvider.System.GetUtcNow(), out _)).IsTrue();
    }

    /// <summary>Verifies an entry older than the maximum age is rebuilt although the certificate is still valid.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EntryOlderThanMaxAgeIsRebuilt(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(TimeProvider.System.GetUtcNow());
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");
        var cache = new PeerChainCache(bundle.Ca, PeerChainCache.DefaultCapacity, MaxAge);

        _ = await Assert.That(cache.TryVerify(certificate, clock.GetUtcNow(), out _)).IsTrue();
        clock.Advance(MaxAge + TimeSpan.FromSeconds(1));

        _ = await Assert.That(cache.TryVerify(certificate, clock.GetUtcNow(), out _)).IsTrue();
        _ = await Assert.That(cache.ChainBuilds).IsEqualTo(2L);
    }

    /// <summary>Verifies a cached certificate is rebuilt past the end of its validity and then refused and forgotten.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredCertificateIsRebuiltAndRefused(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(TimeProvider.System.GetUtcNow());
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b", clock.GetUtcNow().AddHours(-1), clock.GetUtcNow().AddHours(1));
        var cache = new PeerChainCache(bundle.Ca, PeerChainCache.DefaultCapacity, MaxAge);

        _ = await Assert.That(cache.TryVerify(certificate, clock.GetUtcNow(), out _)).IsTrue();
        clock.Advance(TimeSpan.FromHours(2));

        _ = await Assert.That(cache.TryVerify(certificate, clock.GetUtcNow(), out _)).IsFalse();
        _ = await Assert.That(cache.ChainBuilds).IsEqualTo(2L);
        _ = await Assert.That(cache.Count).IsEqualTo(0);
    }

    /// <summary>Verifies the refusal of an expired certificate names the time validity failure.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredCertificateRefusalNamesTime(CancellationToken cancellationToken)
    {
        var now = TimeProvider.System.GetUtcNow();
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b", now.AddHours(-2), now.AddHours(-1));
        var cache = new PeerChainCache(bundle.Ca, PeerChainCache.DefaultCapacity, MaxAge);

        _ = await Assert.That(cache.TryVerify(certificate, now, out _, out var refusal)).IsFalse();

        _ = await Assert.That(refusal.ChainStatus.HasFlag(X509ChainStatusFlags.NotTimeValid)).IsTrue();
        _ = await Assert.That(refusal.MissingNodeId).IsFalse();
    }

    /// <summary>Verifies a failed build is not remembered: an untrusted certificate is built again and a not yet valid one is accepted once valid.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailureIsNotCached(CancellationToken cancellationToken)
    {
        var now = TimeProvider.System.GetUtcNow();
        var clock = new FakeTimeProvider(now.AddHours(-3));
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var other = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var untrusted = MtlsTestCertificateFactory.CreatePeerCertificate(other.Ca, "node-b");
        using var delayed = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-c", now.AddHours(-1), now.AddHours(1));
        var cache = new PeerChainCache(bundle.Ca, PeerChainCache.DefaultCapacity, MaxAge);

        _ = await Assert.That(cache.TryVerify(untrusted, clock.GetUtcNow(), out _)).IsFalse();
        _ = await Assert.That(cache.TryVerify(untrusted, clock.GetUtcNow(), out _)).IsFalse();
        _ = await Assert.That(cache.TryVerify(delayed, clock.GetUtcNow(), out _)).IsFalse();
        clock.Advance(TimeSpan.FromHours(3));

        _ = await Assert.That(cache.TryVerify(delayed, clock.GetUtcNow(), out _)).IsTrue();
        _ = await Assert.That(cache.ChainBuilds).IsEqualTo(4L);
    }

    /// <summary>Verifies a peer certificate cannot make the node download a missing intermediate from the address in its authority information access extension.</summary>
    /// <exception cref="InvalidOperationException">The issuer endpoint is not an IP endpoint.</exception>
    [Test]
    public async Task MissingIntermediateIsNotDownloaded()
    {
        var now = TimeProvider.System.GetUtcNow();
        using var root = CreateAuthority("CN=Squirix Test Root", null, now);
        using var intermediate = CreateAuthority("CN=Squirix Test Intermediate", root, now);
        using var issuerEndpoint = new TcpListener(IPAddress.Loopback, 0);
        issuerEndpoint.Start();
        if (issuerEndpoint.LocalEndpoint is not IPEndPoint endpoint)
            throw new InvalidOperationException("The issuer endpoint is not an IP endpoint.");

        var issuerPort = endpoint.Port;
        using var leaf = CreateLeaf(intermediate, new UriBuilder(Uri.UriSchemeHttp, IPAddress.Loopback.ToString(), issuerPort, "intermediate.cer").Uri.AbsoluteUri, now);
        var cache = new PeerChainCache(root, PeerChainCache.DefaultCapacity, MaxAge);

        var trusted = cache.TryVerify(leaf, now, out _);

        _ = await Assert.That(trusted).IsFalse();
        _ = await Assert.That(issuerEndpoint.Pending()).IsFalse();
        _ = await Assert.That(CanBuildWithSuppliedIntermediate(root, intermediate, leaf, now)).IsTrue();
    }

    /// <summary>Verifies the refusal of a certificate with one role names the usage failure.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OneRoleCertificateRefusalNamesUsage(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificateWithPurposes(bundle.Ca, "node-b", [MtlsCertificateIdentity.ServerAuthOid]);
        var cache = new PeerChainCache(bundle.Ca, PeerChainCache.DefaultCapacity, MaxAge);

        _ = await Assert.That(cache.TryVerify(certificate, TimeProvider.System.GetUtcNow(), out _, out var refusal)).IsFalse();

        _ = await Assert.That(refusal.ChainStatus.HasFlag(X509ChainStatusFlags.NotValidForUsage)).IsTrue();
    }

    /// <summary>Verifies the refusal of a certificate of another authority reports a failed chain.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OtherAuthorityRefusalReportsChainFailure(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var other = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(other.Ca, "node-b");
        var cache = new PeerChainCache(bundle.Ca, PeerChainCache.DefaultCapacity, MaxAge);

        _ = await Assert.That(cache.TryVerify(certificate, TimeProvider.System.GetUtcNow(), out _, out var refusal)).IsFalse();

        _ = await Assert.That(refusal.ChainStatus).IsNotEqualTo(X509ChainStatusFlags.NoError);
        _ = await Assert.That(refusal.MissingNodeId).IsFalse();
    }

    /// <summary>Verifies a repeated certificate, also as a separate instance with the same bytes, builds one chain and reports its NodeId.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RepeatedCertificateBuildsOneChain(CancellationToken cancellationToken)
    {
        var now = TimeProvider.System.GetUtcNow();
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");
        using var copy = X509CertificateLoader.LoadCertificate(certificate.RawData);
        var cache = new PeerChainCache(bundle.Ca, PeerChainCache.DefaultCapacity, MaxAge);

        _ = await Assert.That(cache.TryVerify(certificate, now, out var first)).IsTrue();
        _ = await Assert.That(cache.TryVerify(certificate, now, out _)).IsTrue();
        _ = await Assert.That(cache.TryVerify(copy, now, out var second)).IsTrue();

        _ = await Assert.That(first.NodeId).IsEqualTo("node-b");
        _ = await Assert.That(second).IsEqualTo(first);
        _ = await Assert.That(cache.ChainBuilds).IsEqualTo(1L);
        _ = await Assert.That(cache.Count).IsEqualTo(1);
    }

    private static bool CanBuildWithSuppliedIntermediate(X509Certificate2 root, X509Certificate2 intermediate, X509Certificate2 leaf, DateTimeOffset now)
    {
        using var chain = new X509Chain();
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.DisableCertificateDownloads = true;
        chain.ChainPolicy.VerificationTime = now.UtcDateTime;
        _ = chain.ChainPolicy.CustomTrustStore.Add(root);
        _ = chain.ChainPolicy.ExtraStore.Add(intermediate);
        return chain.Build(leaf);
    }

    private static X509Certificate2 CreateAuthority(string subject, X509Certificate2? issuer, DateTimeOffset now)
    {
        using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest(subject, key, HashAlgorithmName.SHA256);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        var notBefore = now.AddDays(-BackdateDays);
        var notAfter = notBefore.AddDays(ValidityDays);
        if (issuer == null)
            return request.CreateSelfSigned(notBefore, notAfter);

        var serial = Guid.NewGuid().ToByteArray();
        using var publicOnly = request.Create(issuer, notBefore, notAfter, serial);
        return publicOnly.CopyWithPrivateKey(key);
    }

    private static X509Certificate2 CreateLeaf(X509Certificate2 issuer, string issuerUrl, DateTimeOffset now)
    {
        using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest("CN=node-b", key, HashAlgorithmName.SHA256);
        request.AddClusterNodeExtensions();
        request.CertificateExtensions.Add(new X509AuthorityInformationAccessExtension(null, [issuerUrl]));
        var notBefore = now.AddDays(-BackdateDays);
        using var publicOnly = request.Create(issuer, notBefore, notBefore.AddDays(ValidityDays), Guid.NewGuid().ToByteArray());
        return publicOnly.CopyWithPrivateKey(key);
    }
}
