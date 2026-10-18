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
    private const int RsaKeySize = 2048;
    private const int ValidityDays = 30;
    private const int BackdateDays = 1;

    private static readonly TimeSpan MaxAge = TimeSpan.FromMinutes(10);

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
        using var key = RSA.Create(RsaKeySize);
        var request = new CertificateRequest(subject, key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
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
        using var key = RSA.Create(RsaKeySize);
        var request = new CertificateRequest("CN=node-b", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.AddClusterNodeExtensions();
        request.CertificateExtensions.Add(new X509AuthorityInformationAccessExtension(null, [issuerUrl]));
        var notBefore = now.AddDays(-BackdateDays);
        using var publicOnly = request.Create(issuer, notBefore, notBefore.AddDays(ValidityDays), Guid.NewGuid().ToByteArray());
        return publicOnly.CopyWithPrivateKey(key);
    }
}
