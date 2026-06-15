using System;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Squirix.Server.Cluster.Transport;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>
/// Unit tests for inbound cluster mTLS client certificate validation.
/// </summary>
public sealed class MtlsClientCertificateValidatorTests
{
    /// <summary>
    /// Ensures client certificates signed by the cluster CA are accepted.
    /// </summary>
    [Fact]
    public void ValidateAcceptsCertificateSignedByClusterCa()
    {
        using var bundle = MtlsTestCertificateFactory.Create();
        using var peerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");

        Assert.True(MtlsClientCertificateValidator.Validate(peerCertificate, bundle.Ca));
    }

    /// <summary>
    /// Ensures inbound validation accepts configured remote peer identities only.
    /// </summary>
    [Fact]
    public void ValidateForConfiguredRemotePeerAcceptsOnlyConfiguredNodeIds()
    {
        using var bundle = MtlsTestCertificateFactory.Create();
        using var peerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");

        Assert.True(MtlsClientCertificateValidator.ValidateForConfiguredRemotePeer(peerCertificate, bundle.Ca, ["node-b", "node-c"]));
        Assert.False(MtlsClientCertificateValidator.ValidateForConfiguredRemotePeer(peerCertificate, bundle.Ca, ["node-c"]));
    }

    /// <summary>
    /// Ensures expected node identity is enforced for peer certificates.
    /// </summary>
    [Fact]
    public void ValidateForExpectedNodeIdRejectsMismatchedIdentity()
    {
        using var bundle = MtlsTestCertificateFactory.Create();
        using var peerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");

        Assert.True(MtlsClientCertificateValidator.ValidateForExpectedNodeId(peerCertificate, bundle.Ca, "node-b"));
        Assert.False(MtlsClientCertificateValidator.ValidateForExpectedNodeId(peerCertificate, bundle.Ca, "node-c"));
    }

    /// <summary>
    /// Ensures certificates signed by an untrusted CA are rejected.
    /// </summary>
    [Fact]
    public void ValidateRejectsCertificateSignedByUntrustedCa()
    {
        using var bundle = MtlsTestCertificateFactory.Create();
        using var otherCa = CreateStandaloneCa("CN=Other CA");
        using var peerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(otherCa, "node-b");

        Assert.False(MtlsClientCertificateValidator.Validate(peerCertificate, bundle.Ca));
    }

    /// <summary>
    /// Ensures expired client certificates are rejected.
    /// </summary>
    [Fact]
    public void ValidateRejectsExpiredCertificate()
    {
        using var bundle = MtlsTestCertificateFactory.Create();
        var notBefore = new DateTimeOffset(bundle.Ca.NotBefore.ToUniversalTime());
        var notAfter = notBefore.AddHours(1);
        using var expiredCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b", notBefore, notAfter);

        Assert.False(MtlsClientCertificateValidator.Validate(expiredCertificate, bundle.Ca));
    }

    /// <summary>
    /// Ensures missing client certificates are rejected.
    /// </summary>
    [Fact]
    public void ValidateRejectsMissingClientCertificate()
    {
        using var bundle = MtlsTestCertificateFactory.Create();

        Assert.False(MtlsClientCertificateValidator.Validate(null, bundle.Ca));
    }

    private static X509Certificate2 CreateStandaloneCa(string distinguishedName)
    {
        using var caKey = RSA.Create(2048);
        var caRequest = new CertificateRequest(distinguishedName, caKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        caRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        var notBefore = DateTimeOffset.UtcNow.AddDays(-1);
        var notAfter = notBefore.AddDays(30);
        return caRequest.CreateSelfSigned(notBefore, notAfter);
    }
}
