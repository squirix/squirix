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
    /// Ensures missing client certificates are rejected.
    /// </summary>
    [Fact]
    public void ValidateRejectsMissingClientCertificate()
    {
        using var bundle = MtlsTestCertificateFactory.Create();

        Assert.False(MtlsClientCertificateValidator.Validate(null, null, bundle.Ca));
    }

    /// <summary>
    /// Ensures client certificates signed by the cluster CA are accepted.
    /// </summary>
    [Fact]
    public void ValidateAcceptsCertificateSignedByClusterCa()
    {
        using var bundle = MtlsTestCertificateFactory.Create();
        using var peerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "peer-node-b");

        Assert.True(MtlsClientCertificateValidator.Validate(peerCertificate, null, bundle.Ca));
    }

    /// <summary>
    /// Ensures certificates signed by an untrusted CA are rejected.
    /// </summary>
    [Fact]
    public void ValidateRejectsCertificateSignedByUntrustedCa()
    {
        using var bundle = MtlsTestCertificateFactory.Create();
        using var otherCa = CreateStandaloneCa("CN=Other CA");
        using var peerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(otherCa, "peer-node-b");

        Assert.False(MtlsClientCertificateValidator.Validate(peerCertificate, null, bundle.Ca));
    }

    /// <summary>
    /// Ensures expired client certificates are rejected.
    /// </summary>
    [Fact]
    public void ValidateRejectsExpiredCertificate()
    {
        using var bundle = MtlsTestCertificateFactory.Create();
        var notBefore = bundle.Ca.NotBefore;
        var notAfter = notBefore.AddHours(1);
        using var expiredCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "expired-peer", notBefore, notAfter);

        Assert.False(MtlsClientCertificateValidator.Validate(expiredCertificate, null, bundle.Ca));
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
