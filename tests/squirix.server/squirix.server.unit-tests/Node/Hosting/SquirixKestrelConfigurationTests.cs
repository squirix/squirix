using System;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Squirix.Server.Node.Hosting;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Hosting;

/// <summary>
/// Unit tests for Kestrel mTLS client certificate validation.
/// </summary>
public sealed class SquirixKestrelConfigurationTests
{
    /// <summary>
    /// Ensures untrusted self-signed client certificates are rejected.
    /// </summary>
    [Fact]
    public void ValidateMutualTlsClientCertificateRejectsUntrustedSelfSignedCertificate()
    {
        using var certificates = TestCertificates.Create();
        using var chain = CreateChain(certificates.Ca, false);

        Assert.False(SquirixKestrelConfiguration.ValidateMutualTlsClientCertificate(certificates.UntrustedClient, chain));
    }

    /// <summary>
    /// Ensures client certificates signed by a trusted CA are accepted.
    /// </summary>
    [Fact]
    public void ValidateMutualTlsClientCertificateAcceptsCertificateSignedByTrustedCa()
    {
        using var certificates = TestCertificates.Create();
        using var chain = CreateChain(certificates.Ca, true);

        Assert.True(SquirixKestrelConfiguration.ValidateMutualTlsClientCertificate(certificates.TrustedClient, chain));
    }

    /// <summary>
    /// Ensures validation fails when Kestrel does not provide a certificate chain.
    /// </summary>
    [Fact]
    public void ValidateMutualTlsClientCertificateRejectsWhenChainIsNull()
    {
        using var certificates = TestCertificates.Create();

        Assert.False(SquirixKestrelConfiguration.ValidateMutualTlsClientCertificate(certificates.TrustedClient, null));
    }

    private static X509Chain CreateChain(X509Certificate2 ca, bool trustCa)
    {
        var chain = new X509Chain();
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        if (!trustCa)
            return chain;
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        _ = chain.ChainPolicy.CustomTrustStore.Add(ca);
        return chain;
    }

    private sealed class TestCertificates : IDisposable
    {
        private TestCertificates(X509Certificate2 ca, X509Certificate2 trustedClient, X509Certificate2 untrustedClient)
        {
            Ca = ca;
            TrustedClient = trustedClient;
            UntrustedClient = untrustedClient;
        }

        public X509Certificate2 Ca { get; }

        public X509Certificate2 TrustedClient { get; }

        public X509Certificate2 UntrustedClient { get; }

        public static TestCertificates Create()
        {
            using var caKey = RSA.Create(2048);
            var caRequest = new CertificateRequest("CN=Squirix Test CA", caKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            caRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
            var ca = caRequest.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));

            using var trustedKey = RSA.Create(2048);
            var trustedRequest = new CertificateRequest("CN=trusted-node", trustedKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            var trustedPublic = trustedRequest.Create(ca, DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30), Guid.NewGuid().ToByteArray());
            var trustedClient = trustedPublic.CopyWithPrivateKey(trustedKey);

            using var untrustedKey = RSA.Create(2048);
            var untrustedRequest = new CertificateRequest("CN=untrusted-node", untrustedKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            var untrustedClient = untrustedRequest.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));

            return new TestCertificates(ca, trustedClient, untrustedClient);
        }

        public void Dispose()
        {
            Ca.Dispose();
            TrustedClient.Dispose();
            UntrustedClient.Dispose();
        }
    }
}
