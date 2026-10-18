using System;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit.Mtls;

namespace Squirix.Server.UnitTests.Support;

internal static class MtlsTestCertificateFactory
{
    internal static async Task<MtlsTestCertificateBundle> CreateAsync(CancellationToken cancellationToken = default)
    {
        using var caKey = RSA.Create(2048);
        var caRequest = new CertificateRequest("CN=Squirix Cluster Test CA", caKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        caRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        var notBefore = DateTimeOffset.UtcNow.AddDays(-1);
        var notAfter = notBefore.AddDays(30);
        var ca = caRequest.CreateSelfSigned(notBefore, notAfter);

        using var nodeKey = RSA.Create(2048);
        var nodeRequest = new CertificateRequest("CN=node-a", nodeKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        nodeRequest.AddClusterNodeExtensions();
        var nodePublic = nodeRequest.Create(
            ca,
            new DateTimeOffset(ca.NotBefore.ToUniversalTime()),
            new DateTimeOffset(ca.NotAfter.ToUniversalTime()),
            Guid.NewGuid().ToByteArray());
        var nodeCertificate = nodePublic.HasPrivateKey ? nodePublic : nodePublic.CopyWithPrivateKey(nodeKey);

        return await MtlsTestCertificateBundle.CreateAsync(ca, nodeCertificate, cancellationToken);
    }

    internal static X509Certificate2 CreatePeerCertificate(X509Certificate2 ca, string commonName, DateTimeOffset? notBefore = null, DateTimeOffset? notAfter = null)
    {
        ArgumentNullException.ThrowIfNull(ca);
        ArgumentException.ThrowIfNullOrWhiteSpace(commonName);

        var effectiveNotBefore = notBefore ?? new DateTimeOffset(ca.NotBefore.ToUniversalTime());
        var effectiveNotAfter = notAfter ?? new DateTimeOffset(ca.NotAfter.ToUniversalTime());

        using var peerKey = RSA.Create(2048);
        var peerRequest = new CertificateRequest($"CN={commonName}", peerKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        peerRequest.AddClusterNodeExtensions();
        var peerPublic = peerRequest.Create(ca, effectiveNotBefore, effectiveNotAfter, Guid.NewGuid().ToByteArray());
        return peerPublic.CopyWithPrivateKey(peerKey);
    }

    /// <summary>Creates a peer certificate whose extended key usage lists exactly the given purposes, or has no such extension when <paramref name="purposes" /> is <see langword="null" />.</summary>
    /// <param name="ca">The issuing test certificate authority.</param>
    /// <param name="commonName">The certificate common name.</param>
    /// <param name="purposes">The extended key usage OIDs, or <see langword="null" /> for no extended key usage extension.</param>
    /// <returns>A peer certificate with a private key.</returns>
    internal static X509Certificate2 CreatePeerCertificateWithPurposes(X509Certificate2 ca, string commonName, string[]? purposes)
    {
        ArgumentNullException.ThrowIfNull(ca);
        ArgumentException.ThrowIfNullOrWhiteSpace(commonName);

        using var peerKey = RSA.Create(2048);
        var peerRequest = new CertificateRequest($"CN={commonName}", peerKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        peerRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
        if (purposes != null)
        {
            var oids = new OidCollection();
            foreach (var purpose in purposes)
                _ = oids.Add(new Oid(purpose));

            peerRequest.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(oids, false));
        }

        var peerPublic = peerRequest.Create(
            ca,
            new DateTimeOffset(ca.NotBefore.ToUniversalTime()),
            new DateTimeOffset(ca.NotAfter.ToUniversalTime()),
            Guid.NewGuid().ToByteArray());
        return peerPublic.CopyWithPrivateKey(peerKey);
    }
}
