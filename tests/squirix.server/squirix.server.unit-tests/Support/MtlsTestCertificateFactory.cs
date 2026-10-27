using System;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit.Mtls;

namespace Squirix.Server.UnitTests.Support;

internal static class MtlsTestCertificateFactory
{
    internal static Task<MtlsTestCertificateBundle> CreateAsync(CancellationToken cancellationToken = default) => CreateAsync(false, cancellationToken);

    internal static async Task<MtlsTestCertificateBundle> CreateAsync(bool rsaNodeKey, CancellationToken cancellationToken)
    {
        using var caKey = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var caRequest = new CertificateRequest("CN=Squirix Cluster Test CA", caKey, HashAlgorithmName.SHA256);
        caRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        var notBefore = DateTimeOffset.UtcNow.AddDays(-1);
        var notAfter = notBefore.AddDays(30);
        var ca = caRequest.CreateSelfSigned(notBefore, notAfter);

        var nodeCertificate = CreatePeerCertificate(ca, "node-a", rsaKey: rsaNodeKey);
        try
        {
            var bundle = await MtlsTestCertificateBundle.CreateAsync(ca, nodeCertificate, cancellationToken);
            nodeCertificate = null;
            return bundle;
        }
        finally
        {
            nodeCertificate?.Dispose();
        }
    }

    internal static X509Certificate2 CreateAuthorityWithPurposes(string subject, string[] purposes)
    {
        using var caKey = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var caRequest = new CertificateRequest(subject, caKey, HashAlgorithmName.SHA256);
        caRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        var oids = new OidCollection();
        foreach (var purpose in purposes)
            _ = oids.Add(new Oid(purpose));

        caRequest.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(oids, false));
        var notBefore = DateTimeOffset.UtcNow.AddDays(-1);
        return caRequest.CreateSelfSigned(notBefore, notBefore.AddDays(30));
    }

    internal static X509Certificate2 CreatePeerCertificate(X509Certificate2 ca, string commonName, DateTimeOffset? notBefore = null, DateTimeOffset? notAfter = null, bool rsaKey = false)
    {
        ArgumentNullException.ThrowIfNull(ca);
        ArgumentException.ThrowIfNullOrWhiteSpace(commonName);

        var effectiveNotBefore = notBefore ?? new DateTimeOffset(ca.NotBefore.ToUniversalTime());
        var effectiveNotAfter = notAfter ?? new DateTimeOffset(ca.NotAfter.ToUniversalTime());

        return TestCertificates.CreatePeerCertificate(ca, commonName, effectiveNotBefore, effectiveNotAfter, rsaKey);
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

        using var peerKey = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var peerRequest = new CertificateRequest($"CN={commonName}", peerKey, HashAlgorithmName.SHA256);
        peerRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature, true));
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
