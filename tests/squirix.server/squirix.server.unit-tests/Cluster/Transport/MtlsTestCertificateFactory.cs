using System;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Squirix.Server.TestKit.IO;

namespace Squirix.Server.UnitTests.Cluster.Transport;

internal static class MtlsTestCertificateFactory
{
    public static MtlsTestCertificateBundle Create(string? rootDirectory = null)
    {
        var directory = rootDirectory ?? DirectoryKit.CreateTempDirectory("squirix-cluster-mtls-tests");
        if (rootDirectory is not null)
            DirectoryKit.CreateDirectory(directory);

        using var caKey = RSA.Create(2048);
        var caRequest = new CertificateRequest("CN=Squirix Cluster Test CA", caKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        caRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        var notBefore = DateTimeOffset.UtcNow.AddDays(-1);
        var notAfter = notBefore.AddDays(30);
        var ca = caRequest.CreateSelfSigned(notBefore, notAfter);

        using var nodeKey = RSA.Create(2048);
        var nodeRequest = new CertificateRequest("CN=squirix-node-a", nodeKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var nodePublic = nodeRequest.Create(ca, ca.NotBefore, ca.NotAfter, Guid.NewGuid().ToByteArray());
        var nodeCertificate = nodePublic.HasPrivateKey ? nodePublic : nodePublic.CopyWithPrivateKey(nodeKey);

        return new MtlsTestCertificateBundle(directory, ca, nodeCertificate);
    }

    public static X509Certificate2 CreatePeerCertificate(
        X509Certificate2 ca,
        string commonName,
        DateTimeOffset? notBefore = null,
        DateTimeOffset? notAfter = null)
    {
        ArgumentNullException.ThrowIfNull(ca);
        ArgumentException.ThrowIfNullOrWhiteSpace(commonName);

        var effectiveNotBefore = notBefore ?? ca.NotBefore;
        var effectiveNotAfter = notAfter ?? ca.NotAfter;

        using var peerKey = RSA.Create(2048);
        var peerRequest = new CertificateRequest($"CN={commonName}", peerKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var peerPublic = peerRequest.Create(ca, effectiveNotBefore, effectiveNotAfter, Guid.NewGuid().ToByteArray());
        return peerPublic.CopyWithPrivateKey(peerKey);
    }
}
