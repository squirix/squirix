using System;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Squirix.Server.TestKit.IO;

namespace Squirix.Server.UnitTests.Cluster.Transport;

internal static class ClusterMtlsTestCertificateFactory
{
    public static ClusterMtlsTestCertificateBundle Create(string? rootDirectory = null)
    {
        var directory = rootDirectory ?? DirectoryKit.CreateTempDirectory("squirix-cluster-mtls-tests");
        if (rootDirectory is not null)
            DirectoryKit.CreateDirectory(directory);

        using var caKey = RSA.Create(2048);
        var caRequest = new CertificateRequest("CN=Squirix Cluster Test CA", caKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        caRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        var ca = caRequest.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));

        using var nodeKey = RSA.Create(2048);
        var nodeRequest = new CertificateRequest("CN=squirix-node-a", nodeKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var nodePublic = nodeRequest.Create(ca, DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30), Guid.NewGuid().ToByteArray());
        var nodeCertificate = nodePublic.HasPrivateKey ? nodePublic : nodePublic.CopyWithPrivateKey(nodeKey);

        return new ClusterMtlsTestCertificateBundle(directory, ca, nodeCertificate);
    }
}
