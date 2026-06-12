using System;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using Squirix.Server.TestKit.IO;

namespace Squirix.Server.UnitTests.Cluster.Transport;

internal sealed class MtlsTestCertificateBundle : IDisposable
{
    private readonly X509Certificate2 _nodeCertificate;

    internal MtlsTestCertificateBundle(string rootDirectory, X509Certificate2 ca, X509Certificate2 nodeCertificate)
    {
        RootDirectory = rootDirectory;
        Ca = ca;
        _nodeCertificate = nodeCertificate;
        CaPath = PathKit.Combine(rootDirectory, "cluster-ca.crt");
        CertPath = PathKit.Combine(rootDirectory, "node.crt");
        KeyPath = PathKit.Combine(rootDirectory, "node.key");
        PfxPath = PathKit.Combine(rootDirectory, "node.pfx");

        FileKit.WriteAllText(CaPath, ca.ExportCertificatePem());
        FileKit.WriteAllText(CertPath, nodeCertificate.ExportCertificatePem());
        FileKit.WriteAllText(KeyPath, nodeCertificate.GetRSAPrivateKey()!.ExportRSAPrivateKeyPem());
        File.WriteAllBytes(PfxPath, nodeCertificate.Export(X509ContentType.Pfx));
    }

    public string RootDirectory { get; }

    public string CaPath { get; }

    public string CertPath { get; }

    public string KeyPath { get; }

    public string PfxPath { get; }

    public X509Certificate2 Ca { get; }

    public void Dispose()
    {
        _nodeCertificate.Dispose();
        Ca.Dispose();
        DirectoryKit.TryDeleteDirectory(RootDirectory);
    }
}
