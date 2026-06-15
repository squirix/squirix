using System;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using Squirix.Server.TestKit.IO;

namespace Squirix.Server.UnitTests.Cluster.Transport;

internal sealed class MtlsTestCertificateBundle : IDisposable
{
    private readonly X509Certificate2 _nodeCertificate;
    private readonly TempDirectory _rootDirectory;

    internal MtlsTestCertificateBundle(X509Certificate2 ca, X509Certificate2 nodeCertificate)
    {
        _rootDirectory = new TempDirectory("squirix-cluster-mtls-tests");
        RootDirectory = _rootDirectory.Path;
        Ca = ca;
        _nodeCertificate = nodeCertificate;
        CaPath = PathKit.Combine(_rootDirectory, "cluster-ca.crt");
        CertPath = PathKit.Combine(_rootDirectory, "node.crt");
        KeyPath = PathKit.Combine(_rootDirectory, "node.key");
        PfxPath = PathKit.Combine(_rootDirectory, "node.pfx");

        FileKit.WriteAllText(CaPath, ca.ExportCertificatePem());
        FileKit.WriteAllText(CertPath, nodeCertificate.ExportCertificatePem());
        FileKit.WriteAllText(KeyPath, nodeCertificate.GetRSAPrivateKey()!.ExportRSAPrivateKeyPem());
        File.WriteAllBytes(PfxPath, nodeCertificate.Export(X509ContentType.Pfx));
    }

    public X509Certificate2 Ca { get; }

    public string CaPath { get; }

    public string CertPath { get; }

    public string KeyPath { get; }

    public string PfxPath { get; }

    public string RootDirectory { get; }

    public void Dispose()
    {
        _nodeCertificate.Dispose();
        Ca.Dispose();
        _rootDirectory.Dispose();
    }
}
