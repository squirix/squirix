using System;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit.IO;

namespace Squirix.Server.UnitTests.Support;

internal sealed class MtlsTestCertificateBundle : IDisposable
{
    private readonly X509Certificate2 _nodeCertificate;
    private readonly TempDirectory _rootDirectory;

    private MtlsTestCertificateBundle(
        TempDirectory rootDirectory,
        X509Certificate2 ca,
        X509Certificate2 nodeCertificate,
        string caPath,
        string certPath,
        string keyPath,
        string pfxPath)
    {
        _rootDirectory = rootDirectory;
        RootDirectory = rootDirectory.Path;
        Ca = ca;
        _nodeCertificate = nodeCertificate;
        CaPath = caPath;
        CertPath = certPath;
        KeyPath = keyPath;
        PfxPath = pfxPath;
    }

    internal string CaPath { get; }

    internal string CertPath { get; }

    internal string KeyPath { get; }

    internal string PfxPath { get; }

    internal string RootDirectory { get; }

    internal X509Certificate2 Ca { get; }

    public void Dispose()
    {
        _nodeCertificate.Dispose();
        Ca.Dispose();
        _rootDirectory.Dispose();
    }

    internal static async Task<MtlsTestCertificateBundle> CreateAsync(X509Certificate2 ca, X509Certificate2 nodeCertificate, CancellationToken cancellationToken)
    {
        var rootDirectory = new TempDirectory("squirix-cluster-mtls-tests");
        var caPath = NodePathKit.Combine(rootDirectory, "cluster-ca.crt");
        var certPath = NodePathKit.Combine(rootDirectory, "node.crt");
        var keyPath = NodePathKit.Combine(rootDirectory, "node.key");
        var pfxPath = NodePathKit.Combine(rootDirectory, "node.pfx");

        FileKit.WriteAllText(caPath, ca.ExportCertificatePem());
        FileKit.WriteAllText(certPath, nodeCertificate.ExportCertificatePem());
        FileKit.WriteAllText(keyPath, nodeCertificate.GetRSAPrivateKey()!.ExportRSAPrivateKeyPem());
        await File.WriteAllBytesAsync(pfxPath, nodeCertificate.Export(X509ContentType.Pfx), cancellationToken);

        return new MtlsTestCertificateBundle(rootDirectory, ca, nodeCertificate, caPath, certPath, keyPath, pfxPath);
    }
}
