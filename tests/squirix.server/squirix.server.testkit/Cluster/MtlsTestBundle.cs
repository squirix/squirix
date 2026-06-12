using System;
using System.IO;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit.IO;

namespace Squirix.Server.TestKit.Cluster;

/// <summary>
/// Shared cluster CA and per-node mTLS material for multi-node integration and smoke tests.
/// </summary>
internal sealed class MtlsTestBundle : IDisposable
{
    private readonly X509Certificate2 _ca;
    private readonly string _rootDirectory;

    /// <summary>
    /// Initializes a new instance of the <see cref="MtlsTestBundle" /> class.
    /// </summary>
    public MtlsTestBundle()
    {
        _rootDirectory = DirectoryKit.CreateTempDirectory("squirix-cluster-mtls-cluster");
        _ca = CreateCertificateAuthority();
        FileKit.WriteAllText(CaPath, _ca.ExportCertificatePem());
    }

    private string CaPath => PathKit.Combine(_rootDirectory, "cluster-ca.crt");

    /// <summary>
    /// Creates validated cluster mTLS options and loaded material for a test node.
    /// </summary>
    /// <param name="nodeId">Local node identifier.</param>
    /// <param name="primaryListenPort">Primary external HTTPS listener port.</param>
    /// <param name="internalListenPort">Dedicated internal HTTPS listener port.</param>
    /// <returns>Options and material suitable for host startup overrides.</returns>
    public (MtlsOptions Options, MtlsCertificateMaterial Material) CreateNode(string nodeId, int primaryListenPort, int internalListenPort)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);

        var nodeDirectory = PathKit.Combine(_rootDirectory, nodeId);
        DirectoryKit.CreateDirectory(nodeDirectory);

        using var nodeCertificate = CreateNodeCertificate(nodeId);
        var pfxPath = PathKit.Combine(nodeDirectory, "node.pfx");
        File.WriteAllBytes(pfxPath, nodeCertificate.Export(X509ContentType.Pfx));

        var options = new MtlsOptions
        {
            CaPath = CaPath,
            CertPfxPath = pfxPath,
            InternalListenPort = internalListenPort,
        };

        var material = MtlsCertificateMaterial.Load(options, primaryListenPort, true);
        return (options, material);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _ca.Dispose();
        DirectoryKit.TryDeleteDirectory(_rootDirectory);
    }

    private static X509Certificate2 CreateCertificateAuthority()
    {
        using var caKey = RSA.Create(2048);
        var caRequest = new CertificateRequest("CN=Squirix Cluster Test CA", caKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        caRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        var notBefore = DateTimeOffset.UtcNow.AddDays(-1);
        var notAfter = notBefore.AddDays(30);
        return caRequest.CreateSelfSigned(notBefore, notAfter);
    }

    private X509Certificate2 CreateNodeCertificate(string nodeId)
    {
        using var nodeKey = RSA.Create(2048);
        var nodeRequest = new CertificateRequest($"CN={nodeId}", nodeKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        nodeRequest.AddClusterNodeExtensions();
        var nodePublic = nodeRequest.Create(_ca, _ca.NotBefore, _ca.NotAfter, Guid.NewGuid().ToByteArray());
        return nodePublic.HasPrivateKey ? nodePublic : nodePublic.CopyWithPrivateKey(nodeKey);
    }
}
