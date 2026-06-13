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
        FileKit.WriteAllText(GetClusterCertificateAuthorityPath(), _ca.ExportCertificatePem());
    }

    /// <summary>
    /// Creates validated cluster mTLS options and loaded material for a test node.
    /// </summary>
    /// <param name="nodeId">Local node identifier.</param>
    /// <param name="internalListenPort">Dedicated internal HTTPS listener port.</param>
    /// <returns>Options and material suitable for host startup overrides.</returns>
    public (MtlsOptions Options, MtlsCertificateMaterial Material) CreateNode(string nodeId, int internalListenPort)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);

        var nodeDirectory = PathKit.Combine(_rootDirectory, nodeId);
        DirectoryKit.CreateDirectory(nodeDirectory);

        using var nodeCertificate = CreateNodeCertificate(nodeId);
        return CreateNodeFromCertificate(nodeId, internalListenPort, nodeDirectory, nodeCertificate);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _ca.Dispose();
        DirectoryKit.TryDeleteDirectory(_rootDirectory);
    }

    internal (MtlsOptions Options, MtlsCertificateMaterial Material) CreateNodeFromCertificate(string nodeId, int internalListenPort, X509Certificate2 nodeCertificate)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        ArgumentNullException.ThrowIfNull(nodeCertificate);

        var nodeDirectory = PathKit.Combine(_rootDirectory, nodeId);
        DirectoryKit.CreateDirectory(nodeDirectory);
        return CreateNodeFromCertificate(nodeId, internalListenPort, nodeDirectory, nodeCertificate);
    }

    internal X509Certificate2 GetClusterCertificateAuthority() => _ca;

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

    private (MtlsOptions Options, MtlsCertificateMaterial Material) CreateNodeFromCertificate(
        string nodeId,
        int internalListenPort,
        string nodeDirectory,
        X509Certificate2 nodeCertificate)
    {
        _ = nodeId;
        var exportableCertificate = MtlsTestCertificates.LoadExportableCertificate(nodeCertificate);
        var pfxPath = PathKit.Combine(nodeDirectory, "node.pfx");
        File.WriteAllBytes(pfxPath, exportableCertificate.Export(X509ContentType.Pfx));

        var options = new MtlsOptions
        {
            CaPath = GetClusterCertificateAuthorityPath(),
            CertPfxPath = pfxPath,
            InternalListenPort = internalListenPort,
        };

        var material = MtlsCertificateMaterial.FromCertificates(exportableCertificate, _ca);
        return (options, material);
    }

    private string GetClusterCertificateAuthorityPath() => PathKit.Combine(_rootDirectory, "cluster-ca.crt");
}
