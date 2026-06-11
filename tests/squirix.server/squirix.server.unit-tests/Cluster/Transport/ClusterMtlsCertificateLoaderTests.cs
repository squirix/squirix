using System;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>
/// Unit tests for cluster mTLS certificate loading.
/// </summary>
public sealed class ClusterMtlsCertificateLoaderTests
{
    /// <summary>
    /// Ensures disabled options return an empty material instance.
    /// </summary>
    [Fact]
    public void LoadReturnsDisabledMaterialWhenClusterMtlsIsDisabled()
    {
        var material = ClusterMtlsCertificateMaterial.Load(new ClusterMtlsOptions { Enabled = false });

        Assert.Same(ClusterMtlsCertificateMaterial.Disabled, material);
        Assert.False(material.Enabled);
    }

    /// <summary>
    /// Ensures PFX loading works for trusted test certificates.
    /// </summary>
    [Fact]
    public void LoadLoadsPfxBackedNodeCertificateAndTrustAnchor()
    {
        using var bundle = ClusterMtlsTestCertificateFactory.Create();
        var options = new ClusterMtlsOptions
        {
            Enabled = true,
            CaPath = bundle.CaPath,
            CertPfxPath = bundle.PfxPath,
            InternalListenPort = 6101,
        };

        using var material = ClusterMtlsCertificateMaterial.Load(options);

        Assert.True(material.Enabled);
        Assert.NotNull(material.NodeCertificate);
        Assert.NotNull(material.TrustAnchor);
        Assert.True(material.NodeCertificate.HasPrivateKey);
    }

    /// <summary>
    /// Ensures PEM loading works for trusted test certificates.
    /// </summary>
    [Fact]
    public void LoadLoadsPemBackedNodeCertificateAndTrustAnchor()
    {
        using var bundle = ClusterMtlsTestCertificateFactory.Create();
        var options = new ClusterMtlsOptions
        {
            Enabled = true,
            CaPath = bundle.CaPath,
            CertPath = bundle.CertPath,
            KeyPath = bundle.KeyPath,
            InternalListenPort = 6102,
        };

        using var material = ClusterMtlsCertificateMaterial.Load(options);

        Assert.True(material.Enabled);
        Assert.True(material.NodeCertificate!.HasPrivateKey);
    }

    /// <summary>
    /// Ensures certificates without a private key are rejected.
    /// </summary>
    [Fact]
    public void LoadRejectsCertificateWithoutPrivateKey()
    {
        using var bundle = ClusterMtlsTestCertificateFactory.Create();
        using var certOnly = X509CertificateLoader.LoadCertificateFromFile(bundle.CaPath);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            ClusterMtlsCertificateLoader.EnsureNodeCertificateChainsToTrustAnchor(certOnly, bundle.Ca));

        Assert.Contains("private key", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Ensures untrusted node certificates are rejected.
    /// </summary>
    [Fact]
    public void LoadRejectsUntrustedNodeCertificate()
    {
        using var bundle = ClusterMtlsTestCertificateFactory.Create();
        using var untrustedKey = RSA.Create(2048);
        var untrustedRequest = new CertificateRequest("CN=untrusted-node", untrustedKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var untrustedCertificate = untrustedRequest.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
        var untrustedCertPath = PathKit.Combine(bundle.RootDirectory, "untrusted.crt");
        var untrustedKeyPath = PathKit.Combine(bundle.RootDirectory, "untrusted.key");
        FileKit.WriteAllText(untrustedCertPath, untrustedCertificate.ExportCertificatePem());
        FileKit.WriteAllText(untrustedKeyPath, untrustedKey.ExportRSAPrivateKeyPem());

        var options = new ClusterMtlsOptions
        {
            Enabled = true,
            CaPath = bundle.CaPath,
            CertPath = untrustedCertPath,
            KeyPath = untrustedKeyPath,
            InternalListenPort = 6103,
        };

        var ex = Assert.Throws<InvalidOperationException>(() => ClusterMtlsCertificateMaterial.Load(options));
        Assert.Contains("does not chain", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
