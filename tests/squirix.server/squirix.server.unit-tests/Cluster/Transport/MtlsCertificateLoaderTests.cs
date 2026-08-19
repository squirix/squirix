using System;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>Unit tests for cluster mTLS certificate loading.</summary>
[Immutable]
public sealed class MtlsCertificateLoaderTests : ServerUnitTestBase
{
    /// <summary>Ensures PEM loading works for trusted test certificates.</summary>
    [Fact]
    public async Task LoadLoadsPemBackedNodeCertificateAndTrustAnchor()
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(DefaultCancellationToken);
        var options = new MtlsOptions
        {
            CaPath = bundle.CaPath,
            CertPath = bundle.CertPath,
            KeyPath = bundle.KeyPath,
            InternalListenPort = 6102,
        };

        using var material = MtlsCertificateMaterial.Load(options, 6001, true, "node-a");

        Assert.True(material.Enabled);
        Assert.True(material.NodeCertificate!.HasPrivateKey);
    }

    /// <summary>Ensures PFX loading works for trusted test certificates.</summary>
    [Fact]
    public async Task LoadLoadsPfxBackedNodeCertificateAndTrustAnchor()
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(DefaultCancellationToken);
        var options = new MtlsOptions
        {
            CaPath = bundle.CaPath,
            CertPfxPath = bundle.PfxPath,
            InternalListenPort = 6101,
        };

        using var material = MtlsCertificateMaterial.Load(options, 6001, true, "node-a");

        Assert.True(material.Enabled);
        Assert.NotNull(material.NodeCertificate);
        Assert.NotNull(material.TrustAnchor);
        Assert.True(material.NodeCertificate.HasPrivateKey);
    }

    /// <summary>Ensures untrusted node certificates are rejected.</summary>
    [Fact]
    public async Task LoadRejectsUntrustedNodeCertificate()
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(DefaultCancellationToken);
        using var untrustedKey = RSA.Create(2048);
        var untrustedRequest = new CertificateRequest("CN=untrusted-node", untrustedKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var untrustedCertificate = untrustedRequest.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
        var untrustedCertPath = NodePathKit.Combine(bundle.RootDirectory, "untrusted.crt");
        var untrustedKeyPath = NodePathKit.Combine(bundle.RootDirectory, "untrusted.key");
        FileKit.WriteAllText(untrustedCertPath, untrustedCertificate.ExportCertificatePem());
        FileKit.WriteAllText(untrustedKeyPath, untrustedKey.ExportRSAPrivateKeyPem());

        var options = new MtlsOptions
        {
            CaPath = bundle.CaPath,
            CertPath = untrustedCertPath,
            KeyPath = untrustedKeyPath,
            InternalListenPort = 6103,
        };

        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static value => _ = MtlsCertificateMaterial.Load(value, 6001, true, "untrusted-node"));
        Assert.Contains("does not chain", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Ensures standalone topology returns an empty material instance.</summary>
    [Fact]
    public void LoadReturnsDisabledMaterialInterNodeMtlsIsRequired()
    {
        var material = MtlsCertificateMaterial.Load(new MtlsOptions(), 6001, false);

        Assert.False(material.Enabled);
    }
}
