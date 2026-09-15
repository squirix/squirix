using System;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>Unit tests for cluster mTLS certificate loading.</summary>
[Immutable]
public sealed class MtlsCertificateLoaderTests : ServerUnitTestBase
{
    /// <summary>Ensures untrusted node certificates are rejected.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LoadRejectsUntrustedNodeCertificate(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
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
        _ = await Assert.That(ex.Message).Contains("does not chain", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Ensures PEM loading works for trusted test certificates.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LoadsPemNodeCertificateAndTrustAnchor(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        var options = new MtlsOptions
        {
            CaPath = bundle.CaPath,
            CertPath = bundle.CertPath,
            KeyPath = bundle.KeyPath,
            InternalListenPort = 6102,
        };

        using var material = MtlsCertificateMaterial.Load(options, 6001, true, "node-a");

        _ = await Assert.That(material.Enabled).IsTrue();
        _ = await Assert.That(material.NodeCertificate!.HasPrivateKey).IsTrue();
    }

    /// <summary>Ensures PFX loading works for trusted test certificates.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LoadsPfxNodeCertificateAndTrustAnchor(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        var options = new MtlsOptions
        {
            CaPath = bundle.CaPath,
            CertPfxPath = bundle.PfxPath,
            InternalListenPort = 6101,
        };

        using var material = MtlsCertificateMaterial.Load(options, 6001, true, "node-a");

        _ = await Assert.That(material.Enabled).IsTrue();
        _ = await Assert.That(material.NodeCertificate).IsNotNull();
        _ = await Assert.That(material.TrustAnchor).IsNotNull();
        _ = await Assert.That(material.NodeCertificate.HasPrivateKey).IsTrue();
    }

    /// <summary>Ensures standalone topology returns an empty material instance.</summary>
    [Test]
    public async Task ReturnsDisabledWhenMaterialMissing()
    {
        var material = MtlsCertificateMaterial.Load(new MtlsOptions(), 6001, false);

        _ = await Assert.That(material.Enabled).IsFalse();
    }
}
