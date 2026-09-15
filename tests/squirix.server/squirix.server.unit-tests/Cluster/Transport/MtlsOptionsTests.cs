using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>Unit tests for <see cref="MtlsOptions" /> validation.</summary>
[Immutable]
public sealed class MtlsOptionsTests : IsolatedStorageTestBase
{
    /// <summary>Ensures PFX and PEM inputs cannot be mixed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MixedPfxAndPemPathsRejectedAsync(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        var options = new MtlsOptions
        {
            CaPath = bundle.CaPath,
            CertPfxPath = bundle.PfxPath,
            CertPath = bundle.CertPath,
            KeyPath = bundle.KeyPath,
            InternalListenPort = 6101,
        };

        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static value => value.Validate(6001, true));
        _ = await Assert.That(ex.Message).Contains("not both", StringComparison.Ordinal);
    }

    /// <summary>Ensures multi-node topology rejects an internal port that matches the primary listener.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemotePeersExcludePrimaryListenerAsync(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        var options = new MtlsOptions
        {
            CaPath = bundle.CaPath,
            CertPfxPath = bundle.PfxPath,
            InternalListenPort = 6001,
        };

        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static value => value.Validate(6001, true));
        _ = await Assert.That(ex.Message).Contains("must differ", StringComparison.Ordinal);
    }

    /// <summary>Ensures missing files fail validation for multi-node topology.</summary>
    [Test]
    public async Task RemotePeersRejectMissingFiles()
    {
        var options = new MtlsOptions
        {
            CaPath = NodePathKit.Combine(Dir, "missing-ca.crt"),
            CertPath = NodePathKit.Combine(Dir, "missing-node.crt"),
            KeyPath = NodePathKit.Combine(Dir, "missing-node.key"),
            InternalListenPort = 6101,
        };

        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static value => value.Validate(6001, true));
        _ = await Assert.That(ex.Message).Contains("CA file was not found", StringComparison.Ordinal);
        _ = await Assert.That(ex.Message).Contains("certificate file was not found", StringComparison.Ordinal);
        _ = await Assert.That(ex.Message).Contains("private key file was not found", StringComparison.Ordinal);
    }

    /// <summary>Ensures multi-node topology requires CA, node certificate, and internal listen port.</summary>
    [Test]
    public async Task RemotePeersRequireCaForListenPort()
    {
        var options = new MtlsOptions();

        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static value => value.Validate(6001, true));
        _ = await Assert.That(ex.Message).Contains("SQUIRIX_CLUSTER_MTLS_CA_PATH", StringComparison.Ordinal);
        _ = await Assert.That(ex.Message).Contains("SQUIRIX_CLUSTER_MTLS_CERT_PFX_PATH", StringComparison.Ordinal);
        _ = await Assert.That(ex.Message).Contains("SQUIRIX_CLUSTER_MTLS_INTERNAL_PORT", StringComparison.Ordinal);
    }

    /// <summary>Ensures standalone topology does not require cluster mTLS material.</summary>
    [Test]
    public void StandaloneTopologyOmitsCertificatePaths()
    {
        var options = new MtlsOptions();

        options.Validate(6001, false);
    }

    /// <summary>Ensures startup validation allows standalone topology without mTLS material.</summary>
    [Test]
    public async Task ValidatorAcceptsTopologyMtlsMaterial()
    {
        var cluster = new TopologyOptions(new ServerPeer { NodeId = "node-a", Uri = new Uri("https://localhost:6001") })
        {
            ClusterId = "test",
            NodeId = "node-a",
            Uri = new Uri("https://localhost:6001"),
        };
        var validator = new MtlsOptionsValidator(cluster);

        var result = validator.Validate(null, new MtlsOptions());

        _ = await Assert.That(result.Failed).IsFalse();
    }
}
