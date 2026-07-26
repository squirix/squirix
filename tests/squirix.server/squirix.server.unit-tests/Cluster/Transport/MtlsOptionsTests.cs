using System;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>
/// Unit tests for <see cref="MtlsOptions" /> validation.
/// </summary>
public sealed class MtlsOptionsTests
{
    /// <summary>Ensures multi-node topology rejects an internal port that matches the primary listener.</summary>
    [Fact]
    public async Task RemotePeersRejectInternalMatchingPrimaryListener()
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(TestContext.Current.CancellationToken);
        var options = new MtlsOptions
        {
            CaPath = bundle.CaPath,
            CertPfxPath = bundle.PfxPath,
            InternalListenPort = 6001,
        };

        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static value => value.Validate(6001, true));
        Assert.Contains("must differ", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Ensures missing files fail validation for multi-node topology.</summary>
    [Fact]
    public void RemotePeersRejectMissingFiles()
    {
        using var missingRoot = new TempDirectory("squirix-cluster-mtls-missing");
        var options = new MtlsOptions
        {
            CaPath = NodePathKit.Combine(missingRoot, "missing-ca.crt"),
            CertPath = NodePathKit.Combine(missingRoot, "missing-node.crt"),
            KeyPath = NodePathKit.Combine(missingRoot, "missing-node.key"),
            InternalListenPort = 6101,
        };

        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static value => value.Validate(6001, true));
        Assert.Contains("CA file was not found", ex.Message, StringComparison.Ordinal);
        Assert.Contains("certificate file was not found", ex.Message, StringComparison.Ordinal);
        Assert.Contains("private key file was not found", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Ensures PFX and PEM inputs cannot be mixed.</summary>
    [Fact]
    public async Task RemotePeersRejectMixedPfxAndPemPaths()
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(TestContext.Current.CancellationToken);
        var options = new MtlsOptions
        {
            CaPath = bundle.CaPath,
            CertPfxPath = bundle.PfxPath,
            CertPath = bundle.CertPath,
            KeyPath = bundle.KeyPath,
            InternalListenPort = 6101,
        };

        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static value => value.Validate(6001, true));
        Assert.Contains("not both", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Ensures multi-node topology requires CA, node certificate, and internal listen port.</summary>
    [Fact]
    public void RemotePeersRequireCaCertificateInternalListenPort()
    {
        var options = new MtlsOptions();

        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static value => value.Validate(6001, true));
        Assert.Contains("SQUIRIX_CLUSTER_MTLS_CA_PATH", ex.Message, StringComparison.Ordinal);
        Assert.Contains("SQUIRIX_CLUSTER_MTLS_CERT_PFX_PATH", ex.Message, StringComparison.Ordinal);
        Assert.Contains("SQUIRIX_CLUSTER_MTLS_INTERNAL_PORT", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Ensures standalone topology does not require cluster mTLS material.</summary>
    [Fact]
    public void StandaloneTopologyDoesNotRequireCertificatePaths()
    {
        var options = new MtlsOptions();

        options.Validate(6001, false);
    }

    /// <summary>Ensures startup validation allows standalone topology without mTLS material.</summary>
    [Fact]
    public void StartupValidatorAllowsTopologyMtlsMaterial()
    {
        var cluster = new TopologyOptions(new ServerPeer { NodeId = "node-a", Uri = new Uri("https://localhost:6001") })
        {
            ClusterId = "test",
            NodeId = "node-a",
            Uri = new Uri("https://localhost:6001"),
        };
        var validator = new MtlsOptionsValidator(cluster);

        var result = validator.Validate(null, new MtlsOptions());

        Assert.False(result.Failed);
    }
}
