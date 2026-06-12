using System;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Hosting;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>
/// Unit tests for <see cref="ClusterMtlsOptions" /> validation.
/// </summary>
public sealed class ClusterMtlsOptionsTests
{
    /// <summary>
    /// Ensures standalone topology does not require cluster mTLS material.
    /// </summary>
    [Fact]
    public void StandaloneTopologyDoesNotRequireCertificatePaths()
    {
        var options = new ClusterMtlsOptions();

        var ex = Record.Exception(() => options.Validate(6001, false));
        Assert.Null(ex);
    }

    /// <summary>
    /// Ensures multi-node topology requires CA, node certificate, and internal listen port.
    /// </summary>
    [Fact]
    public void RemotePeersRequireCaNodeCertificateAndInternalListenPort()
    {
        var options = new ClusterMtlsOptions();

        var ex = Assert.Throws<InvalidOperationException>(() => options.Validate(6001, true));
        Assert.Contains("SQUIRIX_CLUSTER_MTLS_CA_PATH", ex.Message, StringComparison.Ordinal);
        Assert.Contains("SQUIRIX_CLUSTER_MTLS_CERT_PFX_PATH", ex.Message, StringComparison.Ordinal);
        Assert.Contains("SQUIRIX_CLUSTER_MTLS_INTERNAL_PORT", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Ensures multi-node topology rejects an internal port that matches the primary listener.
    /// </summary>
    [Fact]
    public void RemotePeersRejectInternalPortMatchingPrimaryListener()
    {
        using var bundle = ClusterMtlsTestCertificateFactory.Create();
        var options = new ClusterMtlsOptions
        {
            CaPath = bundle.CaPath,
            CertPfxPath = bundle.PfxPath,
            InternalListenPort = 6001,
        };

        var ex = Assert.Throws<InvalidOperationException>(() => options.Validate(6001, true));
        Assert.Contains("must differ", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Ensures missing files fail validation for multi-node topology.
    /// </summary>
    [Fact]
    public void RemotePeersRejectMissingFiles()
    {
        var missingRoot = DirectoryKit.CreateTempDirectory("squirix-cluster-mtls-missing");
        var options = new ClusterMtlsOptions
        {
            CaPath = PathKit.Combine(missingRoot, "missing-ca.crt"),
            CertPath = PathKit.Combine(missingRoot, "missing-node.crt"),
            KeyPath = PathKit.Combine(missingRoot, "missing-node.key"),
            InternalListenPort = 6101,
        };

        try
        {
            var ex = Assert.Throws<InvalidOperationException>(() => options.Validate(6001, true));
            Assert.Contains("CA file was not found", ex.Message, StringComparison.Ordinal);
            Assert.Contains("certificate file was not found", ex.Message, StringComparison.Ordinal);
            Assert.Contains("private key file was not found", ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(missingRoot);
        }
    }

    /// <summary>
    /// Ensures PFX and PEM inputs cannot be mixed.
    /// </summary>
    [Fact]
    public void RemotePeersRejectMixedPfxAndPemPaths()
    {
        using var bundle = ClusterMtlsTestCertificateFactory.Create();
        var options = new ClusterMtlsOptions
        {
            CaPath = bundle.CaPath,
            CertPfxPath = bundle.PfxPath,
            CertPath = bundle.CertPath,
            KeyPath = bundle.KeyPath,
            InternalListenPort = 6101,
        };

        var ex = Assert.Throws<InvalidOperationException>(() => options.Validate(6001, true));
        Assert.Contains("not both", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Ensures startup validation allows standalone topology without mTLS material.
    /// </summary>
    [Fact]
    public void StartupValidatorAllowsStandaloneTopologyWithoutMtlsMaterial()
    {
        var cluster = new ClusterConfig
        {
            ClusterId = "test",
            NodeId = "node-a",
            Url = "https://localhost:6001",
            Peers = [new Peer { NodeId = "node-a", Url = "https://localhost:6001" }],
        };
        var validator = new SquirixOptionsValidators.ClusterMtlsOptionsValidator(cluster);

        var result = validator.Validate(null, new ClusterMtlsOptions());

        Assert.False(result.Failed);
    }
}
