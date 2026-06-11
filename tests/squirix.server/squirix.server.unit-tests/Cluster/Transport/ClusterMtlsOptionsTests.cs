using System;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>
/// Unit tests for <see cref="ClusterMtlsOptions" /> validation.
/// </summary>
public sealed class ClusterMtlsOptionsTests
{
    /// <summary>
    /// Ensures disabled cluster mTLS does not require certificate paths.
    /// </summary>
    [Fact]
    public void DisabledOptionsDoNotRequireCertificatePaths()
    {
        var options = new ClusterMtlsOptions { Enabled = false };

        var ex = Record.Exception(() => options.Validate());
        Assert.Null(ex);
    }

    /// <summary>
    /// Ensures enabled cluster mTLS requires a CA path and node certificate material.
    /// </summary>
    [Fact]
    public void EnabledOptionsRequireCaAndNodeCertificatePaths()
    {
        var options = new ClusterMtlsOptions { Enabled = true };

        var ex = Assert.Throws<InvalidOperationException>(() => options.Validate());
        Assert.Contains("SQUIRIX_CLUSTER_MTLS_CA_PATH", ex.Message, StringComparison.Ordinal);
        Assert.Contains("SQUIRIX_CLUSTER_MTLS_CERT_PFX_PATH", ex.Message, StringComparison.Ordinal);
        Assert.Contains("SQUIRIX_CLUSTER_MTLS_INTERNAL_PORT", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Ensures enabled cluster mTLS rejects an internal port that matches the primary listener.
    /// </summary>
    [Fact]
    public void EnabledOptionsRejectInternalPortMatchingPrimaryListener()
    {
        using var bundle = ClusterMtlsTestCertificateFactory.Create();
        var options = new ClusterMtlsOptions
        {
            Enabled = true,
            CaPath = bundle.CaPath,
            CertPfxPath = bundle.PfxPath,
            InternalListenPort = 6001,
        };

        var ex = Assert.Throws<InvalidOperationException>(() => options.Validate(6001));
        Assert.Contains("must differ", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Ensures missing files fail validation.
    /// </summary>
    [Fact]
    public void EnabledOptionsRejectMissingFiles()
    {
        var missingRoot = DirectoryKit.CreateTempDirectory("squirix-cluster-mtls-missing");
        var options = new ClusterMtlsOptions
        {
            Enabled = true,
            CaPath = PathKit.Combine(missingRoot, "missing-ca.crt"),
            CertPath = PathKit.Combine(missingRoot, "missing-node.crt"),
            KeyPath = PathKit.Combine(missingRoot, "missing-node.key"),
        };

        try
        {
            var ex = Assert.Throws<InvalidOperationException>(() => options.Validate());
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
    public void EnabledOptionsRejectMixedPfxAndPemPaths()
    {
        using var bundle = ClusterMtlsTestCertificateFactory.Create();
        var options = new ClusterMtlsOptions
        {
            Enabled = true,
            CaPath = bundle.CaPath,
            CertPfxPath = bundle.PfxPath,
            CertPath = bundle.CertPath,
            KeyPath = bundle.KeyPath,
        };

        var ex = Assert.Throws<InvalidOperationException>(() => options.Validate());
        Assert.Contains("not both", ex.Message, StringComparison.Ordinal);
    }
}
