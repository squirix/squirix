using System;
using Microsoft.AspNetCore.Builder;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Hosting;
using Squirix.Server.TestKit.Http;
using Squirix.Server.UnitTests.Cluster.Transport;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Hosting;

/// <summary>
/// Unit tests for Kestrel HTTPS and cluster mTLS listener configuration.
/// </summary>
public sealed class SquirixKestrelConfigurationTests
{
    /// <summary>
    /// Ensures plaintext HTTP cluster URLs are rejected.
    /// </summary>
    [Fact]
    public void EnsureHttpsTransportRejectsPlaintextHttpUrl()
    {
        var cluster = new ClusterConfig
        {
            ClusterId = "test",
            NodeId = "node-a",
            Url = "http://localhost:5001",
        };

        var ex = Assert.Throws<InvalidOperationException>(() => SquirixKestrelConfiguration.EnsureHttpsTransport(cluster));
        Assert.Contains("HTTPS", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Ensures disabled cluster mTLS keeps the primary HTTPS listener configuration buildable.
    /// </summary>
    [Fact]
    public void ConfigureKestrelWithDisabledClusterMtlsBuildsPrimaryListenerOnly()
    {
        var port = new PortAllocator(31000, 31999).Allocate();
        var builder = WebApplication.CreateBuilder();
        var options = new ClusterMtlsOptions { Enabled = false };
        var material = ClusterMtlsCertificateMaterial.Load(options);

        SquirixKestrelConfiguration.ConfigureKestrel(builder, new Uri($"https://localhost:{port}"), options, material);

        using var app = builder.Build();
        Assert.NotNull(app);
    }

    /// <summary>
    /// Ensures enabled cluster mTLS can configure a dedicated internal listener.
    /// </summary>
    [Fact]
    public void ConfigureKestrelWithEnabledClusterMtlsBuildsInternalListener()
    {
        using var bundle = ClusterMtlsTestCertificateFactory.Create();
        var primaryPort = new PortAllocator(32000, 32999).Allocate();
        var internalPort = new PortAllocator(33000, 33999).Allocate();
        var options = new ClusterMtlsOptions
        {
            Enabled = true,
            CaPath = bundle.CaPath,
            CertPfxPath = bundle.PfxPath,
            InternalListenPort = internalPort,
        };
        using var material = ClusterMtlsCertificateMaterial.Load(options, primaryPort);
        var builder = WebApplication.CreateBuilder();

        SquirixKestrelConfiguration.ConfigureKestrel(builder, new Uri($"https://localhost:{primaryPort}"), options, material);

        using var app = builder.Build();
        Assert.NotNull(app);
    }

    /// <summary>
    /// Ensures the Kestrel validation helper delegates to cluster trust-root validation.
    /// </summary>
    [Fact]
    public void ValidateClusterClientCertificateAcceptsCertificateSignedByClusterCa()
    {
        using var bundle = ClusterMtlsTestCertificateFactory.Create();
        using var peerCertificate = ClusterMtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "peer-node-b");

        Assert.True(SquirixKestrelConfiguration.ValidateClusterClientCertificate(peerCertificate, null, bundle.Ca));
    }

    /// <summary>
    /// Ensures the Kestrel validation helper rejects missing client certificates.
    /// </summary>
    [Fact]
    public void ValidateClusterClientCertificateRejectsMissingCertificate()
    {
        using var bundle = ClusterMtlsTestCertificateFactory.Create();

        Assert.False(SquirixKestrelConfiguration.ValidateClusterClientCertificate(null, null, bundle.Ca));
    }
}
