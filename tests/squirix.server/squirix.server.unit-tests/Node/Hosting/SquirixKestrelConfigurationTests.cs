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
    /// Ensures enabled cluster mTLS can configure a dedicated internal listener.
    /// </summary>
    [Fact]
    public void ConfigureKestrelWithMtlsBuildsInternalListener()
    {
        using var bundle = MtlsTestCertificateFactory.Create();
        using var primaryAllocator = new PortAllocator(32000, 32999);
        using var internalAllocator = new PortAllocator(33000, 33999);
        var primaryPort = primaryAllocator.Allocate();
        var internalPort = internalAllocator.Allocate();
        var options = new MtlsOptions
        {
            CaPath = bundle.CaPath,
            CertPfxPath = bundle.PfxPath,
            InternalListenPort = internalPort,
        };
        using var material = MtlsCertificateMaterial.Load(options, primaryPort, true, "node-a");
        var builder = WebApplication.CreateBuilder();
        var cluster = new ClusterConfig
        {
            ClusterId = "test",
            NodeId = "node-a",
            Url = $"https://localhost:{primaryPort}",
            Peers =
            [
                new Peer { NodeId = "node-a", Url = $"https://localhost:{primaryPort}" },
                new Peer { NodeId = "node-b", Url = "https://localhost:6002" },
            ],
        };

        SquirixKestrelConfiguration.ConfigureKestrel(builder, new Uri($"https://localhost:{primaryPort}"), cluster, options, material);

        using var app = builder.Build();
        Assert.NotNull(app);
    }

    /// <summary>
    /// Ensures disabled cluster mTLS keeps the primary HTTPS listener configuration buildable.
    /// </summary>
    [Fact]
    public void ConfigureKestrelWithStandaloneTopologyBuildsPrimaryListenerOnly()
    {
        using var allocator = new PortAllocator(31000, 31999);
        var port = allocator.Allocate();
        var builder = WebApplication.CreateBuilder();
        var options = new MtlsOptions();
        var material = MtlsCertificateMaterial.Load(options, port, false);

        var cluster = new ClusterConfig
        {
            ClusterId = "test",
            NodeId = "node-a",
            Url = $"https://localhost:{port}",
            Peers = [new Peer { NodeId = "node-a", Url = $"https://localhost:{port}" }],
        };

        SquirixKestrelConfiguration.ConfigureKestrel(builder, new Uri($"https://localhost:{port}"), cluster, options, material);

        using var app = builder.Build();
        Assert.NotNull(app);
    }

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
    /// Ensures the Kestrel validation helper delegates to cluster trust-root validation.
    /// </summary>
    [Fact]
    public void ValidateClientCertificateAcceptsCertificateSignedByClusterCa()
    {
        using var bundle = MtlsTestCertificateFactory.Create();
        using var peerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");

        Assert.True(SquirixKestrelConfiguration.ValidateClientCertificate(peerCertificate, bundle.Ca, ["node-b"]));
    }

    /// <summary>
    /// Ensures the Kestrel validation helper rejects missing client certificates.
    /// </summary>
    [Fact]
    public void ValidateClientCertificateRejectsMissingCertificate()
    {
        using var bundle = MtlsTestCertificateFactory.Create();

        Assert.False(SquirixKestrelConfiguration.ValidateClientCertificate(null, bundle.Ca, ["node-b"]));
    }
}
