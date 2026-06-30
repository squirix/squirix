using System;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Hosting;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Unit tests for Kestrel HTTPS and cluster mTLS listener configuration.</summary>
public sealed class SquirixKestrelConfigurationTests
{
    /// <summary>Ensures enabled cluster mTLS can configure a dedicated internal listener.</summary>
    [Fact]
    public async Task ConfigureKestrelWithMtlsBuildsInternalListener()
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(TestContext.Current.CancellationToken);
        var primaryPort = ListenPortPool.ServerUnitTests.AllocatePort();
        var internalPort = ListenPortPool.ServerUnitTests.AllocatePort();
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
            Uri = new Uri($"https://localhost:{primaryPort.ToString(CultureInfo.InvariantCulture)}"),
            Peers =
            [
                new Peer { NodeId = "node-a", Uri = new Uri($"https://localhost:{primaryPort.ToString(CultureInfo.InvariantCulture)}") },
                new Peer { NodeId = "node-b", Uri = new Uri("https://localhost:6002") },
            ],
        };

        SquirixKestrelConfiguration.ConfigureKestrel(builder, new Uri($"https://localhost:{primaryPort.ToString(CultureInfo.InvariantCulture)}"), cluster, options, material);

        await using var app = builder.Build();
        Assert.NotNull(app);
    }

    /// <summary>Ensures disabled cluster mTLS keeps the primary HTTPS listener configuration buildable.</summary>
    [Fact]
    public void ConfigureKestrelWithStandaloneTopologyBuildsPrimaryListenerOnly()
    {
        var port = ListenPortPool.ServerUnitTests.AllocatePort();
        var builder = WebApplication.CreateBuilder();
        var options = new MtlsOptions();
        var material = MtlsCertificateMaterial.Load(options, port, false);

        var cluster = new ClusterConfig
        {
            ClusterId = "test",
            NodeId = "node-a",
            Uri = new Uri($"https://localhost:{port.ToString(CultureInfo.InvariantCulture)}"),
            Peers = [new Peer { NodeId = "node-a", Uri = new Uri($"https://localhost:{port.ToString(CultureInfo.InvariantCulture)}") }],
        };

        SquirixKestrelConfiguration.ConfigureKestrel(builder, new Uri($"https://localhost:{port.ToString(CultureInfo.InvariantCulture)}"), cluster, options, material);

        using var app = builder.Build();
        Assert.NotNull(app);
    }

    /// <summary>Ensures plaintext HTTP cluster URLs are rejected.</summary>
    [Fact]
    public void EnsureHttpsTransportRejectsPlaintextHttpUrl()
    {
        var cluster = new ClusterConfig
        {
            ClusterId = "test",
            NodeId = "node-a",
            Uri = new Uri("http://localhost:5001"),
        };

        var ex = Assert.Throws<InvalidOperationException>(() => SquirixKestrelConfiguration.EnsureHttpsTransport(cluster));
        Assert.Contains("HTTPS", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Ensures the Kestrel validation helper delegates to cluster trust-root validation.</summary>
    [Fact]
    public async Task ValidateClientCertificateAcceptsCertificateSignedByClusterCa()
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(TestContext.Current.CancellationToken);
        using var peerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");

        Assert.True(SquirixKestrelConfiguration.ValidateClientCertificate(peerCertificate, bundle.Ca, ["node-b"]));
    }

    /// <summary>Ensures the Kestrel validation helper rejects missing client certificates.</summary>
    [Fact]
    public async Task ValidateClientCertificateRejectsMissingCertificate()
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(TestContext.Current.CancellationToken);

        Assert.False(SquirixKestrelConfiguration.ValidateClientCertificate(null, bundle.Ca, ["node-b"]));
    }
}
