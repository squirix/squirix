using System;
using System.Net.Http;
using System.Net.Security;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>Unit tests for outbound cluster gRPC transport handler configuration.</summary>
[Immutable]
public sealed class GrpcTransportEndpointsTests : ServerUnitTestBase
{
    /// <summary>Ensures disabled material keeps the default HTTPS handler without a client certificate.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the created handler is not a <see cref="SocketsHttpHandler" />.</exception>
    [Fact]
    public void DisabledChannelUsesDefaultHandler()
    {
        using var createdHandler = TestCertificates.CreateDefaultChannelHandler();
        Assert.Null(createdHandler.SslOptions.ClientCertificates);
    }

    /// <summary>Ensures enabled cluster mTLS attaches the local node certificate to outbound calls.</summary>
    [Fact]
    public async Task MtlsHandlerAttachesLocalNodeCert()
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(DefaultCancellationToken);
        using var material = MtlsCertificateMaterial.Load(
            new MtlsOptions
            {
                CaPath = bundle.CaPath,
                CertPfxPath = bundle.PfxPath,
                InternalListenPort = 6101,
            },
            6001,
            true,
            "node-a");

        using var handler = TestCertificates.CreateMtlsHandler(material.NodeCertificate!, material.TrustAnchor!, "node-b");

        Assert.NotNull(handler.SslOptions.ClientCertificates);
        var clientCertificate = Assert.Single(handler.SslOptions.ClientCertificates);
        Assert.Equal(material.NodeCertificate, clientCertificate);
    }

    /// <summary>Ensures the outbound handler rejects missing peer server certificates.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the remote certificate validation callback was not configured.</exception>
    [Fact]
    public async Task MtlsHandlerRejectsUntrustedPeerCert()
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(DefaultCancellationToken);
        using var material = MtlsCertificateMaterial.Load(
            new MtlsOptions
            {
                CaPath = bundle.CaPath,
                CertPfxPath = bundle.PfxPath,
                InternalListenPort = 6102,
            },
            6001,
            true,
            "node-a");
        using var handler = TestCertificates.CreateMtlsHandler(material.NodeCertificate!, material.TrustAnchor!, "node-b");
        var callback = handler.SslOptions.RemoteCertificateValidationCallback ?? throw new InvalidOperationException("Remote certificate validation callback was not configured.");

        Assert.False(callback(this, null, null, SslPolicyErrors.None));
    }
}
