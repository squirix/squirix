using System;
using System.Net.Http;
using System.Net.Security;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>Unit tests for outbound cluster gRPC transport handler configuration.</summary>
[Immutable]
public sealed class GrpcTransportEndpointsTests : ServerUnitTestBase
{
    /// <summary>Ensures disabled material keeps the default HTTPS handler without a client certificate.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the created handler is not a <see cref="SocketsHttpHandler" />.</exception>
    [Test]
    public async Task DisabledChannelUsesDefaultHandler()
    {
        using var createdHandler = TestCertificates.CreateDefaultChannelHandler();
        _ = await Assert.That(createdHandler.SslOptions.ClientCertificates).IsNull();
    }

    /// <summary>Ensures enabled cluster mTLS attaches the local node certificate to outbound calls.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MtlsHandlerAttachesLocalNodeCert(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
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

        var certificates = await Assert.That(handler.SslOptions.ClientCertificates).IsNotNull();
        _ = await Assert.That(certificates.Count).IsEqualTo(1);
        var clientCertificate = await Assert.That(certificates[0]).IsNotNull();
        _ = await Assert.That(clientCertificate).IsEqualTo(material.NodeCertificate);
    }

    /// <summary>Ensures the outbound handler rejects missing peer server certificates.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">Thrown when the remote certificate validation callback was not configured.</exception>
    [Test]
    public async Task MtlsHandlerRejectsUntrustedPeerCert(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
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
        var callback = ThrowHelper.Required(handler.SslOptions.RemoteCertificateValidationCallback, "Remote certificate validation callback was not configured.");

        _ = await Assert.That(callback(this, null, null, SslPolicyErrors.None)).IsFalse();
    }
}
