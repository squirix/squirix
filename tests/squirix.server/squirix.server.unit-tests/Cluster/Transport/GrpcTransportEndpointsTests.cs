using System;
using System.Net.Http;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Squirix.Server.Cluster.Transport;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>
/// Unit tests for outbound cluster gRPC transport handler configuration.
/// </summary>
public sealed class GrpcTransportEndpointsTests
{
    /// <summary>
    /// Ensures disabled cluster mTLS keeps the default HTTPS handler without a client certificate.
    /// </summary>
    [Fact]
    public void CreateChannelHandlerWithoutMtlsUsesDefaultHandler()
    {
        using var handler = (SocketsHttpHandler)GrpcTransportEndpoints.CreateChannelHandler();

        Assert.Null(handler.SslOptions.ClientCertificates);
    }

    /// <summary>
    /// Ensures disabled material keeps the default HTTPS handler without a client certificate.
    /// </summary>
    [Fact]
    public void CreateChannelHandlerWithDisabledMaterialUsesDefaultHandler()
    {
        using var handler = (SocketsHttpHandler)GrpcTransportEndpoints.CreateChannelHandler(MtlsCertificateMaterial.Disabled);

        Assert.Null(handler.SslOptions.ClientCertificates);
    }

    /// <summary>
    /// Ensures enabled cluster mTLS attaches the local node certificate to outbound calls.
    /// </summary>
    [Fact]
    public void CreateMtlsHandlerAttachesLocalNodeCertificate()
    {
        using var bundle = MtlsTestCertificateFactory.Create();
        using var material = MtlsCertificateMaterial.Load(
            new MtlsOptions
            {
                CaPath = bundle.CaPath,
                CertPfxPath = bundle.PfxPath,
                InternalListenPort = 6101,
            },
            6001,
            true);

        using var handler = GrpcTransportEndpoints.CreateMtlsHandler(material);

        Assert.NotNull(handler.SslOptions.ClientCertificates);
        var clientCertificate = Assert.Single(handler.SslOptions.ClientCertificates);
        Assert.Equal(material.NodeCertificate, clientCertificate);
    }

    /// <summary>
    /// Ensures peer server certificates signed by the cluster CA are accepted.
    /// </summary>
    [Fact]
    public void ValidatePeerServerCertificateAcceptsCertificateSignedByClusterCa()
    {
        using var bundle = MtlsTestCertificateFactory.Create();
        using var peerServerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "peer-node-b");

        Assert.True(GrpcTransportEndpoints.ValidatePeerServerCertificate(peerServerCertificate, null, bundle.Ca));
    }

    /// <summary>
    /// Ensures peer server certificates signed by an untrusted CA are rejected.
    /// </summary>
    [Fact]
    public void ValidatePeerServerCertificateRejectsCertificateSignedByUntrustedCa()
    {
        using var bundle = MtlsTestCertificateFactory.Create();
        using var untrustedCa = CreateStandaloneCa("CN=Other CA");
        using var peerServerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(untrustedCa, "peer-node-b");

        Assert.False(GrpcTransportEndpoints.ValidatePeerServerCertificate(peerServerCertificate, null, bundle.Ca));
    }

    /// <summary>
    /// Ensures expired peer server certificates are rejected.
    /// </summary>
    [Fact]
    public void ValidatePeerServerCertificateRejectsExpiredCertificate()
    {
        using var bundle = MtlsTestCertificateFactory.Create();
        var notBefore = bundle.Ca.NotBefore;
        var notAfter = notBefore.AddHours(1);
        using var expiredServerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "expired-peer", notBefore, notAfter);

        Assert.False(GrpcTransportEndpoints.ValidatePeerServerCertificate(expiredServerCertificate, null, bundle.Ca));
    }

    /// <summary>
    /// Ensures the outbound handler rejects missing peer server certificates.
    /// </summary>
    [Fact]
    public void CreateMtlsHandlerRejectsMissingPeerServerCertificate()
    {
        using var bundle = MtlsTestCertificateFactory.Create();
        using var material = MtlsCertificateMaterial.Load(
            new MtlsOptions
            {
                CaPath = bundle.CaPath,
                CertPfxPath = bundle.PfxPath,
                InternalListenPort = 6102,
            },
            6001,
            true);
        using var handler = GrpcTransportEndpoints.CreateMtlsHandler(material);
        var callback = handler.SslOptions.RemoteCertificateValidationCallback ?? throw new InvalidOperationException("Remote certificate validation callback was not configured.");

        Assert.False(callback(this, null, null, sslPolicyErrors: System.Net.Security.SslPolicyErrors.None));
    }

    private static X509Certificate2 CreateStandaloneCa(string distinguishedName)
    {
        using var caKey = RSA.Create(2048);
        var caRequest = new CertificateRequest(distinguishedName, caKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        caRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        var notBefore = DateTimeOffset.UtcNow.AddDays(-1);
        var notAfter = notBefore.AddDays(30);
        return caRequest.CreateSelfSigned(notBefore, notAfter);
    }
}
