using System;
using System.Net.Http;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Squirix.Server.Cluster.Transport;

namespace Squirix.Server.TestKit.Cluster;

/// <summary>
/// Test-only certificate utilities for multi-node cluster mTLS scenarios. Not for production use.
/// </summary>
public static class MtlsTestCertificates
{
    /// <summary>
    /// Creates an outbound handler that trusts the cluster CA but does not present a client certificate.
    /// </summary>
    /// <param name="trustAnchor">Configured cluster trust root.</param>
    /// <returns>A handler for negative inter-node mTLS client-auth tests.</returns>
    public static SocketsHttpHandler CreateClusterCaTrustingHandlerWithoutClientCertificate(X509Certificate2 trustAnchor)
    {
        ArgumentNullException.ThrowIfNull(trustAnchor);

        return new SocketsHttpHandler
        {
            UseProxy = false,
            EnableMultipleHttp2Connections = true,
            SslOptions = new SslClientAuthenticationOptions
            {
                ApplicationProtocols = [SslApplicationProtocol.Http2, SslApplicationProtocol.Http11],
                RemoteCertificateValidationCallback = (_, certificate, _, _) => GrpcTransportEndpoints.ValidatePeerServerCertificate(certificate, trustAnchor),
            },
        };
    }

    /// <summary>
    /// Creates a peer certificate signed by the provided test CA.
    /// </summary>
    /// <param name="issuer">Issuing test certificate authority.</param>
    /// <param name="commonName">Peer certificate common name.</param>
    /// <param name="notBefore">Optional validity start.</param>
    /// <param name="notAfter">Optional validity end.</param>
    /// <returns>A peer certificate with a private key.</returns>
    public static X509Certificate2 CreatePeerCertificate(X509Certificate2 issuer, string commonName, DateTimeOffset? notBefore = null, DateTimeOffset? notAfter = null)
    {
        ArgumentNullException.ThrowIfNull(issuer);
        ArgumentException.ThrowIfNullOrWhiteSpace(commonName);

        var effectiveNotBefore = notBefore ?? issuer.NotBefore;
        var effectiveNotAfter = notAfter ?? issuer.NotAfter;

        using var peerKey = RSA.Create(2048);
        var peerRequest = new CertificateRequest($"CN={commonName}", peerKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        peerRequest.AddClusterNodeExtensions();
        var peerPublic = peerRequest.Create(issuer, effectiveNotBefore, effectiveNotAfter, Guid.NewGuid().ToByteArray());
        return peerPublic.CopyWithPrivateKey(peerKey);
    }

    /// <summary>
    /// Creates a standalone test certificate authority.
    /// </summary>
    /// <param name="commonName">Certificate authority distinguished name common name.</param>
    /// <returns>A self-signed test CA certificate.</returns>
    public static X509Certificate2 CreateStandaloneCertificateAuthority(string commonName = "CN=Squirix E2E Untrusted Test CA")
    {
        using var caKey = RSA.Create(2048);
        var caRequest = new CertificateRequest(commonName, caKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        caRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        var notBefore = DateTimeOffset.UtcNow.AddDays(-1);
        var notAfter = notBefore.AddDays(30);
        return caRequest.CreateSelfSigned(notBefore, notAfter);
    }

    /// <summary>
    /// Loads an exportable certificate copy suitable for Schannel client and server authentication.
    /// </summary>
    /// <param name="certificate">Source certificate with private key.</param>
    /// <returns>Exportable certificate copy.</returns>
    internal static X509Certificate2 LoadExportableCertificate(X509Certificate2 certificate) =>
        X509CertificateLoader.LoadPkcs12(certificate.Export(X509ContentType.Pfx), null, X509KeyStorageFlags.Exportable);
}
