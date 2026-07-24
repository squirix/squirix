using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Squirix.Server.Cluster.Transport;

namespace Squirix.Server.TestKit.Mtls;

/// <summary>Test-only certificate utilities for multi-node cluster mTLS scenarios. Not for production use.</summary>
public static class TestCertificates
{
    /// <summary>Creates an outbound handler that trusts the cluster CA but does not present a client certificate.</summary>
    /// <param name="trustAnchor">Configured cluster trust root.</param>
    /// <param name="expectedPeerNodeId">Configured cluster node identifier for the remote peer.</param>
    /// <returns>A handler for negative inter-node mTLS client-auth tests.</returns>
    public static SocketsHttpHandler CreateClusterCaTrustingHandlerNoClientCert(X509Certificate2 trustAnchor, string expectedPeerNodeId)
    {
        ArgumentNullException.ThrowIfNull(trustAnchor);
        ArgumentException.ThrowIfNullOrWhiteSpace(expectedPeerNodeId);

        return new SocketsHttpHandler
        {
            UseProxy = false,
            EnableMultipleHttp2Connections = true,
            SslOptions = new SslClientAuthenticationOptions
            {
                ApplicationProtocols = [SslApplicationProtocol.Http2, SslApplicationProtocol.Http11],
                RemoteCertificateValidationCallback = (_, certificate, _, _) =>
                {
                    if (certificate is null)
                        return false;

                    using var peerCertificate = new X509Certificate2(certificate);
                    return MtlsClientCertificateValidator.ValidateForExpectedNodeId(peerCertificate, trustAnchor, expectedPeerNodeId);
                },
            },
        };
    }

    /// <summary>Creates the default HTTP handler for HTTPS gRPC channels in tests.</summary>
    /// <returns>A handler suitable for secure gRPC transport.</returns>
    public static SocketsHttpHandler CreateDefaultChannelHandler() => new();

    /// <summary>Creates an outbound cluster mTLS HTTP handler with explicit client certificate material.</summary>
    /// <param name="clientCertificates">Client certificates presented to the peer.</param>
    /// <param name="trustAnchor">Configured cluster trust root.</param>
    /// <param name="expectedPeerNodeId">Configured cluster node identifier for the remote peer.</param>
    /// <returns>A handler configured for inter-node mutual TLS.</returns>
    public static SocketsHttpHandler CreateMtlsHandler(X509CertificateCollection clientCertificates, X509Certificate2 trustAnchor, string expectedPeerNodeId)
    {
        ArgumentNullException.ThrowIfNull(clientCertificates);
        ArgumentNullException.ThrowIfNull(trustAnchor);
        ArgumentException.ThrowIfNullOrWhiteSpace(expectedPeerNodeId);

        var validator = new PeerCertificateValidator(trustAnchor, expectedPeerNodeId);
        return new SocketsHttpHandler
        {
            UseProxy = false,
            EnableMultipleHttp2Connections = true,
            SslOptions = new SslClientAuthenticationOptions
            {
                ClientCertificates = clientCertificates,
                ApplicationProtocols = Http2PreferredProtocols,
                RemoteCertificateValidationCallback = validator.Validate,
            },
        };
    }

    /// <summary>Creates an outbound cluster mTLS HTTP handler with explicit client certificate material.</summary>
    /// <param name="clientCertificate">Client certificate presented to the peer.</param>
    /// <param name="trustAnchor">Configured cluster trust root.</param>
    /// <param name="expectedPeerNodeId">Configured cluster node identifier for the remote peer.</param>
    /// <returns>A handler configured for inter-node mutual TLS.</returns>
    public static SocketsHttpHandler CreateMtlsHandler(X509Certificate2 clientCertificate, X509Certificate2 trustAnchor, string expectedPeerNodeId)
    {
        ArgumentNullException.ThrowIfNull(clientCertificate);
        ArgumentNullException.ThrowIfNull(trustAnchor);
        ArgumentException.ThrowIfNullOrWhiteSpace(expectedPeerNodeId);

        return new SocketsHttpHandler
        {
            UseProxy = false,
            EnableMultipleHttp2Connections = true,
            SslOptions = new SslClientAuthenticationOptions
            {
                ClientCertificates = [clientCertificate],
                ApplicationProtocols = [SslApplicationProtocol.Http2, SslApplicationProtocol.Http11],
                RemoteCertificateValidationCallback = (_, certificate, _, _) => ValidatePeerServerCertificate(certificate, trustAnchor, expectedPeerNodeId),
            },
        };
    }

    /// <summary>Creates a peer certificate signed by the provided test CA.</summary>
    /// <param name="issuer">Issuing test certificate authority.</param>
    /// <param name="commonName">ServerPeer certificate common name.</param>
    /// <param name="notBefore">Optional validity start.</param>
    /// <param name="notAfter">Optional validity end.</param>
    /// <returns>A peer certificate with a private key.</returns>
    public static X509Certificate2 CreatePeerCertificate(X509Certificate2 issuer, string commonName, DateTimeOffset? notBefore = null, DateTimeOffset? notAfter = null)
    {
        ArgumentNullException.ThrowIfNull(issuer);
        ArgumentException.ThrowIfNullOrWhiteSpace(commonName);

        var effectiveNotBefore = notBefore ?? new DateTimeOffset(issuer.NotBefore.ToUniversalTime());
        var effectiveNotAfter = notAfter ?? new DateTimeOffset(issuer.NotAfter.ToUniversalTime());

        using var peerKey = RSA.Create(2048);
        var peerRequest = new CertificateRequest($"CN={commonName}", peerKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        peerRequest.AddClusterNodeExtensions();
        var peerPublic = peerRequest.Create(issuer, effectiveNotBefore, effectiveNotAfter, Guid.NewGuid().ToByteArray());
        return peerPublic.CopyWithPrivateKey(peerKey);
    }

    /// <summary>Creates a standalone test certificate authority.</summary>
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

    /// <summary>Validates a peer server certificate against the configured cluster trust root.</summary>
    /// <param name="serverCertificate">The presented peer server certificate.</param>
    /// <param name="trustAnchor">Configured cluster trust root.</param>
    /// <param name="expectedPeerNodeId">Configured cluster node identifier for the remote peer.</param>
    /// <returns><see langword="true" /> when the certificate is trusted for inter-node traffic.</returns>
    public static bool ValidatePeerServerCertificate(X509Certificate? serverCertificate, X509Certificate2 trustAnchor, string expectedPeerNodeId)
    {
        if (serverCertificate is null)
            return false;

        using var certificate = new X509Certificate2(serverCertificate);
        return MtlsClientCertificateValidator.ValidateForExpectedNodeId(certificate, trustAnchor, expectedPeerNodeId);
    }

    /// <summary>Loads an exportable certificate copy suitable for Schannel client and server authentication.</summary>
    /// <param name="certificate">Source certificate with private key.</param>
    /// <returns>Exportable certificate copy.</returns>
    internal static X509Certificate2 LoadExportableCertificate(X509Certificate2 certificate) =>
        X509CertificateLoader.LoadPkcs12(certificate.Export(X509ContentType.Pfx), null, X509KeyStorageFlags.Exportable);

    private sealed class PeerCertificateValidator
    {
        private readonly string _expectedPeerNodeId;
        private readonly X509Certificate2 _trustAnchor;

        internal PeerCertificateValidator(X509Certificate2 trustAnchor, string expectedPeerNodeId)
        {
            _trustAnchor = trustAnchor;
            _expectedPeerNodeId = expectedPeerNodeId;
        }

        internal bool Validate(object? sender, X509Certificate? certificate, X509Chain? chain, SslPolicyErrors errors)
        {
            _ = sender;
            _ = chain;
            _ = errors;
            return ValidatePeerServerCertificate(certificate, _trustAnchor, _expectedPeerNodeId);
        }
    }
}
