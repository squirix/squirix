using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Utils;

namespace Squirix.Server.TestKit.Mtls;

/// <summary>Test-only certificate utilities for multi-node cluster mTLS scenarios. Not for production use.</summary>
public static class TestCertificates
{
    private const int RsaKeySizeBits = 2048;

    private static readonly List<SslApplicationProtocol> Http2PreferredProtocols = [SslApplicationProtocol.Http2, SslApplicationProtocol.Http11];

    /// <summary>Creates an outbound handler that trusts the cluster CA but does not present a client certificate.</summary>
    /// <param name="trustAnchor">Configured cluster trust root.</param>
    /// <param name="expectedPeerNodeId">Configured cluster node identifier for the remote peer.</param>
    /// <returns>A handler for negative internode mTLS client-auth tests.</returns>
    public static SocketsHttpHandler CreateCaTrustingHandlerNoClientCert(X509Certificate2 trustAnchor, string expectedPeerNodeId)
    {
        ArgumentNullException.ThrowIfNull(trustAnchor);
        ArgumentException.ThrowIfNullOrWhiteSpace(expectedPeerNodeId);

        var validator = new PeerCertificateValidator(trustAnchor, expectedPeerNodeId);
        return new SocketsHttpHandler
        {
            UseProxy = false,
            EnableMultipleHttp2Connections = true,
            SslOptions = new SslClientAuthenticationOptions
            {
                ApplicationProtocols = Http2PreferredProtocols,
                RemoteCertificateValidationCallback = validator.Validate,
            },
        };
    }

    /// <summary>Creates the default HTTP handler for HTTPS gRPC channels in tests.</summary>
    /// <returns>A handler suitable for secure gRPC transport.</returns>
    public static SocketsHttpHandler CreateDefaultChannelHandler() => new();

    /// <summary>Creates an outbound cluster mTLS HTTP handler with explicit client certificate material.</summary>
    /// <remarks>On Unix the first collection entry must carry a private key (context creation throws otherwise); other entries are ignored.</remarks>
    /// <param name="clientCertificates">Client certificates presented to the peer.</param>
    /// <param name="trustAnchor">Configured cluster trust root.</param>
    /// <param name="expectedPeerNodeId">Configured cluster node identifier for the remote peer.</param>
    /// <returns>A handler configured for internode mutual TLS.</returns>
    public static SocketsHttpHandler CreateMtlsHandler(X509CertificateCollection clientCertificates, X509Certificate2 trustAnchor, string expectedPeerNodeId)
    {
        ArgumentNullException.ThrowIfNull(clientCertificates);
        ArgumentNullException.ThrowIfNull(trustAnchor);
        ArgumentException.ThrowIfNullOrWhiteSpace(expectedPeerNodeId);

        var validator = new PeerCertificateValidator(trustAnchor, expectedPeerNodeId);

        // Mirrors the product handler: on Unix a prebuilt offline context removes the per-handshake chain build; Windows needs none and fails for ephemeral keys.
        var clientCertificateContext = !OperatingSystem.IsWindows() && clientCertificates is [X509Certificate2 first, ..] ? SslStreamCertificateContext.Create(first, null, true) : null;
        return new SocketsHttpHandler
        {
            UseProxy = false,
            EnableMultipleHttp2Connections = true,
            SslOptions = new SslClientAuthenticationOptions
            {
                ClientCertificates = clientCertificates,
                ClientCertificateContext = clientCertificateContext,
                ApplicationProtocols = Http2PreferredProtocols,
                RemoteCertificateValidationCallback = validator.Validate,
            },
        };
    }

    /// <summary>Creates an outbound cluster mTLS HTTP handler with explicit client certificate material.</summary>
    /// <param name="clientCertificate">Client certificate presented to the peer.</param>
    /// <param name="trustAnchor">Configured cluster trust root.</param>
    /// <param name="expectedPeerNodeId">Configured cluster node identifier for the remote peer.</param>
    /// <returns>A handler configured for internode mutual TLS.</returns>
    public static SocketsHttpHandler CreateMtlsHandler(X509Certificate2 clientCertificate, X509Certificate2 trustAnchor, string expectedPeerNodeId)
    {
        ArgumentNullException.ThrowIfNull(clientCertificate);
        return CreateMtlsHandler([clientCertificate], trustAnchor, expectedPeerNodeId);
    }

    /// <summary>Creates a peer certificate signed by the provided test CA.</summary>
    /// <param name="issuer">Issuing test certificate authority.</param>
    /// <param name="commonName">ServerPeer certificate common name.</param>
    /// <param name="notBefore">Optional validity start.</param>
    /// <param name="notAfter">Optional validity end.</param>
    /// <param name="rsaKey">Whether the certificate key is RSA instead of the default ECDSA P-256; an RSA certificate must be issued by an ECDSA authority.</param>
    /// <returns>A peer certificate with a private key.</returns>
    public static X509Certificate2 CreatePeerCertificate(
        X509Certificate2 issuer,
        string commonName,
        DateTimeOffset? notBefore = null,
        DateTimeOffset? notAfter = null,
        bool rsaKey = false)
    {
        ArgumentNullException.ThrowIfNull(issuer);
        ArgumentException.ThrowIfNullOrWhiteSpace(commonName);

        var effectiveNotBefore = notBefore ?? new DateTimeOffset(issuer.NotBefore.ToUniversalTime());
        var effectiveNotAfter = notAfter ?? new DateTimeOffset(issuer.NotAfter.ToUniversalTime());

        var subject = $"CN={commonName}";
        if (rsaKey)
        {
            using var rsa = RSA.Create(RsaKeySizeBits);
            var rsaRequest = new CertificateRequest(subject, rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            rsaRequest.AddClusterNodeExtensions(true);
            using var issuerKey = ThrowHelper.Required(issuer.GetECDsaPrivateKey(), "An RSA certificate must be issued by an ECDSA authority.");
            var generator = X509SignatureGenerator.CreateForECDsa(issuerKey);
            using var rsaPublic = rsaRequest.Create(issuer.SubjectName, generator, effectiveNotBefore, effectiveNotAfter, Guid.NewGuid().ToByteArray());
            return rsaPublic.CopyWithPrivateKey(rsa);
        }

        using var peerKey = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var peerRequest = new CertificateRequest(subject, peerKey, HashAlgorithmName.SHA256);
        peerRequest.AddClusterNodeExtensions();
        var peerPublic = peerRequest.Create(issuer, effectiveNotBefore, effectiveNotAfter, Guid.NewGuid().ToByteArray());
        return peerPublic.CopyWithPrivateKey(peerKey);
    }

    /// <summary>Creates a standalone test certificate authority.</summary>
    /// <param name="commonName">Certificate authority distinguished name common name.</param>
    /// <returns>A self-signed test CA certificate.</returns>
    public static X509Certificate2 CreateStandaloneCertificateAuthority(string commonName = "CN=Squirix E2E Untrusted Test CA")
    {
        using var caKey = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var caRequest = new CertificateRequest(commonName, caKey, HashAlgorithmName.SHA256);
        caRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        var notBefore = DateTimeOffset.UtcNow.AddDays(-1);
        var notAfter = notBefore.AddDays(30);
        return caRequest.CreateSelfSigned(notBefore, notAfter);
    }

    /// <summary>Loads an exportable certificate copy suitable for Schannel client and server authentication.</summary>
    /// <param name="certificate">Source certificate with private key.</param>
    /// <returns>Exportable certificate copy.</returns>
    internal static X509Certificate2 LoadExportableCertificate(X509Certificate2 certificate) =>
        X509CertificateLoader.LoadPkcs12(certificate.Export(X509ContentType.Pfx), null, X509KeyStorageFlags.Exportable);

    [ThreadSafe]
    private sealed class PeerCertificateValidator
    {
        private readonly PeerChainCache _chains;
        private readonly string _expectedPeerNodeId;

        internal PeerCertificateValidator(X509Certificate2 trustAnchor, string expectedPeerNodeId)
        {
            _chains = new PeerChainCache(trustAnchor, PeerChainCache.DefaultCapacity, PeerChainCache.DefaultMaxAge);
            _expectedPeerNodeId = expectedPeerNodeId;
        }

        internal bool Validate(object? sender, X509Certificate? certificate, X509Chain? chain, SslPolicyErrors errors)
        {
            _ = sender;
            _ = chain;
            _ = errors;
            if (certificate == null)
                return false;

            if (certificate is X509Certificate2 presented)
                return IsExpectedPeer(presented);

            using var copy = new X509Certificate2(certificate);
            return IsExpectedPeer(copy);
        }

        private bool IsExpectedPeer(X509Certificate2 certificate) =>
            _chains.TryVerify(certificate, TimeProvider.System.GetUtcNow(), out var verdict) && string.Equals(verdict.NodeId, _expectedPeerNodeId, StringComparison.Ordinal);
    }
}
