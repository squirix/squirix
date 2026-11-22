using System;
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

    internal static SslClientAuthenticationOptions CreatePeerTlsOptions(X509CertificateCollection? clientCertificates, X509Certificate2 trustAnchor, string expectedPeerNodeId)
    {
        ArgumentNullException.ThrowIfNull(trustAnchor);
        ArgumentException.ThrowIfNullOrWhiteSpace(expectedPeerNodeId);

        var validator = new PeerCertificateValidator(trustAnchor, expectedPeerNodeId);

        // Match the product's offline client certificate context on Unix and Schannel client authentication on Windows.
        var clientCertificateContext = !OperatingSystem.IsWindows() && clientCertificates is [X509Certificate2 first, ..]
            ? SslStreamCertificateContext.Create(first, null, true)
            : null;
        return new SslClientAuthenticationOptions
        {
            ClientCertificates = clientCertificates,
            ClientCertificateContext = clientCertificateContext,
            RemoteCertificateValidationCallback = validator.Validate,
        };
    }

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
