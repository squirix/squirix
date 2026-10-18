using System;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Validates inbound and outbound internode certificates against the cluster trust root and node identity.</summary>
internal static class MtlsClientCertificateValidator
{
    /// <summary>Validates a peer certificate against the cluster CA and one of the configured remote peer node identifiers.</summary>
    /// <param name="peerCertificate">The presented peer certificate.</param>
    /// <param name="trustAnchor">Configured cluster trust root.</param>
    /// <param name="nodeIds">Configured remote peer node identifiers.</param>
    /// <returns><see langword="true" /> when the certificate is trusted and bound to an allowed peer.</returns>
    internal static bool ValidateForConfiguredRemotePeer(X509Certificate2? peerCertificate, X509Certificate2 trustAnchor, ReadOnlySpan<string> nodeIds)
    {
        if (!Validate(peerCertificate, trustAnchor))
            return false;

        if (!MtlsCertificateIdentity.TryGetNodeId(peerCertificate!, out var nodeId))
            return false;

        for (var i = 0; i < nodeIds.Length; i++)
        {
            if (string.Equals(nodeId, nodeIds[i], StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Validates a peer certificate against the cluster CA at a given time and one of the configured remote peer node identifiers, and reports
    /// the window in which every certificate of its chain is valid.
    /// </summary>
    /// <param name="peerCertificate">The presented peer certificate.</param>
    /// <param name="trustAnchor">Configured cluster trust root.</param>
    /// <param name="nodeIds">Configured remote peer node identifiers.</param>
    /// <param name="verificationTime">The time the chain is verified at.</param>
    /// <param name="window">The latest start and the earliest end of validity among the certificates of the chain, when it is valid.</param>
    /// <param name="nodeId">The NodeId of the certificate, when it is trusted.</param>
    /// <returns><see langword="true" /> when the certificate is trusted at <paramref name="verificationTime" /> and bound to an allowed peer.</returns>
    internal static bool TryValidateForConfiguredRemotePeer(
        X509Certificate2 peerCertificate,
        X509Certificate2 trustAnchor,
        ReadOnlySpan<string> nodeIds,
        DateTimeOffset verificationTime,
        out (DateTimeOffset NotBefore, DateTimeOffset NotAfter) window,
        out string nodeId)
    {
        ArgumentNullException.ThrowIfNull(peerCertificate);
        ArgumentNullException.ThrowIfNull(trustAnchor);
        nodeId = string.Empty;
        if (!TryBuildTrustChain(peerCertificate, trustAnchor, verificationTime, out window) || !MtlsCertificateIdentity.TryGetNodeId(peerCertificate, out var certificateNodeId))
            return false;

        for (var i = 0; i < nodeIds.Length; i++)
        {
            if (!string.Equals(certificateNodeId, nodeIds[i], StringComparison.Ordinal))
                continue;

            nodeId = certificateNodeId;
            return true;
        }

        return false;
    }

    /// <summary>Validates a peer certificate against the cluster CA and an expected node identifier.</summary>
    /// <param name="peerCertificate">The presented peer certificate.</param>
    /// <param name="trustAnchor">Configured cluster trust root.</param>
    /// <param name="expectedNodeId">Configured cluster node identifier for the remote peer.</param>
    /// <returns><see langword="true" /> when the certificate is trusted and bound to <paramref name="expectedNodeId" />.</returns>
    internal static bool ValidateForExpectedNodeId(X509Certificate2? peerCertificate, X509Certificate2 trustAnchor, string expectedNodeId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(expectedNodeId);

        return Validate(peerCertificate, trustAnchor) && MtlsCertificateIdentity.MatchesNodeId(peerCertificate!, expectedNodeId);
    }

    /// <summary>Validates a peer certificate against the configured cluster CA.</summary>
    /// <param name="clientCertificate">The presented peer certificate.</param>
    /// <param name="trustAnchor">Configured cluster trust root.</param>
    /// <returns><see langword="true" /> when the certificate chains to the cluster trust root and is time-valid.</returns>
    private static bool Validate(X509Certificate2? clientCertificate, X509Certificate2 trustAnchor)
    {
        ArgumentNullException.ThrowIfNull(trustAnchor);
        return clientCertificate != null && ValidateTrustChain(clientCertificate, trustAnchor);
    }

    private static bool ValidateTrustChain(X509Certificate2 clientCertificate, X509Certificate2 trustAnchor) =>
        TryBuildTrustChain(clientCertificate, trustAnchor, null, out _);

    private static bool TryBuildTrustChain(
        X509Certificate2 clientCertificate,
        X509Certificate2 trustAnchor,
        DateTimeOffset? verificationTime,
        out (DateTimeOffset NotBefore, DateTimeOffset NotAfter) window)
    {
        window = default;
        try
        {
            using var validationChain = new X509Chain();
            validationChain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
            validationChain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
            if (verificationTime is { } time)
                validationChain.ChainPolicy.VerificationTime = time.UtcDateTime;

            validationChain.ChainPolicy.CustomTrustStore.Clear();
            _ = validationChain.ChainPolicy.CustomTrustStore.Add(trustAnchor);
            if (!validationChain.Build(clientCertificate))
                return false;

            var notBefore = DateTimeOffset.MinValue;
            var notAfter = DateTimeOffset.MaxValue;
            foreach (var element in validationChain.ChainElements)
            {
                var start = new DateTimeOffset(element.Certificate.NotBefore.ToUniversalTime());
                var end = new DateTimeOffset(element.Certificate.NotAfter.ToUniversalTime());
                notBefore = start > notBefore ? start : notBefore;
                notAfter = end < notAfter ? end : notAfter;
            }

            window = (notBefore, notAfter);
            return true;
        }
        catch (CryptographicException)
        {
            return false;
        }
    }
}
