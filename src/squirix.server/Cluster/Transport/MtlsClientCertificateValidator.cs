using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Squirix.Server.Cluster.Transport;

/// <summary>
/// Validates inbound and outbound inter-node certificates against the cluster trust root and node identity.
/// </summary>
internal static class MtlsClientCertificateValidator
{
    /// <summary>
    /// Validates a peer certificate against the configured cluster CA.
    /// </summary>
    /// <param name="clientCertificate">The presented peer certificate.</param>
    /// <param name="trustAnchor">Configured cluster trust root.</param>
    /// <returns><see langword="true" /> when the certificate chains to the cluster trust root and is time-valid.</returns>
    public static bool Validate(X509Certificate2? clientCertificate, X509Certificate2 trustAnchor)
    {
        ArgumentNullException.ThrowIfNull(trustAnchor);

        return clientCertificate is not null && ValidateTrustChain(clientCertificate, trustAnchor);
    }

    /// <summary>
    /// Validates a peer certificate against the cluster CA and one of the configured remote peer node identifiers.
    /// </summary>
    /// <param name="peerCertificate">The presented peer certificate.</param>
    /// <param name="trustAnchor">Configured cluster trust root.</param>
    /// <param name="allowedRemotePeerNodeIds">Configured remote peer node identifiers.</param>
    /// <returns><see langword="true" /> when the certificate is trusted and bound to an allowed peer.</returns>
    public static bool ValidateForConfiguredRemotePeer(X509Certificate2? peerCertificate, X509Certificate2 trustAnchor, IReadOnlyCollection<string> allowedRemotePeerNodeIds)
    {
        ArgumentNullException.ThrowIfNull(allowedRemotePeerNodeIds);

        if (!Validate(peerCertificate, trustAnchor))
            return false;

        if (!MtlsCertificateIdentity.TryGetNodeId(peerCertificate!, out var nodeId))
            return false;

        foreach (var allowedNodeId in allowedRemotePeerNodeIds)
        {
            if (string.Equals(nodeId, allowedNodeId, StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Validates a peer certificate against the cluster CA and an expected node identifier.
    /// </summary>
    /// <param name="peerCertificate">The presented peer certificate.</param>
    /// <param name="trustAnchor">Configured cluster trust root.</param>
    /// <param name="expectedNodeId">Configured cluster node identifier for the remote peer.</param>
    /// <returns><see langword="true" /> when the certificate is trusted and bound to <paramref name="expectedNodeId" />.</returns>
    public static bool ValidateForExpectedNodeId(X509Certificate2? peerCertificate, X509Certificate2 trustAnchor, string expectedNodeId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(expectedNodeId);

        return Validate(peerCertificate, trustAnchor) && MtlsCertificateIdentity.MatchesNodeId(peerCertificate!, expectedNodeId);
    }

    private static bool ValidateTrustChain(X509Certificate2 clientCertificate, X509Certificate2 trustAnchor)
    {
        try
        {
            using var validationChain = new X509Chain();
            validationChain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
            validationChain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
            validationChain.ChainPolicy.CustomTrustStore.Clear();
            _ = validationChain.ChainPolicy.CustomTrustStore.Add(trustAnchor);
            return validationChain.Build(clientCertificate);
        }
        catch (CryptographicException)
        {
            return false;
        }
    }
}
