using System;
using System.Security.Cryptography.X509Certificates;

namespace Squirix.Server.Cluster.Transport;

/// <summary>
/// Reads cluster node identity from inter-node mTLS certificates.
/// </summary>
internal static class MtlsCertificateIdentity
{
    /// <summary>
    /// Returns whether the certificate common name matches the expected cluster node identifier.
    /// </summary>
    /// <param name="certificate">Peer or node certificate.</param>
    /// <param name="expectedNodeId">Configured cluster node identifier.</param>
    /// <returns><see langword="true" /> when identities match using ordinal comparison.</returns>
    public static bool MatchesNodeId(X509Certificate2 certificate, string expectedNodeId)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        ArgumentException.ThrowIfNullOrWhiteSpace(expectedNodeId);

        return TryGetNodeId(certificate, out var nodeId) && string.Equals(nodeId, expectedNodeId, StringComparison.Ordinal);
    }

    /// <summary>
    /// Reads the cluster <see cref="Membership.Peer.NodeId" /> from the certificate common name.
    /// </summary>
    /// <param name="certificate">Peer or node certificate.</param>
    /// <param name="nodeId">Parsed node identifier when present.</param>
    /// <returns><see langword="true" /> when the certificate exposes a non-empty common name.</returns>
    public static bool TryGetNodeId(X509Certificate2 certificate, out string nodeId)
    {
        ArgumentNullException.ThrowIfNull(certificate);

        nodeId = certificate.GetNameInfo(X509NameType.SimpleName, false);
        return !string.IsNullOrWhiteSpace(nodeId);
    }
}
