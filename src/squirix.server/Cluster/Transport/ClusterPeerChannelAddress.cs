using System;
using Squirix.Server.Cluster.Membership;

namespace Squirix.Server.Cluster.Transport;

/// <summary>
/// Resolves gRPC channel addresses for inter-node cluster transport.
/// </summary>
internal static class ClusterPeerChannelAddress
{
    /// <summary>
    /// Resolves the gRPC endpoint used for inter-node cluster calls.
    /// </summary>
    /// <param name="peer">Configured cluster peer.</param>
    /// <param name="mtlsOptions">Cluster mTLS options for the local node.</param>
    /// <param name="interNodeMtlsEnabled">Whether inter-node mTLS transport is active.</param>
    /// <returns>The HTTPS gRPC address for pooled cluster clients.</returns>
    public static string Resolve(Peer peer, MtlsOptions mtlsOptions, bool interNodeMtlsEnabled)
    {
        ArgumentNullException.ThrowIfNull(peer);
        ArgumentNullException.ThrowIfNull(mtlsOptions);

        if (!interNodeMtlsEnabled)
            return peer.Url;

        if (!string.IsNullOrWhiteSpace(peer.InterNodeUrl))
            return peer.InterNodeUrl;

        if (mtlsOptions.InternalListenPort <= 0)
            throw new InvalidOperationException("Cluster mTLS internal listen port must be configured for inter-node transport.");

        return !Uri.TryCreate(peer.Url, UriKind.Absolute, out var primaryUri) ? throw new InvalidOperationException($"Cluster peer URL is invalid: '{peer.Url}'.")
            : new UriBuilder(primaryUri.Scheme, primaryUri.Host, mtlsOptions.InternalListenPort).Uri.AbsoluteUri;
    }
}
