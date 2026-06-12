using System;
using Squirix.Server.Cluster.Membership;

namespace Squirix.Server.Cluster.Transport;

/// <summary>
/// Determines when inter-node cluster mTLS is required from cluster topology.
/// </summary>
internal static class MtlsTopology
{
    /// <summary>
    /// Returns whether the configured topology performs inter-node traffic that requires mTLS.
    /// </summary>
    /// <param name="cluster">Cluster topology configuration.</param>
    /// <returns><see langword="true" /> when at least one remote peer is configured.</returns>
    public static bool RequiresInterNodeMtls(ClusterConfig cluster)
    {
        ArgumentNullException.ThrowIfNull(cluster);

        var peers = cluster.Peers;
        for (var i = 0; i < peers.Length; i++)
        {
            if (!string.Equals(peers[i].NodeId, cluster.NodeId, StringComparison.Ordinal))
                return true;
        }

        return false;
    }
}
