using System;
using Squirix.Server.Cluster.Membership;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Determines when inter-node cluster mTLS is required from cluster topology.</summary>
internal static class MtlsTopology
{
    /// <summary>Returns configured remote peer node identifiers for inbound inter-node certificate checks.</summary>
    /// <param name="cluster">Cluster topology configuration.</param>
    /// <returns>Remote peer node identifiers excluding the local node.</returns>
    internal static string[] GetRemotePeerNodeIds(ClusterConfig cluster)
    {
        ArgumentNullException.ThrowIfNull(cluster);

        var peers = cluster.Peers;
        var remotePeerNodeIds = new string[peers.Length];
        var writeIndex = 0;

        for (var i = 0; i < peers.Length; i++)
        {
            if (!string.Equals(peers[i].NodeId, cluster.NodeId, StringComparison.Ordinal))
                remotePeerNodeIds[writeIndex++] = peers[i].NodeId;
        }

        if (writeIndex is 0)
            return [];

        if (writeIndex != remotePeerNodeIds.Length)
            Array.Resize(ref remotePeerNodeIds, writeIndex);

        return remotePeerNodeIds;
    }

    /// <summary>Returns whether the configured topology performs inter-node traffic that requires mTLS.</summary>
    /// <param name="cluster">Cluster topology configuration.</param>
    /// <returns><see langword="true" /> when at least one remote peer is configured.</returns>
    internal static bool RequiresInterNodeMtls(ClusterConfig cluster)
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
