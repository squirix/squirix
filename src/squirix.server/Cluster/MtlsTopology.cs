using System;

namespace Squirix.Server.Cluster;

/// <summary>Determines when internode cluster mTLS is required from cluster topology.</summary>
internal static class MtlsTopology
{
    /// <summary>Returns configured remote peer node identifiers for inbound internode certificate checks.</summary>
    /// <param name="cluster">Cluster topology configuration.</param>
    /// <returns>Remote peer node identifiers excluding the local node.</returns>
    internal static string[] GetRemotePeerNodeIds(TopologyOptions cluster)
    {
        ArgumentNullException.ThrowIfNull(cluster);

        var peers = cluster.Peers;
        var remotePeerNodeIds = new string[peers.Count];
        var writeIndex = 0;

        for (var i = 0; i < peers.Count; i++)
        {
            if (!string.Equals(peers[i].NodeId, cluster.NodeId, StringComparison.Ordinal))
                remotePeerNodeIds[writeIndex++] = peers[i].NodeId;
        }

        if (writeIndex == 0)
            return [];

        if (writeIndex == remotePeerNodeIds.Length)
            return remotePeerNodeIds;

        var trimmed = new string[writeIndex];
        remotePeerNodeIds.AsSpan(0, writeIndex).CopyTo(trimmed);
        return trimmed;
    }

    /// <summary>Returns whether the configured topology performs internode traffic that requires mTLS.</summary>
    /// <param name="cluster">Cluster topology configuration.</param>
    /// <returns><see langword="true" /> when at least one remote peer is configured.</returns>
    internal static bool RequiresInterNodeMtls(TopologyOptions cluster)
    {
        ArgumentNullException.ThrowIfNull(cluster);

        var peers = cluster.Peers;
        for (var i = 0; i < peers.Count; i++)
        {
            if (!string.Equals(peers[i].NodeId, cluster.NodeId, StringComparison.Ordinal))
                return true;
        }

        return false;
    }
}
