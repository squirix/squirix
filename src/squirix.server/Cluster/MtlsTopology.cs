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

    /// <summary>Returns whether a certificate node identifier is one of the configured remote peers.</summary>
    /// <param name="remotePeerNodeIds">Configured remote peer node identifiers.</param>
    /// <param name="nodeId">The node identifier read from the peer certificate.</param>
    /// <returns><see langword="true" /> when <paramref name="nodeId" /> equals one of <paramref name="remotePeerNodeIds" /> by ordinal comparison.</returns>
    internal static bool IsRemotePeer(string[] remotePeerNodeIds, string nodeId)
    {
        ArgumentNullException.ThrowIfNull(remotePeerNodeIds);

        for (var i = 0; i < remotePeerNodeIds.Length; i++)
        {
            if (string.Equals(nodeId, remotePeerNodeIds[i], StringComparison.Ordinal))
                return true;
        }

        return false;
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
