using System;
using System.Collections.Generic;
using System.Text;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>
/// Refuses node ids that cannot name a replica group directory. A replica group is identified by its original owner's
/// node id, and <see cref="GroupStoragePaths" /> encodes that id as a hexadecimal directory name.
/// </summary>
internal static class ReplicaGroupIdLimit
{
    /// <summary>The failure reported for a local node id over <see cref="GroupStoragePaths.MaxGroupIdUtf8Bytes" /> bytes.</summary>
    internal static readonly string NodeIdTooLong = $"NodeId cannot exceed {GroupStoragePaths.MaxGroupIdUtf8Bytes} UTF-8 bytes when ReplicaCount is greater than one: it names a replica group directory.";

    /// <summary>The failure reported for a peer node id over <see cref="GroupStoragePaths.MaxGroupIdUtf8Bytes" /> bytes.</summary>
    internal static readonly string PeersNodeIdTooLong = $"Peers[].NodeId cannot exceed {GroupStoragePaths.MaxGroupIdUtf8Bytes} UTF-8 bytes when ReplicaCount is greater than one: it names a replica group directory.";

    /// <summary>Appends a failure for each node id that is too long to name a replica group directory; nothing is appended without replication.</summary>
    /// <param name="failures">Caller-owned failure list.</param>
    /// <param name="topology">The topology whose local and peer node ids are checked.</param>
    internal static void CollectFailures(List<string> failures, TopologyOptions topology)
    {
        ArgumentNullException.ThrowIfNull(failures);
        ArgumentNullException.ThrowIfNull(topology);

        if (topology.ReplicaCount <= 1)
            return;

        var limit = GroupStoragePaths.MaxGroupIdUtf8Bytes;
        if (topology.NodeId is { } nodeId && Encoding.UTF8.GetByteCount(nodeId) > limit)
            failures.Add(NodeIdTooLong);

        for (var i = 0; i < topology.Peers.Count; i++)
        {
            if (topology.Peers[i].NodeId is not { } peerNodeId || Encoding.UTF8.GetByteCount(peerNodeId) <= limit)
                continue;

            failures.Add(PeersNodeIdTooLong);
            return;
        }
    }
}
