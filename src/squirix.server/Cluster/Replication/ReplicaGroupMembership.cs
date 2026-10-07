using System;
using System.Collections.Generic;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Resolves which replica groups a node is a member of.</summary>
internal static class ReplicaGroupMembership
{
    /// <summary>Gets the owners whose replica group contains <paramref name="nodeId" />.</summary>
    /// <param name="locator">Replica group locator over the ring.</param>
    /// <param name="owners">Candidate original owners, in the order the result keeps.</param>
    /// <param name="nodeId">The node whose memberships are resolved.</param>
    /// <returns>The owners whose replica group includes the node, in <paramref name="owners" /> order.</returns>
    internal static string[] GroupsServedBy(IReplicaGroupLocator locator, IReadOnlyList<string> owners, string nodeId)
    {
        ArgumentNullException.ThrowIfNull(locator);
        ArgumentNullException.ThrowIfNull(owners);
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);

        var group = new string[locator.ReplicaCount];
        var served = new List<string>(owners.Count);
        for (var i = 0; i < owners.Count; i++)
        {
            locator.GetReplicaGroup(owners[i], group);
            for (var r = 0; r < group.Length; r++)
            {
                if (!string.Equals(group[r], nodeId, StringComparison.Ordinal))
                    continue;

                served.Add(owners[i]);
                break;
            }
        }

        return [.. served];
    }
}
