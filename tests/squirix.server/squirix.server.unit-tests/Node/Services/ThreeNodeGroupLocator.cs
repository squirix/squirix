using System;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Places node-a, node-b and node-c in slots 0, 1 and 2 of every group, as many of them as the group has slots.</summary>
[Immutable]
internal sealed class ThreeNodeGroupLocator : IReplicaGroupLocator
{
    public int ReplicaCount => 3;

    public void GetReplicaGroup(string originalOwnerNodeId, Span<string> destination)
    {
        string[] members = ["node-a", "node-b", "node-c"];
        for (var i = 0; i < destination.Length; i++)
            destination[i] = members[i];
    }
}
