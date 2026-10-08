using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Default <see cref="IReplicaMembership" />: the static replica sets of the served groups, resolved once from the ring.</summary>
[Immutable]
internal sealed class ReplicaMembership : IReplicaMembership
{
    private readonly FrozenDictionary<string, string[]> _members;

    /// <summary>Initializes a new instance of the <see cref="ReplicaMembership" /> class.</summary>
    /// <param name="locator">Replica group locator over the ring.</param>
    /// <param name="groupIds">The groups this node serves; any other group has no members here.</param>
    internal ReplicaMembership(IReplicaGroupLocator locator, IReadOnlyList<string> groupIds)
    {
        ArgumentNullException.ThrowIfNull(locator);
        ArgumentNullException.ThrowIfNull(groupIds);

        var members = new Dictionary<string, string[]>(groupIds.Count, StringComparer.Ordinal);
        for (var i = 0; i < groupIds.Count; i++)
        {
            var replicaSet = new string[locator.ReplicaCount];
            locator.GetReplicaGroup(groupIds[i], replicaSet);
            members[groupIds[i]] = replicaSet;
        }

        _members = members.ToFrozenDictionary(StringComparer.Ordinal);
    }

    /// <inheritdoc />
    public bool IsMember(string groupId, string nodeId)
    {
        if (!_members.TryGetValue(groupId, out var replicaSet))
            return false;

        for (var i = 0; i < replicaSet.Length; i++)
        {
            if (string.Equals(replicaSet[i], nodeId, StringComparison.Ordinal))
                return true;
        }

        return false;
    }
}
