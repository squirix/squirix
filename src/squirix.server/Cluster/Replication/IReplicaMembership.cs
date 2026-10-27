namespace Squirix.Server.Cluster.Replication;

/// <summary>Tells which nodes belong to the replica set of each group this node serves.</summary>
internal interface IReplicaMembership
{
    /// <summary>Tells whether a node is a member of the replica set of a served group.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="nodeId">The node to check.</param>
    /// <returns><see langword="true" /> when this node serves the group and the node belongs to its replica set.</returns>
    bool IsMember(string groupId, string nodeId);
}
