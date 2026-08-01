using System;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Resolves the static ordered replica group for an original owner node.</summary>
internal interface IReplicaGroupLocator
{
    /// <summary>Gets the configured replica factor including the original owner.</summary>
    int ReplicaCount { get; }

    /// <summary>
    /// Writes the ordered replica group for <paramref name="originalOwnerNodeId" /> into <paramref name="destination" />.
    /// </summary>
    /// <param name="originalOwnerNodeId">Original owner selected by the vnode ring.</param>
    /// <param name="destination">Caller-owned destination whose length must equal <see cref="ReplicaCount" />.</param>
    void GetReplicaGroup(string originalOwnerNodeId, Span<string> destination);
}
