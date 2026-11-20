using Squirix.Server.Composition;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Replication-owned component registrations for physical replica placement and topology identity.</summary>
internal static class ServiceRegistration
{
    extension(NodeServices services)
    {
        /// <summary>Registers the replica locator and feature state.</summary>
        /// <param name="cluster">Cluster topology configuration.</param>
        /// <param name="foundationOnly">When <see langword="true" />, maps the closed replication service for transport tests without enabling RF&gt;1 mutations.</param>
        /// <returns><paramref name="services" /> for chaining.</returns>
        internal NodeServices AddSquirixClusterReplication(TopologyOptions cluster, bool foundationOnly = false)
        {
            var physicalRing = new PhysicalNodeRing(GetPeerNodeIds(cluster));
            _ = services.Use<IReplicaGroupLocator>(new ReplicaGroupLocator(physicalRing, cluster.ReplicaCount));

            // Reaching registration with RF>1 means the activation guard already accepted the persistence
            // and mTLS prerequisites, so networking is activated; otherwise the host stays disabled.
            _ = services.Use(FeatureState.Resolve(cluster, foundationOnly));
            return services;
        }
    }

    private static string[] GetPeerNodeIds(TopologyOptions cluster)
    {
        var peers = cluster.Peers;
        var nodeIds = new string[peers.Count];

        for (var i = 0; i < peers.Count; i++)
            nodeIds[i] = peers[i].NodeId;

        return nodeIds;
    }
}
