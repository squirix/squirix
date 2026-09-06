using Microsoft.Extensions.DependencyInjection;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Replication-owned DI registrations for physical replica placement and topology identity.</summary>
internal static class ServiceRegistration
{
    extension(IServiceCollection services)
    {
        /// <summary>Registers physical replica ring, replica locator, feature state, and topology fingerprint.</summary>
        /// <param name="cluster">Cluster topology configuration.</param>
        /// <param name="foundationOnly">
        /// When <see langword="true" />, maps the closed replication service for transport tests without enabling RF&gt;1 mutations.
        /// </param>
        /// <returns><paramref name="services" /> for chaining.</returns>
        internal IServiceCollection AddSquirixClusterReplication(TopologyOptions cluster, bool foundationOnly = false)
        {
            var physicalRing = new PhysicalNodeRing(GetPeerNodeIds(cluster));
            _ = services.AddSingleton(physicalRing);
            _ = services.AddSingleton<IReplicaGroupLocator>(new ReplicaGroupLocator(physicalRing, cluster.ReplicaCount));

            // AddSingleton<T>(T) is constrained to class; two-arg instance descriptor boxes the struct.
            // Do not pass ServiceLifetime — that binds the keyed (serviceType, serviceKey, instance) ctor.
            // Reaching registration with RF>1 means the activation guard already accepted the persistence
            // and mTLS prerequisites, so networking is activated; otherwise the host stays disabled.
            FeatureState featureState;
            if (foundationOnly)
                featureState = FeatureState.Foundation;
            else if (cluster.ReplicaCount > 1)
                featureState = FeatureState.Activated;
            else
                featureState = FeatureState.Disabled;

            services.Add(new ServiceDescriptor(typeof(FeatureState), featureState));
            _ = services.AddSingleton(sp => TopologyFingerprint.CreateFromTopology(cluster, sp.GetRequiredService<MtlsOptions>()));
            return services;
        }
    }

    private static string[] GetPeerNodeIds(TopologyOptions cluster)
    {
        var peers = cluster.Peers;
        var nodeIds = new string[peers.Length];

        for (var i = 0; i < peers.Length; i++)
            nodeIds[i] = peers[i].NodeId;

        return nodeIds;
    }
}
