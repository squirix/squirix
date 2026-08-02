using System;
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
        /// <exception cref="InvalidOperationException">Thrown when network replication is enabled before M8-09.</exception>
        internal IServiceCollection AddSquirixClusterReplication(TopologyOptions cluster, bool foundationOnly = false)
        {
            var physicalRing = new PhysicalNodeRing(GetPeerNodeIds(cluster));
            _ = services.AddSingleton(physicalRing);
            _ = services.AddSingleton<IReplicaGroupLocator>(new ReplicaGroupLocator(physicalRing, cluster.ReplicaCount));

            // AddSingleton<T>(T) is constrained to class; two-arg instance descriptor boxes the struct.
            // Do not pass ServiceLifetime — that binds the keyed (serviceType, serviceKey, instance) ctor.
            var featureState = foundationOnly ? FeatureState.Foundation : FeatureState.Disabled;
            if (featureState.NetworkReplicationEnabled)
                throw new InvalidOperationException("Network replication must stay disabled until M8-09 activation.");

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
