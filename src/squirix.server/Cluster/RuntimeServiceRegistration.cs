using System;
using System.Net.Http;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Reliability;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Cluster;

/// <summary>Cluster-owned DI registrations for static topology transport and inter-node client pooling.</summary>
internal static class RuntimeServiceRegistration
{
    /// <summary>
    /// Extension methods that register cluster runtime services on <see cref="IServiceCollection" />.
    /// </summary>
    extension(IServiceCollection services)
    {
        /// <summary>Registers static topology node location, gRPC client pool, and shared cluster-side singletons used by the node host.</summary>
        /// <param name="cluster">Cluster topology configuration.</param>
        /// <param name="callPolicyFactory">Optional per-endpoint call policy factory; defaults to a conservative remote policy.</param>
        /// <param name="peerHandlerFactory">Optional per-peer HTTP handler factory for pooled gRPC channels.</param>
        /// <returns><paramref name="services" /> for chaining.</returns>
        internal IServiceCollection AddSquirixClusterServices(
            ClusterConfig cluster,
            Func<string, ServerCallPolicy>? callPolicyFactory,
            Func<string, HttpMessageHandler>? peerHandlerFactory)
        {
            _ = services.AddSingleton(new ConsistentHashNodeLocator(GetPeerNodeIds(cluster), cluster.VirtualNodes));
            _ = services.AddSingleton<INodeLocator>(static sp => sp.GetRequiredService<ConsistentHashNodeLocator>());
            _ = services.AddSingleton<INodeOwnershipResolver, NodeOwnershipResolver>();
            _ = services.AddSingleton<Correlation.ClientInterceptor>();
            _ = services.AddSingleton<Correlation.ServerInterceptor>();
            _ = services.AddSingleton<ClusterInternalOwnerClientInterceptor>();
            _ = services.AddSingleton<IServerClientPool>(sp =>
            {
                var material = sp.GetRequiredService<MtlsCertificateMaterial>();
                var mtlsOptions = sp.GetRequiredService<MtlsOptions>();
                var interNodeMtlsEnabled = material.Enabled;
                return new ServerClientPool(
                    CopyPeers(cluster),
                    new ServerClientPoolArgs
                    {
                        PolicyFactory = callPolicyFactory ?? (static _ => new ServerCallPolicy(TimeSpan.FromSeconds(3), 3, TimeSpan.FromMilliseconds(60), TimeSpan.FromMilliseconds(600))),
                        PeerHandlerFactory = peerHandlerFactory,
                        Interceptor = sp.GetRequiredService<Correlation.ClientInterceptor>(),
                        MtlsOptions = mtlsOptions,
                        MtlsMaterial = material,
                        InterNodeMtlsEnabled = interNodeMtlsEnabled,
                        InternalOwnerInterceptor = interNodeMtlsEnabled ? sp.GetRequiredService<ClusterInternalOwnerClientInterceptor>() : null,
                    });
            });

            return services;

            static string[] GetPeerNodeIds(ClusterConfig cluster)
            {
                var peers = cluster.Peers;
                var nodeIds = new string[peers.Length];

                for (var i = 0; i < peers.Length; i++)
                    nodeIds[i] = peers[i].NodeId;

                return nodeIds;
            }

            static ServerPeer[] CopyPeers(ClusterConfig cluster)
            {
                var peers = cluster.Peers;
                var copy = new ServerPeer[peers.Length];

                for (var i = 0; i < peers.Length; i++)
                    copy[i] = peers[i];

                return copy;
            }
        }
    }
}
