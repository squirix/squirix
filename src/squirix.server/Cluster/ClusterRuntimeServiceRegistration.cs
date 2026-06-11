using System;
using System.Net.Http;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Reliability;
using Squirix.Server.Cluster.Routing;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Cluster;

/// <summary>
/// Cluster-owned DI registrations for static topology transport and the hosted <see cref="ClusteredCache{T}" /> singleton.
/// </summary>
/// <remarks>
///     <para>
///     Keeps <see cref="SquirixCachePipelineRegistration" /> free of concrete <see cref="ClusteredCache{T}" />
///     construction while preserving registration order: call <see cref="AddClusteredCacheSingleton" /> from the cache pipeline
///     after <see cref="OwnershipGuardCacheDecorator{T}" /> and before <see cref="MemoryAdmissionCacheDecorator{T}" />.
///     </para>
/// </remarks>
internal static class ClusterRuntimeServiceRegistration
{
    /// <summary>
    /// Extension methods that register cluster runtime services on <see cref="IServiceCollection" />.
    /// </summary>
    /// <param name="services">The service collection.</param>
    extension(IServiceCollection services)
    {
        /// <summary>
        /// Registers the hosted object-cache <see cref="ClusteredCache{T}" /> singleton between ownership guard and memory admission.
        /// </summary>
        /// <returns><paramref name="services" /> for chaining.</returns>
        public IServiceCollection AddClusteredCacheSingleton()
        {
            _ = services.AddSingleton<ClusteredCache<object?>>(static sp => new ClusteredCache<object?>(
                sp.GetRequiredService<ClusterConfig>().NodeId,
                sp.GetRequiredService<OwnershipGuardCacheDecorator<object?>>(),
                sp.GetRequiredService<INodeLocator>(),
                sp.GetRequiredService<IClientPool>()));
            return services;
        }

        /// <summary>
        /// Registers static topology node location, gRPC client pool, and shared cluster-side singletons used by the node host.
        /// </summary>
        /// <param name="cluster">Cluster topology configuration.</param>
        /// <param name="callPolicyFactory">Optional per-endpoint call policy factory; defaults to a conservative remote policy.</param>
        /// <param name="httpHandlerOverride">Optional HTTP handler override for pooled gRPC channels.</param>
        /// <returns><paramref name="services" /> for chaining.</returns>
        public IServiceCollection AddSquirixClusterServices(ClusterConfig cluster, Func<string, CallPolicy>? callPolicyFactory, HttpMessageHandler? httpHandlerOverride)
        {
            _ = services.AddSingleton(new ConsistentHashNodeLocator(GetPeerNodeIds(cluster), cluster.VirtualNodes));
            _ = services.AddSingleton<INodeLocator>(static sp => sp.GetRequiredService<ConsistentHashNodeLocator>());
            _ = services.AddSingleton<INodeOwnershipResolver, NodeOwnershipResolver>();
            _ = services.AddSingleton<Correlation.ClientInterceptor>();
            _ = services.AddSingleton<Correlation.ServerInterceptor>();
            _ = services.AddSingleton<IdempotencyStore>();
            _ = services.AddSingleton<IClientPool>(sp =>
            {
                var material = sp.GetRequiredService<ClusterMtlsCertificateMaterial>();
                var handler = httpHandlerOverride ?? (material.Enabled ? GrpcTransportEndpoints.CreateClusterMtlsHandler(material) : null);
                return new ClientPool(
                    cluster.Peers,
                    callPolicyFactory ?? (static _ => new CallPolicy(TimeSpan.FromSeconds(3), 3, TimeSpan.FromMilliseconds(60), TimeSpan.FromMilliseconds(600))),
                    handler,
                    sp.GetRequiredService<Correlation.ClientInterceptor>());
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
        }
    }
}
