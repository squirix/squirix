using System;
using System.Net.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Squirix.Server.Node.Observability;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Transport-owned DI registrations for inter-node gRPC client pooling.</summary>
internal static class ServiceRegistration
{
    extension(IServiceCollection services)
    {
        /// <summary>Registers inter-node client pool and ownership interceptors.</summary>
        /// <param name="cluster">Cluster topology configuration.</param>
        /// <param name="callPolicyFactory">Optional per-endpoint call policy factory.</param>
        /// <param name="peerHandlerFactory">Optional per-peer HTTP handler factory.</param>
        /// <returns><paramref name="services" /> for chaining.</returns>
        internal IServiceCollection AddSquirixClusterTransport(
            TopologyOptions cluster,
            Func<string, ServerCallPolicy>? callPolicyFactory,
            Func<string, HttpMessageHandler>? peerHandlerFactory)
        {
            _ = services.AddSingleton(sp => new ClientInterceptor(sp.GetRequiredService<ILogger<ClientInterceptor>>(), cluster.NodeId));
            _ = services.AddSingleton(sp => new ServerInterceptor(sp.GetRequiredService<ILogger<ServerInterceptor>>(), cluster.NodeId));
            _ = services.AddSingleton<InternalOwnerClientInterceptor>();

            _ = services.AddSingleton<IServerClientPool>(sp =>
            {
                var material = sp.GetRequiredService<MtlsCertificateMaterial>();
                var mtlsOptions = sp.GetRequiredService<MtlsOptions>();
                var interNodeMtlsEnabled = material.Enabled;
                return new ServerClientPool(
                    CopyPeers(cluster),
                    new ServerClientPoolArgs
                    {
                        PolicyFactory = callPolicyFactory ?? (_ => new ServerCallPolicy(
                            sp.GetRequiredService<ServerCallPolicyInstrumentation>(),
                            timeouts: new CallPolicyTimeouts(TimeSpan.FromSeconds(3), TimeSpan.FromMilliseconds(60), TimeSpan.FromMilliseconds(600)))),
                        PeerHandlerFactory = peerHandlerFactory,
                        Logger = sp.GetService<ILogger<ServerClientPool>>(),
                        Interceptor = sp.GetRequiredService<ClientInterceptor>(),
                        MtlsOptions = mtlsOptions,
                        MtlsMaterial = material,
                        InterNodeMtlsEnabled = interNodeMtlsEnabled,
                        InternalOwnerInterceptor = interNodeMtlsEnabled ? sp.GetRequiredService<InternalOwnerClientInterceptor>() : null,
                    },
                    sp.GetRequiredService<ServerClientPoolMetrics>());
            });

            return services;
        }
    }

    private static ServerPeer[] CopyPeers(TopologyOptions cluster)
    {
        var peers = cluster.Peers;
        var copy = new ServerPeer[peers.Length];

        for (var i = 0; i < peers.Length; i++)
            copy[i] = peers[i];

        return copy;
    }
}
