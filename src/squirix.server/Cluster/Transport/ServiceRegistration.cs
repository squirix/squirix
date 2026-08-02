using System;
using System.Net.Http;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Invocation;

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
                        PolicyFactory = callPolicyFactory ?? (static _ => new ServerCallPolicy(
                            TimeSpan.FromSeconds(3),
                            3,
                            TimeSpan.FromMilliseconds(60),
                            TimeSpan.FromMilliseconds(600))),
                        PeerHandlerFactory = peerHandlerFactory,
                        Interceptor = sp.GetRequiredService<ClientInterceptor>(),
                        MtlsOptions = mtlsOptions,
                        MtlsMaterial = material,
                        InterNodeMtlsEnabled = interNodeMtlsEnabled,
                        InternalOwnerInterceptor = interNodeMtlsEnabled ? sp.GetRequiredService<InternalOwnerClientInterceptor>() : null,
                    });
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

    /// <summary>Marks outbound cluster owner-routing gRPC calls for trusted inter-node authentication.</summary>
    private sealed class InternalOwnerClientInterceptor : Interceptor
    {
        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
            TRequest request,
            ClientInterceptorContext<TRequest, TResponse> context,
            AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
        {
            var callOptions = AttachInternalOwnerHeader(context.Options);
            var updatedContext = new ClientInterceptorContext<TRequest, TResponse>(context.Method, context.Host, callOptions);
            return base.AsyncUnaryCall(request, updatedContext, continuation);
        }

        private static CallOptions AttachInternalOwnerHeader(CallOptions options)
        {
            // Prefer mutating caller headers. A fresh Metadata is only allocated when the call had none.
            var metadata = options.Headers ?? [];
            Upsert(metadata, RemoteInvocationContract.InternalOwnerRpcHeaderName, RemoteInvocationContract.InternalOwnerRpcHeaderValue);
            return new CallOptions(metadata, options.Deadline, options.CancellationToken, options.WriteOptions, options.PropagationToken, options.Credentials);
        }

        private static void Upsert(Metadata metadata, string key, string value)
        {
            for (var i = 0; i < metadata.Count; i++)
            {
                if (!string.Equals(metadata[i].Key, key, StringComparison.Ordinal))
                    continue;

                metadata.RemoveAt(i);
                break;
            }

            metadata.Add(key, value);
        }
    }
}
