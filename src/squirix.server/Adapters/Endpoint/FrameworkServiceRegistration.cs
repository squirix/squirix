using System;
using System.Threading.Tasks;
using Grpc.AspNetCore.Server;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Invocation;

namespace Squirix.Server.Adapters.Endpoint;

internal static class FrameworkServiceRegistration
{
    internal static IServiceCollection AddSquirixFrameworkServices(this IServiceCollection services, bool enableDetailedGrpcErrors, Action<GrpcServiceOptions>? configureGrpc)
    {
        _ = services.AddGrpc(o =>
        {
            o.EnableDetailedErrors = enableDetailedGrpcErrors;
            o.MaxReceiveMessageSize = EntryLimits.GrpcMaxReceiveMessageSizeBytes;
            o.MaxSendMessageSize = EntryLimits.GrpcMaxSendMessageSizeBytes;
            o.Interceptors.Add<ResourceExhaustedExceptionInterceptor>();
            o.Interceptors.Add<InvocationContextInterceptor>();
            configureGrpc?.Invoke(o);
        });
        _ = services.AddHealthChecks();
        _ = services.ConfigureHttpJsonOptions(static o => o.SerializerOptions.PropertyNameCaseInsensitive = true);
        _ = services.AddSingleton(static sp => new InvocationContextInterceptor(
            sp.GetRequiredService<IRemoteInvocationScopeFactory>(),
            sp.GetRequiredService<TopologyOptions>(),
            sp.GetRequiredService<MtlsOptions>(),
            sp.GetRequiredService<MtlsCertificate>()));
        _ = services.AddSingleton<ResourceExhaustedExceptionInterceptor>();

        return services;
    }

    private sealed class InvocationContextInterceptor : Interceptor
    {
        private readonly TopologyOptions _cluster;
        private readonly MtlsCertificate _mtls;
        private readonly MtlsOptions _mtlsOptions;
        private readonly IRemoteInvocationScopeFactory _scopeFactory;

        internal InvocationContextInterceptor(IRemoteInvocationScopeFactory scopeFactory, TopologyOptions cluster, MtlsOptions mtlsOptions, MtlsCertificate mtls)
        {
            ArgumentNullException.ThrowIfNull(scopeFactory);
            ArgumentNullException.ThrowIfNull(cluster);
            ArgumentNullException.ThrowIfNull(mtlsOptions);
            ArgumentNullException.ThrowIfNull(mtls);
            _scopeFactory = scopeFactory;
            _cluster = cluster;
            _mtlsOptions = mtlsOptions;
            _mtls = mtls;
        }

        public override async Task ServerStreamingServerHandler<TRequest, TResponse>(
            TRequest request,
            IServerStreamWriter<TResponse> responseStream,
            ServerCallContext context,
            ServerStreamingServerMethod<TRequest, TResponse> continuation)
        {
            using var scope = _scopeFactory.EnterRemoteInvocation(ResolveInternalOwnerInvocation(context));
            await continuation(request, responseStream, context).ConfigureAwait(false);
        }

        public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
            TRequest request,
            ServerCallContext context,
            UnaryServerMethod<TRequest, TResponse> continuation)
        {
            using var scope = _scopeFactory.EnterRemoteInvocation(ResolveInternalOwnerInvocation(context));
            return await continuation(request, context).ConfigureAwait(false);
        }

        private bool ResolveInternalOwnerInvocation(ServerCallContext context)
        {
            SquirixClusterConnectionSecurity.RejectSpoofedInternalOwnerHeader(context, _cluster, _mtlsOptions, _mtls);
            return SquirixClusterConnectionSecurity.IsTrustedInternalOwnerCall(context, _cluster, _mtlsOptions, _mtls);
        }

        /// <summary>Centralizes trusted cluster-peer checks used by transport auth and inbound RPC classification.</summary>
        private static class SquirixClusterConnectionSecurity
        {
            internal static bool IsTrustedInternalOwnerCall(ServerCallContext context, TopologyOptions cluster, MtlsOptions mtlsOptions, MtlsCertificate mtls)
            {
                ArgumentNullException.ThrowIfNull(context);
                ArgumentNullException.ThrowIfNull(cluster);
                ArgumentNullException.ThrowIfNull(mtlsOptions);
                ArgumentNullException.ThrowIfNull(mtls);

                if (!mtls.Enabled || mtlsOptions.InternalListenPort <= 0)
                    return false;

                var httpContext = context.GetHttpContext();
                return httpContext.Connection.LocalPort == mtlsOptions.InternalListenPort && IsInternalOwnerHeaderPresent(context) &&
                       IsTrustedClusterPeer(httpContext, cluster, mtls);
            }

            internal static void RejectSpoofedInternalOwnerHeader(ServerCallContext context, TopologyOptions cluster, MtlsOptions mtlsOptions, MtlsCertificate mtls)
            {
                ArgumentNullException.ThrowIfNull(context);
                ArgumentNullException.ThrowIfNull(cluster);
                ArgumentNullException.ThrowIfNull(mtlsOptions);
                ArgumentNullException.ThrowIfNull(mtls);

                if (!IsInternalOwnerHeaderPresent(context) || IsTrustedInternalOwnerCall(context, cluster, mtlsOptions, mtls))
                    return;

                throw new RpcException(new Status(StatusCode.Unauthenticated, "Internal cluster invocation requires trusted peer mTLS."));
            }

            private static bool IsInternalOwnerHeaderPresent(ServerCallContext context)
            {
                var value = context.RequestHeaders.GetValue(RemoteInvocationContract.InternalOwnerRpcHeaderName);
                return string.Equals(value, RemoteInvocationContract.InternalOwnerRpcHeaderValue, StringComparison.Ordinal);
            }

            private static bool IsTrustedClusterPeer(HttpContext httpContext, TopologyOptions cluster, MtlsCertificate mtls)
            {
                if (!mtls.Enabled || mtls.TrustAnchor == null)
                    return false;

                var certificate = httpContext.Connection.ClientCertificate;
                return MtlsClientCertificateValidator.ValidateForConfiguredRemotePeer(certificate, mtls.TrustAnchor, MtlsTopology.GetRemotePeerNodeIds(cluster));
            }
        }
    }
}
