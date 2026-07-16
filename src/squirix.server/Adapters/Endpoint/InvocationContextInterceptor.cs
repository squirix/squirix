using System;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.AspNetCore.Http;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Adapters.Endpoint;

internal sealed class InvocationContextInterceptor : Interceptor
{
    private readonly ClusterConfig _cluster;
    private readonly MtlsCertificateMaterial _mtlsMaterial;
    private readonly MtlsOptions _mtlsOptions;
    private readonly IRemoteInvocationScopeFactory _scopeFactory;

    public InvocationContextInterceptor(IRemoteInvocationScopeFactory scopeFactory, ClusterConfig cluster, MtlsOptions mtlsOptions, MtlsCertificateMaterial mtlsMaterial)
    {
        _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
        _cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
        _mtlsOptions = mtlsOptions ?? throw new ArgumentNullException(nameof(mtlsOptions));
        _mtlsMaterial = mtlsMaterial ?? throw new ArgumentNullException(nameof(mtlsMaterial));
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

    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
    {
        using var scope = _scopeFactory.EnterRemoteInvocation(ResolveInternalOwnerInvocation(context));
        return await continuation(request, context).ConfigureAwait(false);
    }

    private bool ResolveInternalOwnerInvocation(ServerCallContext context)
    {
        SquirixClusterConnectionSecurity.RejectSpoofedInternalOwnerHeader(context, _cluster, _mtlsOptions, _mtlsMaterial);
        return SquirixClusterConnectionSecurity.IsTrustedInternalOwnerCall(context, _cluster, _mtlsOptions, _mtlsMaterial);
    }

    /// <summary>Centralizes trusted cluster-peer checks used by transport auth and inbound RPC classification.</summary>
    private static class SquirixClusterConnectionSecurity
    {
        internal static void RejectSpoofedInternalOwnerHeader(ServerCallContext context, ClusterConfig cluster, MtlsOptions mtlsOptions, MtlsCertificateMaterial mtlsMaterial)
        {
            ArgumentNullException.ThrowIfNull(context);
            ArgumentNullException.ThrowIfNull(cluster);
            ArgumentNullException.ThrowIfNull(mtlsOptions);
            ArgumentNullException.ThrowIfNull(mtlsMaterial);

            if (!IsInternalOwnerHeaderPresent(context) || IsTrustedInternalOwnerCall(context, cluster, mtlsOptions, mtlsMaterial))
                return;

            throw new RpcException(new Status(StatusCode.Unauthenticated, "Internal cluster invocation requires trusted peer mTLS."));
        }

        internal static bool IsTrustedInternalOwnerCall(ServerCallContext context, ClusterConfig cluster, MtlsOptions mtlsOptions, MtlsCertificateMaterial mtlsMaterial)
        {
            ArgumentNullException.ThrowIfNull(context);
            ArgumentNullException.ThrowIfNull(cluster);
            ArgumentNullException.ThrowIfNull(mtlsOptions);
            ArgumentNullException.ThrowIfNull(mtlsMaterial);

            if (!mtlsMaterial.Enabled || mtlsOptions.InternalListenPort <= 0)
                return false;

            var httpContext = context.GetHttpContext();
            return httpContext.Connection.LocalPort == mtlsOptions.InternalListenPort && IsInternalOwnerHeaderPresent(context) &&
                   IsTrustedClusterPeer(httpContext, cluster, mtlsMaterial);
        }

        private static bool IsInternalOwnerHeaderPresent(ServerCallContext context)
        {
            var value = context.RequestHeaders.GetValue(RemoteInvocationContract.InternalOwnerRpcHeaderName);
            return string.Equals(value, RemoteInvocationContract.InternalOwnerRpcHeaderValue, StringComparison.Ordinal);
        }

        private static bool IsTrustedClusterPeer(HttpContext httpContext, ClusterConfig cluster, MtlsCertificateMaterial mtlsMaterial)
        {
            if (!mtlsMaterial.Enabled || mtlsMaterial.TrustAnchor is null)
                return false;

            var certificate = httpContext.Connection.ClientCertificate;
            return MtlsClientCertificateValidator.ValidateForConfiguredRemotePeer(certificate, mtlsMaterial.TrustAnchor, MtlsTopology.GetRemotePeerNodeIds(cluster));
        }
    }
}
