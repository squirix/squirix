using System;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Adapters.Endpoint.Grpc;

internal sealed class GrpcInvocationContextInterceptor : Interceptor
{
    private readonly ClusterConfig _cluster;
    private readonly MtlsCertificateMaterial _mtlsMaterial;
    private readonly MtlsOptions _mtlsOptions;
    private readonly IRemoteInvocationScopeFactory _scopeFactory;

    public GrpcInvocationContextInterceptor(IRemoteInvocationScopeFactory scopeFactory, ClusterConfig cluster, MtlsOptions mtlsOptions, MtlsCertificateMaterial mtlsMaterial)
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
}
