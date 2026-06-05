using System;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Adapters.Endpoint.Grpc;

internal sealed class GrpcInvocationContextInterceptor : Interceptor
{
    private readonly IRemoteInvocationScopeFactory _scopeFactory;

    public GrpcInvocationContextInterceptor(IRemoteInvocationScopeFactory scopeFactory)
    {
        _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
    }

    public override async Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        using var scope = _scopeFactory.EnterRemoteInvocation(IsInternalOwnerRpc(context));
        await continuation(request, responseStream, context).ConfigureAwait(false);
    }

    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
    {
        using var scope = _scopeFactory.EnterRemoteInvocation(IsInternalOwnerRpc(context));
        return await continuation(request, context).ConfigureAwait(false);
    }

    private static bool IsInternalOwnerRpc(ServerCallContext context)
    {
        foreach (var header in context.RequestHeaders)
        {
            if (string.Equals(header.Key, RemoteInvocationContract.InternalOwnerRpcHeaderName, StringComparison.OrdinalIgnoreCase) && string.Equals(
                    header.Value,
                    RemoteInvocationContract.InternalOwnerRpcHeaderValue,
                    StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
