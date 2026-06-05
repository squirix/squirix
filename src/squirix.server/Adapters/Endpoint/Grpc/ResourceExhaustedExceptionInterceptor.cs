using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Squirix.Server.Errors;

namespace Squirix.Server.Adapters.Endpoint.Grpc;

internal sealed class ResourceExhaustedExceptionInterceptor : Interceptor
{
    public override async Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        try
        {
            await continuation(request, responseStream, context).ConfigureAwait(false);
        }
        catch (ResourceExhaustedException ex)
        {
            throw ex.ToRpcException();
        }
        catch (SquirixException ex)
        {
            throw ex.ToRpcException();
        }
    }

    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
    {
        try
        {
            return await continuation(request, context).ConfigureAwait(false);
        }
        catch (ResourceExhaustedException ex)
        {
            throw ex.ToRpcException();
        }
        catch (SquirixException ex)
        {
            throw ex.ToRpcException();
        }
    }
}
