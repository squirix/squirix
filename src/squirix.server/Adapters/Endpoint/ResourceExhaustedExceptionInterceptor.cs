using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Squirix.Server.Errors;

namespace Squirix.Server.Adapters.Endpoint;

internal sealed class ResourceExhaustedExceptionInterceptor : Interceptor
{
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
        catch (JournalCapacityExceededException ex)
        {
            throw ex.ToRpcException();
        }
        catch (ServerOpIdMismatchException)
        {
            throw ServerOpContract.OperationIdReuseMismatch().ToRpcException();
        }
        catch (SquirixException ex)
        {
            throw ex.ToRpcException();
        }
    }
}
