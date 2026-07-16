using Microsoft.AspNetCore.Http;
using Squirix.Server.Errors;

namespace Squirix.Server.Adapters.Rest;

internal static class ResourceExhaustedExceptionHttpEx
{
    extension(ResourceExhaustedException exception)
    {
        internal IResult ToHttpResult()
        {
            _ = exception;
            return ServerOpContract.MemoryPressure().ToHttpResult();
        }
    }
}
