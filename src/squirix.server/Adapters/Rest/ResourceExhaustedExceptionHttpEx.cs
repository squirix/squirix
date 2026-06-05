using Microsoft.AspNetCore.Http;
using Squirix.Server.Errors;

namespace Squirix.Server.Adapters.Rest;

internal static class ResourceExhaustedExceptionHttpEx
{
    extension(ResourceExhaustedException exception)
    {
        public IResult ToHttpResult()
        {
            _ = exception;
            return CacheOperationContract.MemoryPressure().ToHttpResult();
        }
    }
}
