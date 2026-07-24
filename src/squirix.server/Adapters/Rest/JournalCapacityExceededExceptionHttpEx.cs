using Microsoft.AspNetCore.Http;
using Squirix.Server.Errors;

namespace Squirix.Server.Adapters.Rest;

internal static class JournalCapacityExceededExceptionHttpEx
{
    extension(JournalCapacityExceededException exception)
    {
        internal IResult ToHttpResult()
        {
            _ = exception;
            return ServerOpContract.JournalDiskQuota().ToHttpResult();
        }
    }
}
