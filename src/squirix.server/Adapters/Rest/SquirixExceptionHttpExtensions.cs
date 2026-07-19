using Microsoft.AspNetCore.Http;
using Squirix.Server.Errors;

namespace Squirix.Server.Adapters.Rest;

internal static class SquirixExceptionHttpExtensions
{
    extension(SquirixException exception)
    {
        internal IResult ToHttpResult()
        {
            var statusCode = exception.Code switch
            {
                SquirixErrorCode.InvalidCacheName => StatusCodes.Status400BadRequest,
                SquirixErrorCode.InvalidCacheKey => StatusCodes.Status400BadRequest,
                SquirixErrorCode.BadRequest => StatusCodes.Status400BadRequest,
                SquirixErrorCode.NotFound => StatusCodes.Status404NotFound,
                SquirixErrorCode.Conflict => StatusCodes.Status409Conflict,
                SquirixErrorCode.PayloadTooLarge => StatusCodes.Status413PayloadTooLarge,
                SquirixErrorCode.TooManyRequests => StatusCodes.Status429TooManyRequests,
                SquirixErrorCode.MemoryPressure => StatusCodes.Status429TooManyRequests,
                _ => StatusCodes.Status500InternalServerError,
            };

            return Results.Json(
                new ErrorResponse(exception.Error, SquirixErrorMapper.ToPublicCode(exception.Code), exception.Detail),
                RestJsonSerializerContext.Default.ErrorResponse,
                statusCode: statusCode);
        }
    }
}
