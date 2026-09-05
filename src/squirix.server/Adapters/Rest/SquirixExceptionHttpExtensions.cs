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
                SquirixErrorCode.InvalidCacheKey => StatusCodes.Status400BadRequest,
                SquirixErrorCode.InvalidEntryTags => StatusCodes.Status400BadRequest,
                SquirixErrorCode.OperationIdRequired => StatusCodes.Status400BadRequest,
                SquirixErrorCode.OperationIdInvalidFormat => StatusCodes.Status400BadRequest,
                SquirixErrorCode.OperationIdTooLong => StatusCodes.Status400BadRequest,
                SquirixErrorCode.OperationIdReuseMismatch => StatusCodes.Status409Conflict,
                SquirixErrorCode.PayloadTooLarge => StatusCodes.Status413PayloadTooLarge,
                SquirixErrorCode.TooManyRequests => StatusCodes.Status429TooManyRequests,
                SquirixErrorCode.MemoryPressure => StatusCodes.Status429TooManyRequests,
                SquirixErrorCode.JournalDiskQuota => StatusCodes.Status429TooManyRequests,
                SquirixErrorCode.CommitOutcomeUnknown => StatusCodes.Status503ServiceUnavailable,
                _ => StatusCodes.Status500InternalServerError,
            };

            return Results.Json(
                new ErrorResponse(exception.Error, SquirixErrorMapper.ToPublicCode(exception.Code), exception.Detail),
                RestJsonSerializerContext.Default.ErrorResponse,
                statusCode: statusCode);
        }
    }
}
