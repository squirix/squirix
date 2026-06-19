using Grpc.Core;

namespace Squirix.Server.Errors;

internal static class SquirixErrorMapper
{
    public static string ToPublicCode(SquirixErrorCode code) => code switch
    {
        SquirixErrorCode.InvalidCacheName => "INVALID_CACHE_NAME",
        SquirixErrorCode.InvalidCacheKey => "INVALID_CACHE_KEY",
        SquirixErrorCode.BadRequest => "BAD_REQUEST",
        SquirixErrorCode.NotFound => "NOT_FOUND",
        SquirixErrorCode.Conflict => "CONFLICT",
        SquirixErrorCode.PayloadTooLarge => "PAYLOAD_TOO_LARGE",
        SquirixErrorCode.TooManyRequests => "TOO_MANY_REQUESTS",
        SquirixErrorCode.MemoryPressure => ResourceExhaustedException.PublicErrorCode,
        SquirixErrorCode.OperationIdRequired => "OPERATION_ID_REQUIRED",
        SquirixErrorCode.OperationIdInvalidFormat => "OPERATION_ID_INVALID_FORMAT",
        SquirixErrorCode.OperationIdTooLong => "OPERATION_ID_TOO_LONG",
        SquirixErrorCode.OperationIdReuseMismatch => "OPERATION_ID_REUSE_MISMATCH",
        _ => "INTERNAL_ERROR",
    };

    internal static StatusCode ToGrpcStatusCode(SquirixErrorCode code) => code switch
    {
        SquirixErrorCode.InvalidCacheName => StatusCode.InvalidArgument,
        SquirixErrorCode.InvalidCacheKey => StatusCode.InvalidArgument,
        SquirixErrorCode.BadRequest => StatusCode.InvalidArgument,
        SquirixErrorCode.NotFound => StatusCode.NotFound,
        SquirixErrorCode.Conflict => StatusCode.FailedPrecondition,
        SquirixErrorCode.PayloadTooLarge => StatusCode.ResourceExhausted,
        SquirixErrorCode.TooManyRequests => StatusCode.ResourceExhausted,
        SquirixErrorCode.MemoryPressure => StatusCode.ResourceExhausted,
        SquirixErrorCode.OperationIdRequired => StatusCode.InvalidArgument,
        SquirixErrorCode.OperationIdInvalidFormat => StatusCode.InvalidArgument,
        SquirixErrorCode.OperationIdTooLong => StatusCode.InvalidArgument,
        SquirixErrorCode.OperationIdReuseMismatch => StatusCode.FailedPrecondition,
        _ => StatusCode.Internal,
    };
}
