using Grpc.Core;

namespace Squirix.Server.Errors;

internal static class SquirixErrorMapper
{
    internal static StatusCode ToGrpcStatusCode(SquirixErrorCode code) => code switch
    {
        SquirixErrorCode.InvalidCacheKey => StatusCode.InvalidArgument,
        SquirixErrorCode.PayloadTooLarge => StatusCode.ResourceExhausted,
        SquirixErrorCode.TooManyRequests => StatusCode.ResourceExhausted,
        SquirixErrorCode.MemoryPressure => StatusCode.ResourceExhausted,
        SquirixErrorCode.JournalDiskQuota => StatusCode.ResourceExhausted,
        SquirixErrorCode.OperationIdRequired => StatusCode.InvalidArgument,
        SquirixErrorCode.OperationIdInvalidFormat => StatusCode.InvalidArgument,
        SquirixErrorCode.OperationIdTooLong => StatusCode.InvalidArgument,
        SquirixErrorCode.OperationIdReuseMismatch => StatusCode.FailedPrecondition,
        SquirixErrorCode.InvalidEntryTags => StatusCode.InvalidArgument,
        SquirixErrorCode.CommitOutcomeUnknown => StatusCode.Unavailable,
        _ => StatusCode.Internal,
    };

    internal static string ToPublicCode(SquirixErrorCode code) => code switch
    {
        SquirixErrorCode.InvalidCacheKey => "INVALID_CACHE_KEY",
        SquirixErrorCode.PayloadTooLarge => "PAYLOAD_TOO_LARGE",
        SquirixErrorCode.TooManyRequests => "TOO_MANY_REQUESTS",
        SquirixErrorCode.MemoryPressure => "MEMORY_PRESSURE",
        SquirixErrorCode.OperationIdRequired => "OPERATION_ID_REQUIRED",
        SquirixErrorCode.OperationIdInvalidFormat => "OPERATION_ID_INVALID_FORMAT",
        SquirixErrorCode.OperationIdTooLong => "OPERATION_ID_TOO_LONG",
        SquirixErrorCode.OperationIdReuseMismatch => "OPERATION_ID_REUSE_MISMATCH",
        SquirixErrorCode.InvalidEntryTags => "INVALID_ENTRY_TAGS",
        SquirixErrorCode.JournalDiskQuota => "JOURNAL_DISK_QUOTA",
        SquirixErrorCode.CommitOutcomeUnknown => "COMMIT_OUTCOME_UNKNOWN",
        _ => "INTERNAL_ERROR",
    };
}
