using Squirix.Transport.Binary;

namespace Squirix.Server.Errors;

internal static class SquirixErrorMapper
{
    internal static FrameStatus ToFrameStatus(SquirixErrorCode code) => code switch
    {
        SquirixErrorCode.None => FrameStatus.Internal,
        SquirixErrorCode.InvalidCacheKey => FrameStatus.InvalidArgument,
        SquirixErrorCode.PayloadTooLarge => FrameStatus.ResourceExhausted,
        SquirixErrorCode.TooManyRequests => FrameStatus.ResourceExhausted,
        SquirixErrorCode.MemoryPressure => FrameStatus.ResourceExhausted,
        SquirixErrorCode.JournalDiskQuota => FrameStatus.ResourceExhausted,
        SquirixErrorCode.OperationIdRequired => FrameStatus.InvalidArgument,
        SquirixErrorCode.OperationIdInvalidFormat => FrameStatus.InvalidArgument,
        SquirixErrorCode.OperationIdTooLong => FrameStatus.InvalidArgument,
        SquirixErrorCode.OperationIdReuseMismatch => FrameStatus.FailedPrecondition,
        SquirixErrorCode.InvalidEntryTags => FrameStatus.InvalidArgument,
        SquirixErrorCode.CommitOutcomeUnknown => FrameStatus.Unavailable,
        _ => FrameStatus.Internal,
    };

    internal static string ToPublicCode(SquirixErrorCode code) => code switch
    {
        SquirixErrorCode.None => "INTERNAL_ERROR",
        SquirixErrorCode.InvalidCacheKey => "INVALID_CACHE_KEY",
        SquirixErrorCode.PayloadTooLarge => Frames.PayloadTooLargeCode,
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
