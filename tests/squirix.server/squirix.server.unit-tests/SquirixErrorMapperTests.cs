using System.Runtime.CompilerServices;
using Grpc.Core;
using Squirix.Server.Errors;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Errors;

/// <summary>Covers public and gRPC projections for every <see cref="SquirixErrorCode" />.</summary>
public sealed class SquirixErrorMapperTests : ServerUnitTestBase
{
    /// <summary>Maps each known code to its stable public token and gRPC status.</summary>
    [Fact]
    public void MapsEveryKnownErrorCode()
    {
        AssertMapping(SquirixErrorCode.InvalidCacheName, "INVALID_CACHE_NAME", StatusCode.InvalidArgument);
        AssertMapping(SquirixErrorCode.InvalidCacheKey, "INVALID_CACHE_KEY", StatusCode.InvalidArgument);
        AssertMapping(SquirixErrorCode.BadRequest, "BAD_REQUEST", StatusCode.InvalidArgument);
        AssertMapping(SquirixErrorCode.NotFound, "NOT_FOUND", StatusCode.NotFound);
        AssertMapping(SquirixErrorCode.Conflict, "CONFLICT", StatusCode.FailedPrecondition);
        AssertMapping(SquirixErrorCode.PayloadTooLarge, "PAYLOAD_TOO_LARGE", StatusCode.ResourceExhausted);
        AssertMapping(SquirixErrorCode.TooManyRequests, "TOO_MANY_REQUESTS", StatusCode.ResourceExhausted);
        AssertMapping(SquirixErrorCode.MemoryPressure, "MEMORY_PRESSURE", StatusCode.ResourceExhausted);
        AssertMapping(SquirixErrorCode.JournalDiskQuota, "JOURNAL_DISK_QUOTA", StatusCode.ResourceExhausted);
        AssertMapping(SquirixErrorCode.OperationIdRequired, "OPERATION_ID_REQUIRED", StatusCode.InvalidArgument);
        AssertMapping(SquirixErrorCode.OperationIdInvalidFormat, "OPERATION_ID_INVALID_FORMAT", StatusCode.InvalidArgument);
        AssertMapping(SquirixErrorCode.OperationIdTooLong, "OPERATION_ID_TOO_LONG", StatusCode.InvalidArgument);
        AssertMapping(SquirixErrorCode.OperationIdReuseMismatch, "OPERATION_ID_REUSE_MISMATCH", StatusCode.FailedPrecondition);
        AssertMapping(SquirixErrorCode.InvalidEntryTags, "INVALID_ENTRY_TAGS", StatusCode.InvalidArgument);
    }

    /// <summary>Unknown codes fall back to internal error projections.</summary>
    [Fact]
    public void MapsUnknownCodeToInternalFallback()
    {
        var raw = 999;
        var unknown = Unsafe.As<int, SquirixErrorCode>(ref raw);
        Assert.Equal("INTERNAL_ERROR", SquirixErrorMapper.ToPublicCode(unknown));
        Assert.Equal(StatusCode.Internal, SquirixErrorMapper.ToGrpcStatusCode(unknown));
    }

    private static void AssertMapping(SquirixErrorCode code, string publicCode, StatusCode grpcStatus)
    {
        Assert.Equal(publicCode, SquirixErrorMapper.ToPublicCode(code));
        Assert.Equal(grpcStatus, SquirixErrorMapper.ToGrpcStatusCode(code));
    }
}
