using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Errors;

/// <summary>Covers public and gRPC projections for every <see cref="SquirixErrorCode" />.</summary>
[Immutable]
public sealed class SquirixErrorMapperTests : ServerUnitTestBase
{
    /// <summary>Maps each known code to its stable public token and gRPC status.</summary>
    [Test]
    public async Task MapsEveryKnownErrorCode()
    {
        await AssertMappingAsync(SquirixErrorCode.None, "INTERNAL_ERROR", StatusCode.Internal);
        await AssertMappingAsync(SquirixErrorCode.InvalidCacheKey, "INVALID_CACHE_KEY", StatusCode.InvalidArgument);
        await AssertMappingAsync(SquirixErrorCode.PayloadTooLarge, "PAYLOAD_TOO_LARGE", StatusCode.ResourceExhausted);
        await AssertMappingAsync(SquirixErrorCode.TooManyRequests, "TOO_MANY_REQUESTS", StatusCode.ResourceExhausted);
        await AssertMappingAsync(SquirixErrorCode.MemoryPressure, "MEMORY_PRESSURE", StatusCode.ResourceExhausted);
        await AssertMappingAsync(SquirixErrorCode.JournalDiskQuota, "JOURNAL_DISK_QUOTA", StatusCode.ResourceExhausted);
        await AssertMappingAsync(SquirixErrorCode.OperationIdRequired, "OPERATION_ID_REQUIRED", StatusCode.InvalidArgument);
        await AssertMappingAsync(SquirixErrorCode.OperationIdInvalidFormat, "OPERATION_ID_INVALID_FORMAT", StatusCode.InvalidArgument);
        await AssertMappingAsync(SquirixErrorCode.OperationIdTooLong, "OPERATION_ID_TOO_LONG", StatusCode.InvalidArgument);
        await AssertMappingAsync(SquirixErrorCode.OperationIdReuseMismatch, "OPERATION_ID_REUSE_MISMATCH", StatusCode.FailedPrecondition);
        await AssertMappingAsync(SquirixErrorCode.InvalidEntryTags, "INVALID_ENTRY_TAGS", StatusCode.InvalidArgument);
        await AssertMappingAsync(SquirixErrorCode.CommitOutcomeUnknown, "COMMIT_OUTCOME_UNKNOWN", StatusCode.Unavailable);
    }

    /// <summary>Unknown codes fall back to internal error projections.</summary>
    [Test]
    public async Task MapsUnknownCodeToInternalFallback()
    {
        var raw = 999;
        var unknown = Unsafe.As<int, SquirixErrorCode>(ref raw);
        _ = await Assert.That(SquirixErrorMapper.ToPublicCode(unknown)).IsEqualTo("INTERNAL_ERROR");
        _ = await Assert.That(SquirixErrorMapper.ToGrpcStatusCode(unknown)).IsEqualTo(StatusCode.Internal);
    }

    private static async Task AssertMappingAsync(SquirixErrorCode code, string publicCode, StatusCode grpcStatus)
    {
        _ = await Assert.That(SquirixErrorMapper.ToPublicCode(code)).IsEqualTo(publicCode);
        _ = await Assert.That(SquirixErrorMapper.ToGrpcStatusCode(code)).IsEqualTo(grpcStatus);
    }
}
