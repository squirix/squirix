using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Binary;
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
        await AssertMappingAsync(SquirixErrorCode.None, "INTERNAL_ERROR", FrameStatus.Internal);
        await AssertMappingAsync(SquirixErrorCode.InvalidCacheKey, "INVALID_CACHE_KEY", FrameStatus.InvalidArgument);
        await AssertMappingAsync(SquirixErrorCode.PayloadTooLarge, "PAYLOAD_TOO_LARGE", FrameStatus.ResourceExhausted);
        await AssertMappingAsync(SquirixErrorCode.TooManyRequests, "TOO_MANY_REQUESTS", FrameStatus.ResourceExhausted);
        await AssertMappingAsync(SquirixErrorCode.MemoryPressure, "MEMORY_PRESSURE", FrameStatus.ResourceExhausted);
        await AssertMappingAsync(SquirixErrorCode.JournalDiskQuota, "JOURNAL_DISK_QUOTA", FrameStatus.ResourceExhausted);
        await AssertMappingAsync(SquirixErrorCode.OperationIdRequired, "OPERATION_ID_REQUIRED", FrameStatus.InvalidArgument);
        await AssertMappingAsync(SquirixErrorCode.OperationIdInvalidFormat, "OPERATION_ID_INVALID_FORMAT", FrameStatus.InvalidArgument);
        await AssertMappingAsync(SquirixErrorCode.OperationIdTooLong, "OPERATION_ID_TOO_LONG", FrameStatus.InvalidArgument);
        await AssertMappingAsync(SquirixErrorCode.OperationIdReuseMismatch, "OPERATION_ID_REUSE_MISMATCH", FrameStatus.FailedPrecondition);
        await AssertMappingAsync(SquirixErrorCode.InvalidEntryTags, "INVALID_ENTRY_TAGS", FrameStatus.InvalidArgument);
        await AssertMappingAsync(SquirixErrorCode.CommitOutcomeUnknown, "COMMIT_OUTCOME_UNKNOWN", FrameStatus.Unavailable);
    }

    /// <summary>Unknown codes fall back to internal error projections.</summary>
    [Test]
    public async Task MapsUnknownCodeToInternalFallback()
    {
        var raw = 999;
        var unknown = Unsafe.As<int, SquirixErrorCode>(ref raw);
        _ = await Assert.That(SquirixErrorMapper.ToPublicCode(unknown)).IsEqualTo("INTERNAL_ERROR");
        _ = await Assert.That(SquirixErrorMapper.ToFrameStatus(unknown)).IsEqualTo(FrameStatus.Internal);
    }

    private static async Task AssertMappingAsync(SquirixErrorCode code, string publicCode, FrameStatus grpcStatus)
    {
        _ = await Assert.That(SquirixErrorMapper.ToPublicCode(code)).IsEqualTo(publicCode);
        _ = await Assert.That(SquirixErrorMapper.ToFrameStatus(code)).IsEqualTo(grpcStatus);
    }
}
