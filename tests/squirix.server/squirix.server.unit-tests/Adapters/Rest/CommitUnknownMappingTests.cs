using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Cache;
using Squirix.Transport.Wire.Mappers;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Rest;

/// <summary>Covers the stable server transport contract for an ambiguous durable commit.</summary>
[Immutable]
public sealed class CommitUnknownMappingTests : ServerUnitTestBase
{
    private const string StableCode = "COMMIT_OUTCOME_UNKNOWN";

    /// <summary>Guards every pre-existing numeric value while appending the new code.</summary>
    [Test]
    public async Task ExistingErrorCodeNumbersDoNotChange()
    {
        await AssertNumericValueAsync(0, SquirixErrorCode.None);
        await AssertNumericValueAsync(1, SquirixErrorCode.InvalidCacheKey);
        await AssertNumericValueAsync(2, SquirixErrorCode.PayloadTooLarge);
        await AssertNumericValueAsync(3, SquirixErrorCode.TooManyRequests);
        await AssertNumericValueAsync(4, SquirixErrorCode.MemoryPressure);
        await AssertNumericValueAsync(5, SquirixErrorCode.OperationIdRequired);
        await AssertNumericValueAsync(6, SquirixErrorCode.OperationIdInvalidFormat);
        await AssertNumericValueAsync(7, SquirixErrorCode.OperationIdTooLong);
        await AssertNumericValueAsync(8, SquirixErrorCode.OperationIdReuseMismatch);
        await AssertNumericValueAsync(9, SquirixErrorCode.InvalidEntryTags);
        await AssertNumericValueAsync(10, SquirixErrorCode.JournalDiskQuota);
        await AssertNumericValueAsync(11, SquirixErrorCode.CommitOutcomeUnknown);
    }

    /// <summary>Projects commit unknown to unavailable with its stable symbolic code.</summary>
    [Test]
    public async Task GrpcUsesUnavailableAndStableCode()
    {
        _ = await Assert.That(SquirixErrorMapper.ToFrameStatus(SquirixErrorCode.CommitOutcomeUnknown)).IsEqualTo(FrameStatus.Unavailable);
        _ = await Assert.That(SquirixErrorMapper.ToPublicCode(SquirixErrorCode.CommitOutcomeUnknown)).IsEqualTo(StableCode);
    }

    /// <summary>Checks the status, error code trailer and detail received by a binary client.</summary>
    [Test]
    public async Task BinaryPayloadUsesStableFields()
    {
        var contract = ServerOpContract.CommitOutcomeUnknown();
        var response = BinaryResponse.FromError(contract);
        var error = await Assert.That(response.Payload).IsTypeOf<BinaryCallError>();
        _ = await Assert.That(response.Status).IsEqualTo(FrameStatus.Unavailable);
        _ = await Assert.That(error!.Detail).IsEqualTo(contract.Detail);
        _ = await Assert.That(error.Trailers.Count).IsEqualTo(1);
        _ = await Assert.That(error.Trailers[0].Key).IsEqualTo(StaleOwnerMarkers.ErrorCodeMetadataKey);
        _ = await Assert.That(error.Trailers[0].Text).IsEqualTo(StableCode);
    }

    private static async Task AssertNumericValueAsync(int expected, SquirixErrorCode code) => _ = await Assert.That(Unsafe.As<SquirixErrorCode, int>(ref code)).IsEqualTo(expected);
}
