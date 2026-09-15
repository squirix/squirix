using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Squirix.Server.Adapters.Rest;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Errors;

/// <summary>Covers the stable server transport contract for an ambiguous durable commit.</summary>
[Immutable]
public sealed class CommitUnknownMappingTests : ServerUnitTestBase
{
    private const string StableCode = "COMMIT_OUTCOME_UNKNOWN";

    /// <summary>Guards every pre-existing numeric value while appending the new code.</summary>
    [Test]
    public async Task ExistingErrorCodeNumbersDoNotChange()
    {
        await AssertNumericValue(0, SquirixErrorCode.None);
        await AssertNumericValue(1, SquirixErrorCode.InvalidCacheKey);
        await AssertNumericValue(2, SquirixErrorCode.PayloadTooLarge);
        await AssertNumericValue(3, SquirixErrorCode.TooManyRequests);
        await AssertNumericValue(4, SquirixErrorCode.MemoryPressure);
        await AssertNumericValue(5, SquirixErrorCode.OperationIdRequired);
        await AssertNumericValue(6, SquirixErrorCode.OperationIdInvalidFormat);
        await AssertNumericValue(7, SquirixErrorCode.OperationIdTooLong);
        await AssertNumericValue(8, SquirixErrorCode.OperationIdReuseMismatch);
        await AssertNumericValue(9, SquirixErrorCode.InvalidEntryTags);
        await AssertNumericValue(10, SquirixErrorCode.JournalDiskQuota);
        await AssertNumericValue(11, SquirixErrorCode.CommitOutcomeUnknown);
    }

    /// <summary>Projects commit unknown to unavailable with its stable symbolic code.</summary>
    [Test]
    public async Task GrpcUsesUnavailableAndStableCode()
    {
        _ = await Assert.That(SquirixErrorMapper.ToGrpcStatusCode(SquirixErrorCode.CommitOutcomeUnknown)).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(SquirixErrorMapper.ToPublicCode(SquirixErrorCode.CommitOutcomeUnknown)).IsEqualTo(StableCode);
    }

    /// <summary>Projects commit unknown to service unavailable with the same stable symbolic code.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestUsesServiceUnavailableAndStableCode(CancellationToken cancellationToken)
    {
        var contract = ServerOpContract.CommitOutcomeUnknown();
        var (status, payload) = await HttpResultTestKit.ExecuteJsonAsync(contract.ToHttpResult(), cancellationToken);
        using (payload)
        {
            var response = payload.RootElement.Deserialize(RestJsonSerializerContext.Default.ErrorResponse);

            _ = await Assert.That(status).IsEqualTo(StatusCodes.Status503ServiceUnavailable);
            _ = await Assert.That(response).IsNotNull();
            _ = await Assert.That(contract.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
            _ = await Assert.That(response.Code).IsEqualTo(StableCode);
            _ = await Assert.That(response.Error).IsEqualTo(contract.Error);
            _ = await Assert.That(response.Detail).IsEqualTo(contract.Detail);
        }
    }

    private static async Task AssertNumericValue(int expected, SquirixErrorCode code) => _ = await Assert.That(Unsafe.As<SquirixErrorCode, int>(ref code)).IsEqualTo(expected);
}
