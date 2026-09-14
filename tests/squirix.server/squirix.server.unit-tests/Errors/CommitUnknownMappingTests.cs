using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Squirix.Server.Adapters.Rest;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Errors;

/// <summary>Covers the stable server transport contract for an ambiguous durable commit.</summary>
[Immutable]
public sealed class CommitUnknownMappingTests : ServerUnitTestBase
{
    private const string StableCode = "COMMIT_OUTCOME_UNKNOWN";

    /// <summary>Projects commit unknown to unavailable with its stable symbolic code.</summary>
    [Fact]
    public void GrpcUsesUnavailableAndStableCode()
    {
        Assert.Equal(StatusCode.Unavailable, SquirixErrorMapper.ToGrpcStatusCode(SquirixErrorCode.CommitOutcomeUnknown));
        Assert.Equal(StableCode, SquirixErrorMapper.ToPublicCode(SquirixErrorCode.CommitOutcomeUnknown));
    }

    /// <summary>Projects commit unknown to service unavailable with the same stable symbolic code.</summary>
    [Fact]
    public async Task RestUsesServiceUnavailableAndStableCode()
    {
        var contract = ServerOpContract.CommitOutcomeUnknown();
        var (status, payload) = await HttpResultTestKit.ExecuteJsonAsync(contract.ToHttpResult(), DefaultCancellationToken);
        using (payload)
        {
            var response = payload.RootElement.Deserialize(RestJsonSerializerContext.Default.ErrorResponse);

            Assert.Equal(StatusCodes.Status503ServiceUnavailable, status);
            Assert.NotNull(response);
            Assert.Equal(SquirixErrorCode.CommitOutcomeUnknown, contract.Code);
            Assert.Equal(StableCode, response.Code);
            Assert.Equal(contract.Error, response.Error);
            Assert.Equal(contract.Detail, response.Detail);
        }
    }

    /// <summary>Guards every pre-existing numeric value while appending the new code.</summary>
    [Fact]
    public void ExistingErrorCodeNumbersDoNotChange()
    {
        AssertNumericValue(0, SquirixErrorCode.None);
        AssertNumericValue(1, SquirixErrorCode.InvalidCacheKey);
        AssertNumericValue(2, SquirixErrorCode.PayloadTooLarge);
        AssertNumericValue(3, SquirixErrorCode.TooManyRequests);
        AssertNumericValue(4, SquirixErrorCode.MemoryPressure);
        AssertNumericValue(5, SquirixErrorCode.OperationIdRequired);
        AssertNumericValue(6, SquirixErrorCode.OperationIdInvalidFormat);
        AssertNumericValue(7, SquirixErrorCode.OperationIdTooLong);
        AssertNumericValue(8, SquirixErrorCode.OperationIdReuseMismatch);
        AssertNumericValue(9, SquirixErrorCode.InvalidEntryTags);
        AssertNumericValue(10, SquirixErrorCode.JournalDiskQuota);
        AssertNumericValue(11, SquirixErrorCode.CommitOutcomeUnknown);
    }

    private static void AssertNumericValue(int expected, SquirixErrorCode code) =>
        Assert.Equal(expected, Unsafe.As<SquirixErrorCode, int>(ref code));
}
