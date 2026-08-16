using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Squirix.Attributes;
using Squirix.Server.Adapters.Rest;
using Squirix.Server.Errors;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Adapters.Rest;

/// <summary>Covers HTTP status projection for <see cref="SquirixException" />.</summary>
[Immutable]
public sealed class SquirixExceptionHttpExtensionsTests : ServerUnitTestBase
{
    /// <summary>Gets theory rows for the explicit <see cref="SquirixExceptionHttpExtensions" /> switch arms.</summary>
    public static TheoryData<SquirixErrorCode, int> StatusCodeCases =>
        new()
        {
            { SquirixErrorCode.InvalidCacheKey, StatusCodes.Status400BadRequest },
            { SquirixErrorCode.InvalidEntryTags, StatusCodes.Status400BadRequest },
            { SquirixErrorCode.OperationIdRequired, StatusCodes.Status400BadRequest },
            { SquirixErrorCode.OperationIdInvalidFormat, StatusCodes.Status400BadRequest },
            { SquirixErrorCode.OperationIdTooLong, StatusCodes.Status400BadRequest },
            { SquirixErrorCode.OperationIdReuseMismatch, StatusCodes.Status409Conflict },
            { SquirixErrorCode.PayloadTooLarge, StatusCodes.Status413PayloadTooLarge },
            { SquirixErrorCode.TooManyRequests, StatusCodes.Status429TooManyRequests },
            { SquirixErrorCode.MemoryPressure, StatusCodes.Status429TooManyRequests },
            { SquirixErrorCode.JournalDiskQuota, StatusCodes.Status429TooManyRequests },
            { SquirixErrorCode.None, StatusCodes.Status500InternalServerError },
        };

    /// <summary>Maps each rewritten status arm to the expected HTTP code.</summary>
    /// <param name="code">Stable squirix error code.</param>
    /// <param name="expectedStatus">Expected ASP.NET Core status code.</param>
    [Theory]
    [MemberData(nameof(StatusCodeCases))]
    public async Task ToHttpResultMapsStatusCodes(SquirixErrorCode code, int expectedStatus)
    {
        var exception = new SquirixException(code, "ErrorName", "detail");
        var status = await HttpResultTestKit.ExecuteStatusAsync(exception.ToHttpResult(), DefaultCancellationToken);

        Assert.Equal(expectedStatus, status);
    }
}
