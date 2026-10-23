using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Squirix.Server.Adapters.Rest;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Rest;

/// <summary>Covers HTTP status projection for <see cref="SquirixException" />.</summary>
[Immutable]
public sealed class SquirixExceptionHttpExtensionsTests : ServerUnitTestBase
{
    /// <summary>Gets theory rows for the explicit <see cref="SquirixExceptionHttpExtensions" /> switch arms.</summary>
    public static IEnumerable<(SquirixErrorCode Code, int ExpectedStatus)> StatusCodeCases()
    {
        yield return (SquirixErrorCode.InvalidCacheKey, StatusCodes.Status400BadRequest);
        yield return (SquirixErrorCode.InvalidEntryTags, StatusCodes.Status400BadRequest);
        yield return (SquirixErrorCode.OperationIdRequired, StatusCodes.Status400BadRequest);
        yield return (SquirixErrorCode.OperationIdInvalidFormat, StatusCodes.Status400BadRequest);
        yield return (SquirixErrorCode.OperationIdTooLong, StatusCodes.Status400BadRequest);
        yield return (SquirixErrorCode.OperationIdReuseMismatch, StatusCodes.Status409Conflict);
        yield return (SquirixErrorCode.PayloadTooLarge, StatusCodes.Status413PayloadTooLarge);
        yield return (SquirixErrorCode.TooManyRequests, StatusCodes.Status429TooManyRequests);
        yield return (SquirixErrorCode.MemoryPressure, StatusCodes.Status429TooManyRequests);
        yield return (SquirixErrorCode.JournalDiskQuota, StatusCodes.Status429TooManyRequests);
        yield return (SquirixErrorCode.CommitOutcomeUnknown, StatusCodes.Status503ServiceUnavailable);
        yield return (SquirixErrorCode.None, StatusCodes.Status500InternalServerError);
    }

    /// <summary>Maps each rewritten status arm to the expected HTTP code.</summary>
    /// <param name="code">Stable squirix error code.</param>
    /// <param name="expectedStatus">Expected ASP.NET Core status code.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [MethodDataSource(nameof(StatusCodeCases))]
    public async Task ToHttpResultMapsStatusCodes(SquirixErrorCode code, int expectedStatus, CancellationToken cancellationToken)
    {
        var exception = new SquirixException(code, "ErrorName", "detail");
        var status = await HttpResultTestKit.ExecuteStatusAsync(exception.ToHttpResult(), cancellationToken);

        _ = await Assert.That(status).IsEqualTo(expectedStatus);
    }
}
