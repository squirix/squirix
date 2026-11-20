using System.Collections.Generic;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Binary;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Rest;

/// <summary>Covers binary status projection for <see cref="SquirixException" />.</summary>
[Immutable]
public sealed class BinaryErrorStatusTests : ServerUnitTestBase
{
    /// <summary>Gets theory rows for the explicit binary error status switch arms.</summary>
    public static IEnumerable<(SquirixErrorCode Code, int ExpectedStatus)> StatusCodeCases()
    {
        yield return (SquirixErrorCode.InvalidCacheKey, System.Runtime.CompilerServices.Unsafe.BitCast<FrameStatus, int>(FrameStatus.InvalidArgument));
        yield return (SquirixErrorCode.InvalidEntryTags, System.Runtime.CompilerServices.Unsafe.BitCast<FrameStatus, int>(FrameStatus.InvalidArgument));
        yield return (SquirixErrorCode.OperationIdRequired, System.Runtime.CompilerServices.Unsafe.BitCast<FrameStatus, int>(FrameStatus.InvalidArgument));
        yield return (SquirixErrorCode.OperationIdInvalidFormat, System.Runtime.CompilerServices.Unsafe.BitCast<FrameStatus, int>(FrameStatus.InvalidArgument));
        yield return (SquirixErrorCode.OperationIdTooLong, System.Runtime.CompilerServices.Unsafe.BitCast<FrameStatus, int>(FrameStatus.InvalidArgument));
        yield return (SquirixErrorCode.OperationIdReuseMismatch, System.Runtime.CompilerServices.Unsafe.BitCast<FrameStatus, int>(FrameStatus.FailedPrecondition));
        yield return (SquirixErrorCode.PayloadTooLarge, System.Runtime.CompilerServices.Unsafe.BitCast<FrameStatus, int>(FrameStatus.ResourceExhausted));
        yield return (SquirixErrorCode.TooManyRequests, System.Runtime.CompilerServices.Unsafe.BitCast<FrameStatus, int>(FrameStatus.ResourceExhausted));
        yield return (SquirixErrorCode.MemoryPressure, System.Runtime.CompilerServices.Unsafe.BitCast<FrameStatus, int>(FrameStatus.ResourceExhausted));
        yield return (SquirixErrorCode.JournalDiskQuota, System.Runtime.CompilerServices.Unsafe.BitCast<FrameStatus, int>(FrameStatus.ResourceExhausted));
        yield return (SquirixErrorCode.CommitOutcomeUnknown, System.Runtime.CompilerServices.Unsafe.BitCast<FrameStatus, int>(FrameStatus.Unavailable));
        yield return (SquirixErrorCode.None, System.Runtime.CompilerServices.Unsafe.BitCast<FrameStatus, int>(FrameStatus.Internal));
    }

    /// <summary>Maps each rewritten status arm to the expected binary status.</summary>
    /// <param name="code">Stable squirix error code.</param>
    /// <param name="expectedStatus">Expected binary status.</param>
    [Test]
    [MethodDataSource(nameof(StatusCodeCases))]
    public async Task BinaryErrorsMapStatusCodes(SquirixErrorCode code, int expectedStatus)
    {
        var exception = new SquirixException(code, "ErrorName", "detail");
        var result = BinaryResponse.FromError(exception);

        _ = await Assert.That(System.Runtime.CompilerServices.Unsafe.BitCast<FrameStatus, int>(result.Status)).IsEqualTo(expectedStatus);
    }
}
