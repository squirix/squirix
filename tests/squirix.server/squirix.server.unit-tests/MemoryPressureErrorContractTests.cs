using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Squirix.Server.Adapters.Rest;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.UnitTests.Support;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Contract tests for memory-pressure admission mapped through shared error helpers.</summary>
[Immutable]
public sealed class MemoryPressureErrorContractTests : ServerUnitTestBase
{
    /// <summary>Verifies stable codes across REST and gRPC projections for memory pressure.</summary>
    [Test]
    public Task PressureMapsToGrpcResourceExhausted() => ErrorContractTestKit.AssertResourceExhaustedGrpcMapping(
        ServerOpContract.MemoryPressure(),
        SquirixErrorCode.MemoryPressure,
        "MEMORY_PRESSURE",
        ResourceExhaustedException.StableDetail,
        static () => new ResourceExhaustedException().ToRpcException());

    /// <summary>Verifies REST JSON matches canonical error shape for memory pressure.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PressureRestPayloadUsesStableFields(CancellationToken cancellationToken)
    {
        var (status, payload) = await HttpResultTestKit.ExecuteJsonAsync(ServerOpContract.MemoryPressure().ToHttpResult(), cancellationToken);
        using (payload)
        {
            await ErrorContractTestKit.AssertErrorJsonPayload(
                payload,
                status,
                StatusCodes.Status429TooManyRequests,
                "MemoryPressure",
                "MEMORY_PRESSURE",
                ResourceExhaustedException.StableDetail);
        }
    }
}
