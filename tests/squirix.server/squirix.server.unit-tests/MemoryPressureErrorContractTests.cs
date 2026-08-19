using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Squirix.Server.Adapters.Rest;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>Contract tests for memory-pressure admission mapped through shared error helpers.</summary>
[Immutable]
public sealed class MemoryPressureErrorContractTests : ServerUnitTestBase
{
    /// <summary>Verifies stable codes across REST and gRPC projections for memory pressure.</summary>
    [Fact]
    public void MemoryPressureMapsPublicGrpcResourceExhausted() => ErrorContractTestKit.AssertResourceExhaustedGrpcMapping(
        ServerOpContract.MemoryPressure(),
        SquirixErrorCode.MemoryPressure,
        "MEMORY_PRESSURE",
        ResourceExhaustedException.StableDetail,
        static () => new ResourceExhaustedException().ToRpcException());

    /// <summary>Verifies REST JSON matches canonical error shape for memory pressure.</summary>
    [Fact]
    public async Task MemoryPressureRestPayloadUsesStableFields()
    {
        var (status, payload) = await HttpResultTestKit.ExecuteJsonAsync(ServerOpContract.MemoryPressure().ToHttpResult(), DefaultCancellationToken);
        using (payload)
        {
            ErrorContractTestKit.AssertErrorJsonPayload(
                payload,
                status,
                StatusCodes.Status429TooManyRequests,
                "MemoryPressure",
                "MEMORY_PRESSURE",
                ResourceExhaustedException.StableDetail);
        }
    }
}
