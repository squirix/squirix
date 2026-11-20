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

namespace Squirix.Server.UnitTests;

/// <summary>Contract tests for memory-pressure admission mapped through shared error helpers.</summary>
[Immutable]
public sealed class MemoryPressureErrorContractTests : ServerUnitTestBase
{
    /// <summary>Verifies stable codes across REST and gRPC projections for memory pressure.</summary>
    [Test]
    public Task PressureMapsToGrpcResourceExhausted() => ErrorContractTestKit.AssertResourceExhaustedGrpcMappingAsync(
        ServerOpContract.MemoryPressure(),
        SquirixErrorCode.MemoryPressure,
        "MEMORY_PRESSURE",
        ResourceExhaustedException.StableDetail);

    /// <summary>Checks the status, error code trailer and detail received by a binary client.</summary>
    [Test]
    public async Task BinaryPayloadUsesStableFields()
    {
        var contract = ServerOpContract.MemoryPressure();
        var response = BinaryResponse.FromError(contract);
        var error = await Assert.That(response.Payload).IsTypeOf<BinaryCallError>();
        _ = await Assert.That(response.Status).IsEqualTo(FrameStatus.ResourceExhausted);
        _ = await Assert.That(error!.Detail).IsEqualTo(ResourceExhaustedException.StableDetail);
        _ = await Assert.That(error.Trailers.Count).IsEqualTo(1);
        _ = await Assert.That(error.Trailers[0].Key).IsEqualTo(StaleOwnerMarkers.ErrorCodeMetadataKey);
        _ = await Assert.That(error.Trailers[0].Text).IsEqualTo("MEMORY_PRESSURE");
    }
}
