using System;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Cache;
using Squirix.Transport.Wire.Mappers;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Contract tests for journal disk quota mapped through shared error helpers.</summary>
[Immutable]
public sealed class JournalDiskQuotaErrorContractTests : ServerUnitTestBase
{
    /// <summary>Verifies logical cache metrics/tracing classify journal quota as resource exhausted.</summary>
    [Test]
    public async Task ClassifierMapsJournalCapacityToExhausted() => _ = await Assert.That(CacheOperationClassifier.ClassifyException(new JournalCapacityExceededException()))
                                                                                    .IsEqualTo(CacheOperationResults.ResourceExhausted);

    /// <summary>Verifies stable codes across REST and gRPC projections for journal disk quota.</summary>
    [Test]
    public Task DiskQuotaMapsToHttp429AndGrpcExhausted() => ErrorContractTestKit.AssertResourceExhaustedGrpcMappingAsync(
        ServerOpContract.JournalDiskQuota(),
        SquirixErrorCode.JournalDiskQuota,
        "JOURNAL_DISK_QUOTA",
        JournalCapacityExceededException.StableDetail);

    /// <summary>Checks the status, error code trailer and detail received by a binary client.</summary>
    [Test]
    public async Task BinaryPayloadUsesStableFields()
    {
        var contract = ServerOpContract.JournalDiskQuota();
        var response = BinaryResponse.FromError(contract);
        var error = await Assert.That(response.Payload).IsTypeOf<BinaryCallError>();
        _ = await Assert.That(response.Status).IsEqualTo(FrameStatus.ResourceExhausted);
        _ = await Assert.That(error!.Detail).IsEqualTo(JournalCapacityExceededException.StableDetail);
        _ = await Assert.That(error.Trailers.Count).IsEqualTo(1);
        _ = await Assert.That(error.Trailers[0].Key).IsEqualTo(StaleOwnerMarkers.ErrorCodeMetadataKey);
        _ = await Assert.That(error.Trailers[0].Text).IsEqualTo("JOURNAL_DISK_QUOTA");
    }

    /// <summary>Verifies message and message+inner constructor overloads keep the provided detail text.</summary>
    [Test]
    public async Task JournalCapacityCtorsPreserveMessage()
    {
        var withMessage = new JournalCapacityExceededException("quota message");
        _ = await Assert.That(withMessage.Message).IsEqualTo("quota message");

        var inner = new InvalidOperationException("inner");
        var withInner = new JournalCapacityExceededException("outer", inner);
        _ = await Assert.That(withInner.Message).IsEqualTo("outer");
        _ = await Assert.That(withInner.InnerException).IsSameReferenceAs(inner);
    }
}
