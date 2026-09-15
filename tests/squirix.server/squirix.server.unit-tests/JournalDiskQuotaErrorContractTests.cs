using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Squirix.Server.Adapters.Rest;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.UnitTests.Support;
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
    public Task DiskQuotaMapsToHttp429AndGrpcExhausted() => ErrorContractTestKit.AssertResourceExhaustedGrpcMapping(
        ServerOpContract.JournalDiskQuota(),
        SquirixErrorCode.JournalDiskQuota,
        "JOURNAL_DISK_QUOTA",
        JournalCapacityExceededException.StableDetail,
        static () => new JournalCapacityExceededException().ToRpcException());

    /// <summary>Verifies REST JSON matches canonical error shape for journal disk quota.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DiskQuotaRestPayloadUsesStableFields(CancellationToken cancellationToken)
    {
        var (status, payload) = await HttpResultTestKit.ExecuteJsonAsync(new JournalCapacityExceededException().ToHttpResult(), cancellationToken);
        using (payload)
        {
            await ErrorContractTestKit.AssertErrorJsonPayload(
                payload,
                status,
                StatusCodes.Status429TooManyRequests,
                "JournalDiskQuota",
                "JOURNAL_DISK_QUOTA",
                JournalCapacityExceededException.StableDetail);
        }
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
