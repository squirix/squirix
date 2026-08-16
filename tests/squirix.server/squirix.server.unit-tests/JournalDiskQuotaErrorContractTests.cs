using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Squirix.Attributes;
using Squirix.Server.Adapters.Rest;
using Squirix.Server.Errors;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>Contract tests for journal disk quota mapped through shared error helpers.</summary>
[Immutable]
public sealed class JournalDiskQuotaErrorContractTests : ServerUnitTestBase
{
    /// <summary>Verifies logical cache metrics/tracing classify journal quota as resource exhausted.</summary>
    [Fact]
    public void ClassifierMapsJournalCapacityToExhausted() => Assert.Equal(
        CacheOperationResults.ResourceExhausted,
        CacheOperationClassifier.ClassifyException(new JournalCapacityExceededException()));

    /// <summary>Verifies message and message+inner constructor overloads keep the provided detail text.</summary>
    [Fact]
    public void JournalCapacityCtorsPreserveMessage()
    {
        var withMessage = new JournalCapacityExceededException("quota message");
        Assert.Equal("quota message", withMessage.Message);

        var inner = new InvalidOperationException("inner");
        var withInner = new JournalCapacityExceededException("outer", inner);
        Assert.Equal("outer", withInner.Message);
        Assert.Same(inner, withInner.InnerException);
    }

    /// <summary>Verifies stable codes across REST and gRPC projections for journal disk quota.</summary>
    [Fact]
    public void JournalDiskQuotaMapsToHttp429AndGrpcExhausted() => ErrorContractTestKit.AssertResourceExhaustedGrpcMapping(
        ServerOpContract.JournalDiskQuota(),
        SquirixErrorCode.JournalDiskQuota,
        "JOURNAL_DISK_QUOTA",
        JournalCapacityExceededException.StableDetail,
        static () => new JournalCapacityExceededException().ToRpcException());

    /// <summary>Verifies REST JSON matches canonical error shape for journal disk quota.</summary>
    [Fact]
    public async Task JournalDiskQuotaRestPayloadUsesStableFields()
    {
        var (status, payload) = await HttpResultTestKit.ExecuteJsonAsync(new JournalCapacityExceededException().ToHttpResult(), DefaultCancellationToken);
        using (payload)
        {
            ErrorContractTestKit.AssertErrorJsonPayload(
                payload,
                status,
                StatusCodes.Status429TooManyRequests,
                "JournalDiskQuota",
                "JOURNAL_DISK_QUOTA",
                JournalCapacityExceededException.StableDetail);
        }
    }
}
