using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Adapters.Rest;
using Squirix.Server.Errors;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>Contract tests for journal disk quota mapped through shared error helpers.</summary>
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
    public void JournalDiskQuotaMapsToHttp429AndGrpcExhausted()
    {
        var contract = ServerOpContract.JournalDiskQuota();

        Assert.Equal(SquirixErrorCode.JournalDiskQuota, contract.Code);
        Assert.Equal("JOURNAL_DISK_QUOTA", SquirixErrorMapper.ToPublicCode(contract.Code));

        var rpc = contract.ToRpcException();
        Assert.Equal(StatusCode.ResourceExhausted, rpc.StatusCode);
        Assert.Equal(JournalCapacityExceededException.StableDetail, rpc.Status.Detail);

        var direct = new JournalCapacityExceededException().ToRpcException();
        Assert.Equal(StatusCode.ResourceExhausted, direct.StatusCode);
        Assert.Equal(JournalCapacityExceededException.StableDetail, direct.Status.Detail);
    }

    /// <summary>Verifies REST JSON matches canonical error shape for journal disk quota.</summary>
    [Fact]
    public async Task JournalDiskQuotaRestPayloadUsesStableFields()
    {
        var http = new JournalCapacityExceededException().ToHttpResult();
        var context = new DefaultHttpContext
        {
            Response =
            {
                Body = new MemoryStream(),
            },
            RequestServices = new ServiceCollection().AddLogging().BuildServiceProvider(),
        };

        await http.ExecuteAsync(context);
        context.Response.Body.Position = 0;
        using var payload = await JsonDocument.ParseAsync(context.Response.Body, cancellationToken: DefaultCancellationToken);

        Assert.Equal(StatusCodes.Status429TooManyRequests, context.Response.StatusCode);
        Assert.Equal("JournalDiskQuota", payload.RootElement.GetProperty("error").GetString());
        Assert.Equal("JOURNAL_DISK_QUOTA", payload.RootElement.GetProperty("code").GetString());
        Assert.Equal(JournalCapacityExceededException.StableDetail, payload.RootElement.GetProperty("detail").GetString());
    }
}
