using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Adapters.Rest;
using Squirix.Server.Errors;
using Xunit;

namespace Squirix.Server.UnitTests.Errors;

/// <summary>
/// Contract tests for memory-pressure admission mapped through shared error helpers.
/// </summary>
public sealed class MemoryPressureErrorContractTests : ServerUnitTestBase
{
    /// <summary>
    /// Verifies stable codes across REST and gRPC projections for memory pressure.
    /// </summary>
    [Fact]
    public void MemoryPressureMapsToPublicCodeHttp429AndGrpcResourceExhausted()
    {
        var contract = CacheOperationContract.MemoryPressure();

        Assert.Equal(SquirixErrorCode.MemoryPressure, contract.Code);
        Assert.Equal("MEMORY_PRESSURE", SquirixErrorMapper.ToPublicCode(contract.Code));

        var rpc = contract.ToRpcException();
        Assert.Equal(StatusCode.ResourceExhausted, rpc.StatusCode);
        Assert.Equal(ResourceExhaustedException.StableDetail, rpc.Status.Detail);

        var direct = new ResourceExhaustedException().ToRpcException();
        Assert.Equal(StatusCode.ResourceExhausted, direct.StatusCode);
        Assert.Equal(ResourceExhaustedException.StableDetail, direct.Status.Detail);
    }

    /// <summary>
    /// Verifies REST JSON matches canonical error shape for memory pressure.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task MemoryPressureRestPayloadUsesStableFields()
    {
        var contract = CacheOperationContract.MemoryPressure();
        var http = contract.ToHttpResult();
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
        Assert.Equal("MemoryPressure", payload.RootElement.GetProperty("error").GetString());
        Assert.Equal("MEMORY_PRESSURE", payload.RootElement.GetProperty("code").GetString());
        Assert.Equal(ResourceExhaustedException.StableDetail, payload.RootElement.GetProperty("detail").GetString());
    }
}
