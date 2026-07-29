using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Errors;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Adapters.Endpoint;

/// <summary>Covers gRPC mapping of journal capacity through the shared domain-error interceptor.</summary>
public sealed class DomainErrorInterceptorTests : ServerUnitTestBase
{
    /// <summary>Server-streaming handler maps journal capacity to ResourceExhausted.</summary>
    [Fact]
    public async Task StreamingMapsJournalCapacityToRpcResourceExhausted()
    {
        var interceptor = new ResourceExhaustedExceptionInterceptor();
        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            interceptor.ServerStreamingServerHandler(
                "request",
                new NullStreamWriter<string>(),
                new TestServerCallContext(),
                static (_, _, _) => throw new JournalCapacityExceededException()));

        Assert.Equal(StatusCode.ResourceExhausted, ex.StatusCode);
        Assert.Equal(JournalCapacityExceededException.StableDetail, ex.Status.Detail);
    }

    /// <summary>Unary handler maps journal capacity to ResourceExhausted.</summary>
    [Fact]
    public async Task UnaryMapsJournalCapacityToRpcResourceExhausted()
    {
        var interceptor = new ResourceExhaustedExceptionInterceptor();
        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            interceptor.UnaryServerHandler("request", new TestServerCallContext(), static (_, _) => Task.FromException<string>(new JournalCapacityExceededException())));

        Assert.Equal(StatusCode.ResourceExhausted, ex.StatusCode);
        Assert.Equal(JournalCapacityExceededException.StableDetail, ex.Status.Detail);
    }

    private sealed class NullStreamWriter<T> : IServerStreamWriter<T>
    {
        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(T message) => Task.CompletedTask;
    }
}
