using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Errors;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Adapters.Endpoint;

/// <summary>Covers gRPC mapping of journal capacity through the shared domain-error interceptor.</summary>
public sealed class DomainErrorInterceptorTests : ServerUnitTestBase
{
    /// <summary>Unary handler maps journal capacity to ResourceExhausted.</summary>
    [Fact]
    public async Task UnaryMapsJournalCapacityToRpcResourceExhausted()
    {
        var interceptor = new FrameworkServiceRegistration.ResourceExhaustedExceptionInterceptor();
        var ex = await Assert.ThrowsAsync<RpcException>(() => interceptor.UnaryServerHandler(
            "request",
            new TestServerCallContext(),
            static (_, _) => Task.FromException<string>(new JournalCapacityExceededException())));

        Assert.Equal(StatusCode.ResourceExhausted, ex.StatusCode);
        Assert.Equal(JournalCapacityExceededException.StableDetail, ex.Status.Detail);
    }

    /// <summary>Server-streaming handler maps journal capacity to ResourceExhausted.</summary>
    [Fact]
    public async Task StreamingMapsJournalCapacityToRpcResourceExhausted()
    {
        var interceptor = new FrameworkServiceRegistration.ResourceExhaustedExceptionInterceptor();
        var ex = await Assert.ThrowsAsync<RpcException>(() => interceptor.ServerStreamingServerHandler(
            "request",
            new NullStreamWriter<string>(),
            new TestServerCallContext(),
            static (_, _, _) => throw new JournalCapacityExceededException()));

        Assert.Equal(StatusCode.ResourceExhausted, ex.StatusCode);
        Assert.Equal(JournalCapacityExceededException.StableDetail, ex.Status.Detail);
    }

    private sealed class NullStreamWriter<T> : IServerStreamWriter<T>
    {
        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(T message) => Task.CompletedTask;
    }

    private sealed class TestServerCallContext : ServerCallContext
    {
        protected override string MethodCore => "/Test.Test/Unary";

        protected override string HostCore => "localhost";

        protected override string PeerCore => "ipv4:127.0.0.1:5001";

        protected override DateTime DeadlineCore => DateTime.MaxValue;

        protected override Metadata RequestHeadersCore { get; } = [];

        protected override CancellationToken CancellationTokenCore => CancellationToken.None;

        protected override Metadata ResponseTrailersCore => [];

        protected override Status StatusCore { get; set; } = Status.DefaultSuccess;

        protected override WriteOptions? WriteOptionsCore { get; set; }

        protected override AuthContext AuthContextCore => new(null, []);

        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) =>
            throw new NotSupportedException();

        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => Task.CompletedTask;
    }
}
