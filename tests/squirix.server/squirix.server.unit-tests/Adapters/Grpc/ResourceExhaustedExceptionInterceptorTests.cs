using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Adapters.Endpoint.Grpc;
using Squirix.Server.Errors;
using Xunit;

namespace Squirix.Server.UnitTests.Adapters.Grpc;

/// <summary>
/// Focused behavior tests for <see cref="ResourceExhaustedExceptionInterceptor" />.
/// </summary>
public sealed class ResourceExhaustedExceptionInterceptorTests
{
    /// <summary>
    /// Verifies non-squirix exceptions in streaming path pass through unchanged.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task ServerStreamingNonSquirixExceptionPassesThrough()
    {
        var interceptor = new ResourceExhaustedExceptionInterceptor();

        _ = await Assert.ThrowsAsync<InvalidOperationException>(() => interceptor.ServerStreamingServerHandler(
            "request",
            new TestServerStreamWriter<string>(),
            new TestServerCallContext(),
            static (_, _, _) => throw new InvalidOperationException("boom")));
    }

    /// <summary>
    /// Verifies streaming resource exhaustion maps to gRPC ResourceExhausted.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task ServerStreamingResourceExhaustedMapsToRpcResourceExhausted()
    {
        var interceptor = new ResourceExhaustedExceptionInterceptor();

        var ex = await Assert.ThrowsAsync<RpcException>(() => interceptor.ServerStreamingServerHandler(
            "request",
            new TestServerStreamWriter<string>(),
            new TestServerCallContext(),
            static (_, _, _) => throw new ResourceExhaustedException()));

        Assert.Equal(StatusCode.ResourceExhausted, ex.StatusCode);
        Assert.Equal(ResourceExhaustedException.StableDetail, ex.Status.Detail);
    }

    /// <summary>
    /// Verifies successful server-streaming handlers pass through without remapping.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task ServerStreamingSuccessfulCallPassesThrough()
    {
        var interceptor = new ResourceExhaustedExceptionInterceptor();
        var invoked = false;

        await interceptor.ServerStreamingServerHandler(
            "request",
            new TestServerStreamWriter<string>(),
            new TestServerCallContext(),
            (request, _, _) =>
            {
                invoked = request == "request";
                return Task.CompletedTask;
            });

        Assert.True(invoked);
    }

    /// <summary>
    /// Verifies cancellation is not masked as resource exhaustion.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task UnaryOperationCanceledPassesThrough()
    {
        var interceptor = new ResourceExhaustedExceptionInterceptor();

        _ = await Assert.ThrowsAsync<OperationCanceledException>(() => interceptor.UnaryServerHandler(
            "request",
            new TestServerCallContext(),
            static (_, _) => Task.FromException<string>(new OperationCanceledException())));
    }

    /// <summary>
    /// Verifies <see cref="ResourceExhaustedException" /> maps to gRPC ResourceExhausted with stable diagnostic detail.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task UnaryResourceExhaustedMapsToRpcResourceExhausted()
    {
        var interceptor = new ResourceExhaustedExceptionInterceptor();

        var ex = await Assert.ThrowsAsync<RpcException>(() => interceptor.UnaryServerHandler(
            "request",
            new TestServerCallContext(),
            static (_, _) => Task.FromException<string>(new ResourceExhaustedException())));

        Assert.Equal(StatusCode.ResourceExhausted, ex.StatusCode);
        Assert.Equal(ResourceExhaustedException.StableDetail, ex.Status.Detail);
    }

    /// <summary>
    /// Verifies existing <see cref="RpcException" /> is not wrapped again.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task UnaryRpcExceptionPassesThroughUnchanged()
    {
        var interceptor = new ResourceExhaustedExceptionInterceptor();
        var rpc = new RpcException(new Status(StatusCode.Unavailable, "peer unavailable"));

        var ex = await Assert.ThrowsAsync<RpcException>(() => interceptor.UnaryServerHandler("request", new TestServerCallContext(), (_, _) => Task.FromException<string>(rpc)));

        Assert.Same(rpc, ex);
    }

    /// <summary>
    /// Verifies non-resource <see cref="SquirixException" /> maps through the shared deterministic error mapper.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task UnarySquirixExceptionMapsToMappedRpcStatus()
    {
        var interceptor = new ResourceExhaustedExceptionInterceptor();

        var ex = await Assert.ThrowsAsync<RpcException>(() => interceptor.UnaryServerHandler(
            "request",
            new TestServerCallContext(),
            static (_, _) => Task.FromException<string>(new SquirixException(SquirixErrorCode.Conflict, "Conflict", "version conflict"))));

        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
        Assert.Equal("version conflict", ex.Status.Detail);
    }

    /// <summary>
    /// Verifies successful unary responses are passed through unchanged.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task UnarySuccessfulResponsePassesThrough()
    {
        var interceptor = new ResourceExhaustedExceptionInterceptor();

        var response = await interceptor.UnaryServerHandler("request", new TestServerCallContext(), static (_, _) => Task.FromResult("ok"));

        Assert.Equal("ok", response);
    }

    /// <summary>
    /// Verifies synchronous throw in unary handler body is still mapped.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task UnarySynchronousThrowBeforeAwaitIsMapped()
    {
        var interceptor = new ResourceExhaustedExceptionInterceptor();

        var ex = await Assert.ThrowsAsync<RpcException>(() => interceptor.UnaryServerHandler(
            "request",
            new TestServerCallContext(),
            static (_, _) => Task.FromException<string>(new ResourceExhaustedException())));

        Assert.Equal(StatusCode.ResourceExhausted, ex.StatusCode);
    }

    private sealed class TestServerCallContext : ServerCallContext
    {
        protected override AuthContext AuthContextCore => new(string.Empty, []);

        protected override CancellationToken CancellationTokenCore => CancellationToken.None;

        protected override DateTime DeadlineCore => DateTime.MaxValue;

        protected override string HostCore => "localhost";

        protected override string MethodCore => "/squirix.tests/Test";

        protected override string PeerCore => "ipv4:127.0.0.1:5001";

        protected override Metadata RequestHeadersCore => [];

        protected override Metadata ResponseTrailersCore => [];

        protected override Status StatusCore { get; set; } = Status.DefaultSuccess;

        protected override WriteOptions? WriteOptionsCore { get; set; }

        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => throw new NotSupportedException();

        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => Task.CompletedTask;
    }

    private sealed class TestServerStreamWriter<T> : IServerStreamWriter<T>
    {
        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(T message) => Task.CompletedTask;
    }
}
