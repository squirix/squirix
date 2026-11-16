using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Attributes;
using Squirix.Internal;
using Squirix.Internal.Cluster.Reliability;
using Squirix.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Checks error mapping at the remote cache boundary without a network connection.</summary>
[Immutable]
public sealed class RemoteRpcErrorMappingTests
{
    /// <summary>The exact reuse detail maps to the public exception and retains the transport cause.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReuseMismatchPreservesTransportCause(CancellationToken cancellationToken)
    {
        var failure = new RpcException(new Status(StatusCode.FailedPrecondition, OperationIdReuseMismatchException.StableDetail));

        var error = await ExecuteFailureAsync<OperationIdReuseMismatchException>(failure, cancellationToken);

        _ = await Assert.That(error.Message).IsEqualTo(OperationIdReuseMismatchException.StableDetail);
        _ = await Assert.That(error.InnerException).IsSameReferenceAs(failure);
    }

    /// <summary>The missing-operation-id mapping retains its original transport cause.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RequiredIdPreservesTransportCause(CancellationToken cancellationToken)
    {
        var failure = new RpcException(new Status(StatusCode.InvalidArgument, OperationIdRequiredException.StableDetail));

        var error = await ExecuteFailureAsync<OperationIdRequiredException>(failure, cancellationToken);

        _ = await Assert.That(error.Message).IsEqualTo(OperationIdRequiredException.StableDetail);
        _ = await Assert.That(error.InnerException).IsSameReferenceAs(failure);
    }

    /// <summary>Other details and status codes preserve the transport exception and its stack from the call policy.</summary>
    /// <param name="statusCode">The transport status.</param>
    /// <param name="detail">The transport detail.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(StatusCode.FailedPrecondition, "")]
    [Arguments(StatusCode.FailedPrecondition, "unrelated failure")]
    [Arguments(StatusCode.FailedPrecondition, "Type mismatch: expected counter")]
    [Arguments(StatusCode.FailedPrecondition, "Version must be greater than current (current=2, provided=1)")]
    [Arguments(StatusCode.FailedPrecondition, "Operation_id was reused with a different mutation fingerprint.")]
    [Arguments(StatusCode.FailedPrecondition, OperationIdReuseMismatchException.StableDetail + " ")]
    [Arguments(StatusCode.FailedPrecondition, OperationIdReuseMismatchException.StableDetail + " Type mismatch: expected counter")]
    [Arguments(StatusCode.InvalidArgument, OperationIdReuseMismatchException.StableDetail)]
    [Arguments(StatusCode.FailedPrecondition, OperationIdRequiredException.StableDetail)]
    public async Task UnmappedFailurePreservesIdentityAndStack(StatusCode statusCode, string detail, CancellationToken cancellationToken)
    {
        var failure = new RpcException(new Status(statusCode, detail));

        var error = await ExecuteFailureAsync<RpcException>(failure, cancellationToken);

        _ = await Assert.That(error).IsSameReferenceAs(failure);
        _ = await Assert.That(error.StatusCode).IsEqualTo(statusCode);
        _ = await Assert.That(error.Status.Detail).IsEqualTo(detail);
        _ = await Assert.That(error.StackTrace).Contains(nameof(CallPolicy));
    }

    private static async Task<TException> ExecuteFailureAsync<TException>(RpcException failure, CancellationToken cancellationToken)
        where TException : Exception
    {
        await using var policy = new CallPolicy(TimeProvider.System, TimeSpan.FromSeconds(30), 1, TimeSpan.Zero, TimeSpan.Zero, peer: "error-mapping");
        await using var pool = SingleNodeClientPool.Create(new FailureTransport(failure), policy);
        var cache = new RemoteCache<string>(
            "demo",
            new EndpointFailover(["node-0"], "node-0", TimeSpan.FromSeconds(30), TimeProvider.System),
            pool,
            RemoteClientSessionFactory.CreateSerializer());
        var operation = new ValueTask<bool>(cache.TryAddAsync("key", "value", cancellationToken: cancellationToken));
        return await AsyncAssert.ThrowsAsync<TException, bool>(operation, TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
    }

    private sealed class FailureTransport : CallInvoker
    {
        private readonly RpcException _failure;

        internal FailureTransport(RpcException failure)
        {
            _failure = failure;
        }

        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) => new(
            ThrowFailureAsync<TResponse>(),
            Task.FromResult(new Metadata()),
            static () => new Status(StatusCode.OK, string.Empty),
            static () => [],
            static () => { });

        public override TResponse BlockingUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) => throw new NotSupportedException();

        public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options) => throw new NotSupportedException();

        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options) => throw new NotSupportedException();

        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) => throw new NotSupportedException();

        internal async Task<TResponse> ThrowFailureAsync<TResponse>()
        {
            await Task.Yield();
            throw _failure;
        }
    }
}
