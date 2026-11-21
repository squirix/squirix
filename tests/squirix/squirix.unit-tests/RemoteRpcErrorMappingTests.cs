using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Internal;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Squirix.TestKit;
using Squirix.Transport;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Cache;
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
        var failure = new SquirixTransportException(new CallStatus(CacheTransportStatusCode.FailedPrecondition, OperationIdReuseMismatchException.StableDetail));

        var error = await ExecuteFailureAsync<OperationIdReuseMismatchException>(failure, cancellationToken);

        _ = await Assert.That(error.Message).IsEqualTo(OperationIdReuseMismatchException.StableDetail);
        _ = await Assert.That(error.InnerException).IsSameReferenceAs(failure);
    }

    /// <summary>The missing-operation-id mapping retains its original transport cause.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RequiredIdPreservesTransportCause(CancellationToken cancellationToken)
    {
        var failure = new SquirixTransportException(new CallStatus(CacheTransportStatusCode.InvalidArgument, OperationIdRequiredException.StableDetail));

        var error = await ExecuteFailureAsync<OperationIdRequiredException>(failure, cancellationToken);

        _ = await Assert.That(error.Message).IsEqualTo(OperationIdRequiredException.StableDetail);
        _ = await Assert.That(error.InnerException).IsSameReferenceAs(failure);
    }

    /// <summary>Other details and status codes preserve the transport exception and its stack from the call policy.</summary>
    /// <param name="statusCode">The transport status.</param>
    /// <param name="detail">The transport detail.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(CacheTransportStatusCode.FailedPrecondition, "")]
    [Arguments(CacheTransportStatusCode.FailedPrecondition, "unrelated failure")]
    [Arguments(CacheTransportStatusCode.FailedPrecondition, "Type mismatch: expected counter")]
    [Arguments(CacheTransportStatusCode.FailedPrecondition, "Version must be greater than current (current=2, provided=1)")]
    [Arguments(CacheTransportStatusCode.FailedPrecondition, "Operation_id was reused with a different mutation fingerprint.")]
    [Arguments(CacheTransportStatusCode.FailedPrecondition, OperationIdReuseMismatchException.StableDetail + " ")]
    [Arguments(CacheTransportStatusCode.FailedPrecondition, OperationIdReuseMismatchException.StableDetail + " Type mismatch: expected counter")]
    [Arguments(CacheTransportStatusCode.InvalidArgument, OperationIdReuseMismatchException.StableDetail)]
    [Arguments(CacheTransportStatusCode.FailedPrecondition, OperationIdRequiredException.StableDetail)]
    public async Task UnmappedFailurePreservesIdentityAndStack(CacheTransportStatusCode statusCode, string detail, CancellationToken cancellationToken)
    {
        var failure = new SquirixTransportException(new CallStatus(statusCode, detail));

        var error = await ExecuteFailureAsync<SquirixTransportException>(failure, cancellationToken);

        _ = await Assert.That(error).IsSameReferenceAs(failure);
        _ = await Assert.That(error.StatusCode).IsEqualTo(statusCode);
        _ = await Assert.That(error.Status.Detail).IsEqualTo(detail);
        _ = await Assert.That(error.StackTrace).Contains(nameof(CallPolicy));
    }

    private static async Task<TException> ExecuteFailureAsync<TException>(SquirixTransportException failure, CancellationToken cancellationToken)
        where TException : Exception
    {
        await using var policy = new CallPolicy(TimeProvider.System, TimeSpan.FromSeconds(30), 1, TimeSpan.Zero, TimeSpan.Zero, peer: "error-mapping");
        await using var pool = SingleNodeClientPool.Create(new FailureTransport(failure).InvokeAsync, policy);
        var cache = new RemoteCache<string>(
            "demo",
            new EndpointFailover(["node-0"], "node-0", TimeSpan.FromSeconds(30), TimeProvider.System),
            pool,
            RemoteClientSessionFactory.CreateSerializer());
        var operation = new ValueTask<bool>(cache.TryAddAsync("key", "value", cancellationToken: cancellationToken));
        return await AsyncAssert.ThrowsAsync<TException, bool>(operation, TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
    }

    private sealed class FailureTransport
    {
        private readonly SquirixTransportException _failure;

        internal FailureTransport(SquirixTransportException failure)
        {
            _failure = failure;
        }

        internal Func<BinaryOperation, WireMessage, IWireParser, BinaryCallOptions, Task<WireMessage>> InvokeAsync =>
            (_, _, _, _) => ThrowFailureAsync();

        private async Task<WireMessage> ThrowFailureAsync()
        {
            await Task.Yield();
            throw _failure;
        }
    }
}
