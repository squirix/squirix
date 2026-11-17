using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Squirix.Attributes;
using Squirix.Internal;
using Squirix.Internal.Cluster.Reliability;
using Squirix.TestKit;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Covers ownership transfer from private distributed cache responses.</summary>
[Immutable]
public sealed class DistributedCacheReadTests
{
    /// <summary>Transfers the parsed array, detaches the response and keeps subsequent reads independent.</summary>
    /// <param name="suspend">Whether the first response completes asynchronously.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task GetTransfersParsedPayload(bool suspend)
    {
        byte[] expected = [0, 255, 128, 42];
        var wire = new GetDistributedAsyncResponse { Found = true, Value = ByteString.CopyFrom(expected) }.ToByteArray();
        var first = GetDistributedAsyncResponse.Parser.ParseFrom(wire);
        var second = GetDistributedAsyncResponse.Parser.ParseFrom(wire);
        _ = await Assert.That(MemoryMarshal.TryGetArray(first.Value.Memory, out var parsed)).IsTrue();
        _ = await Assert.That(parsed.Offset).IsEqualTo(0);
        _ = await Assert.That(parsed.Count).IsEqualTo(parsed.Array!.Length);
        var release = new TaskCompletionSource<GetDistributedAsyncResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        var transport = new ResponseTransport(suspend ? release.Task : Task.FromResult(first), Task.FromResult(second));
        await using var policy = new CallPolicy(TimeProvider.System, TimeSpan.FromSeconds(30));
        await using var cache = CreateCache(transport, policy);

        var operation = cache.GetAsync("key", CancellationToken.None);
        _ = await Assert.That(operation.IsCompleted).IsEqualTo(!suspend);
        if (suspend)
            _ = release.TrySetResult(first);
        var result = await operation.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, CancellationToken.None);

        _ = await Assert.That(result).IsSameReferenceAs(parsed.Array);
        _ = await Assert.That(result.AsSpan().SequenceEqual(expected)).IsTrue();
        _ = await Assert.That(first.Value).IsEqualTo(ByteString.Empty);
        result![0] = 99;
        var next = await cache.GetAsync("key", CancellationToken.None);
        _ = await Assert.That(ReferenceEquals(result, next)).IsFalse();
        _ = await Assert.That(next.AsSpan().SequenceEqual(expected)).IsTrue();
        _ = await Assert.That(GetDistributedAsyncResponse.Parser.ParseFrom(wire).Value.Span.SequenceEqual(expected)).IsTrue();
    }

    /// <summary>Preserves misses and found entries containing no bytes.</summary>
    /// <param name="found">Whether the response represents a found empty value.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task GetPreservesEmptyAndMissingValues(bool found)
    {
        var response = new GetDistributedAsyncResponse { Found = found, Value = found ? ByteString.Empty : ByteString.CopyFrom(1, 2, 3) };
        await using var policy = new CallPolicy(TimeProvider.System);
        await using var cache = CreateCache(new ResponseTransport(Task.FromResult(response)), policy);

        var result = await cache.GetAsync("key", CancellationToken.None);

        if (found)
            _ = await Assert.That(result is { Length: 0 }).IsTrue();
        else
            _ = await Assert.That(result).IsNull();
    }

    [SuppressMessage("Reliability", "CA2000", Justification = "The returned cache owns the session, which owns and disposes the client pool.")]
    private static RemoteDistributedCache CreateCache(CallInvoker transport, CallPolicy policy)
    {
        var failover = new EndpointFailover(["node"], "node", TimeSpan.FromSeconds(30), TimeProvider.System);
        var session = new RemoteClientSession(SingleNodeClientPool.Create(transport, policy), failover, RemoteClientSessionFactory.CreateSerializer());
        return session.OpenDistributedCache("test");
    }

    /// <summary>Supplies the private responses owned by successive unary reads.</summary>
    /// <remarks>The hand-written invoker scripts generic unary responses, which a Rocks mock cannot express.</remarks>
    private sealed class ResponseTransport : CallInvoker
    {
        private readonly Task<GetDistributedAsyncResponse>[] _responses;
        private int _index = -1;

        internal ResponseTransport(params Task<GetDistributedAsyncResponse>[] responses)
        {
            _responses = responses;
        }

        public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options) =>
            throw new InvalidOperationException("Only unary reads are supported.");

        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options) =>
            throw new InvalidOperationException("Only unary reads are supported.");

        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
            throw new InvalidOperationException("Only unary reads are supported.");

        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) => new(
            ReadResponseAsync<TResponse>(_responses[Interlocked.Increment(ref _index)], options.CancellationToken),
            Task.FromResult(new Metadata()),
            static () => new Status(StatusCode.OK, string.Empty),
            static () => [],
            static () => { });

        public override TResponse BlockingUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
            throw new InvalidOperationException("Only asynchronous reads are supported.");

        private static async Task<TResponse> ReadResponseAsync<TResponse>(Task<GetDistributedAsyncResponse> response, CancellationToken cancellationToken)
            where TResponse : class => KitThrowHelper.Required(await response.WaitAsync(cancellationToken).ConfigureAwait(false) as TResponse, "Unexpected response type.");
    }
}
