using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Rocks;
using Squirix.Attributes;
using Squirix.Internal;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Squirix.TestKit;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Covers the client single-flight around the get-or-add operation.</summary>
[Immutable]
public sealed class RemoteCacheGetOrAddTests
{
    private const string Key = "key-a";

    /// <summary>Ensures a call after a completed get-or-add sends a new request instead of returning the finished result.</summary>
    [Test]
    public async Task SequentialCallsEachReachTransport()
    {
        var transport = new GetOrAddTransport([11, 22]);
        await using var policy = new CallPolicy(TimeProvider.System, TimeSpan.FromSeconds(1), 1, TimeSpan.Zero, TimeSpan.Zero, peer: "get-or-add");
        await using var pool = CreatePool(transport, policy);
        var cache = CreateCache(pool);

        var first = await cache.GetOrAddAsync(Key, static (_, _) => Task.FromResult<long>(1), cancellationToken: CancellationToken.None);
        var second = await cache.GetOrAddAsync(Key, static (_, _) => Task.FromResult<long>(2), cancellationToken: CancellationToken.None);

        _ = await Assert.That(first.Value).IsEqualTo(11L);
        _ = await Assert.That(second.Value).IsEqualTo(22L);
        _ = await Assert.That(transport.OperationIds.Count).IsEqualTo(2);
        _ = await Assert.That(transport.OperationIds[0]).IsNotEqualTo(transport.OperationIds[1]);
    }

    /// <summary>Ensures a value factory that throws synchronously does not block a later get-or-add on the same key.</summary>
    [Test]
    public async Task SyncFactoryFailureDoesNotBlockNext()
    {
        var transport = new GetOrAddTransport([5]);
        await using var policy = new CallPolicy(TimeProvider.System, TimeSpan.FromSeconds(1), 1, TimeSpan.Zero, TimeSpan.Zero, peer: "get-or-add");
        await using var pool = CreatePool(transport, policy);
        var cache = CreateCache(pool);

        var failing = cache.GetOrAddAsync(Key, static (_, _) => throw new InvalidOperationException("factory failed"), cancellationToken: CancellationToken.None);
        _ = await AsyncAssert.ThrowsAsync<InvalidOperationException, bool>(AwaitAsync(failing));
        var next = await cache.GetOrAddAsync(Key, static (_, _) => Task.FromResult<long>(9), cancellationToken: CancellationToken.None);

        _ = await Assert.That(next.Value).IsEqualTo(5L);
        _ = await Assert.That(transport.OperationIds.Count).IsEqualTo(1);
    }

    private static async ValueTask<bool> AwaitAsync(Task<CacheValueResult<long>> operation)
    {
        _ = await operation.ConfigureAwait(false);
        return true;
    }

    private static IClientPool CreatePool(GetOrAddTransport transport, CallPolicy policy)
    {
        var client = new SquirixCacheService.SquirixCacheServiceClient(transport);
        var poolExpectations = new IClientPoolCreateExpectations();
        _ = poolExpectations.Setups.ForNode(Arg.Any<string>()).ReturnValue(client);
        _ = poolExpectations.Setups.PolicyFor(Arg.Any<string>()).ReturnValue(policy);
        _ = poolExpectations.Setups.BeginDrain();
        _ = poolExpectations.Setups.DisposeAsync().ReturnValue(ValueTask.CompletedTask);
        return poolExpectations.Instance();
    }

    private static RemoteCache<long> CreateCache(IClientPool pool) =>
        new("demo", new EndpointFailover(["node-0"], "node-0", TimeSpan.FromSeconds(30), TimeProvider.System), pool, RemoteClientSessionFactory.CreateSerializer());

    private sealed class GetOrAddTransport : CallInvoker
    {
        private readonly Queue<long> _values;

        internal GetOrAddTransport(long[] values)
        {
            _values = new Queue<long>(values);
        }

        internal List<string> OperationIds { get; } = [];

        public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            string? host,
            CallOptions options) => throw new InvalidOperationException("The get-or-add transport supports unary calls only.");

        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            string? host,
            CallOptions options) => throw new InvalidOperationException("The get-or-add transport supports unary calls only.");

        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            string? host,
            CallOptions options,
            TRequest request) => throw new InvalidOperationException("The get-or-add transport supports unary calls only.");

        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
        {
            if (request is not GetOrAddAsyncRequest getOrAdd)
                throw new InvalidOperationException("The get-or-add transport supports get-or-add requests only.");

            OperationIds.Add(getOrAdd.OperationId);
            var response = new GetOrAddAsyncResponse { Found = true, Value = new CacheValue { Int64Value = _values.Dequeue() } };
            return new AsyncUnaryCall<TResponse>(
                Task.FromResult(Unsafe.As<TResponse>(response)),
                Task.FromResult(new Metadata()),
                static () => new Status(StatusCode.OK, string.Empty),
                static () => [],
                static () => { });
        }

        public override TResponse BlockingUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
            throw new InvalidOperationException("The get-or-add transport supports asynchronous calls only.");
    }
}
