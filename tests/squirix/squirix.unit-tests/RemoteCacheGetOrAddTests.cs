using System;
using System.Collections.Generic;
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

    /// <summary>Ensures a caller that cancels stops waiting while the caller that joined it gets the reply of the one request.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledOwnerDoesNotCancelJoiner(CancellationToken cancellationToken)
    {
        var transport = new GetOrAddTransport([11]);
        await using var policy = new CallPolicy(TimeProvider.System, TimeSpan.FromSeconds(1), 1, TimeSpan.Zero, TimeSpan.Zero, peer: "get-or-add");
        await using var pool = CreatePool(transport, policy);
        var cache = CreateCache(pool);
        var factoryGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var owner = new CancellationTokenSource();

        var first = cache.GetOrAddAsync(Key, (_, _) => GatedValueAsync(factoryGate, 1), cancellationToken: owner.Token);
        var second = cache.GetOrAddAsync(Key, static (_, _) => throw new InvalidOperationException("a joiner must not run its factory"), cancellationToken: cancellationToken);
        await owner.CancelAsync();
        var error = await AsyncAssert.ThrowsAnyAsync<OperationCanceledException, CacheValueResult<long>>(new ValueTask<CacheValueResult<long>>(first));
        factoryGate.SetResult();

        _ = await Assert.That((await second.WaitAsync(cancellationToken)).Value).IsEqualTo(11L);
        _ = await Assert.That(error.CancellationToken).IsEqualTo(owner.Token);
        _ = await Assert.That(transport.OperationIds.Count).IsEqualTo(1);
    }

    /// <summary>Ensures a caller that arrives after the request was sent sends its own request with the produced value instead of sharing the reply.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LateCallerSendsOwnRequest(CancellationToken cancellationToken)
    {
        var transport = new GetOrAddTransport([11, 22], true);
        await using var policy = new CallPolicy(TimeProvider.System, TimeSpan.FromSeconds(5), 1, TimeSpan.Zero, TimeSpan.Zero, peer: "get-or-add");
        await using var pool = CreatePool(transport, policy);
        var cache = CreateCache(pool);

        var first = cache.GetOrAddAsync(Key, static (_, _) => Task.FromResult<long>(1), cancellationToken: cancellationToken);
        await transport.Requested.Task.WaitAsync(cancellationToken);
        var second = cache.GetOrAddAsync(Key, static (_, _) => throw new InvalidOperationException("a late caller reuses the produced value"), cancellationToken: cancellationToken);
        transport.ReleaseReplies();

        _ = await Assert.That((await first.WaitAsync(cancellationToken)).Value).IsEqualTo(11L);
        _ = await Assert.That((await second.WaitAsync(cancellationToken)).Value).IsEqualTo(22L);
        _ = await Assert.That(transport.OperationIds.Count).IsEqualTo(2);
        _ = await Assert.That(transport.OperationIds[0]).IsNotEqualTo(transport.OperationIds[1]);
        _ = await Assert.That(transport.SentEntries.Count).IsEqualTo(2);
        _ = await Assert.That(transport.SentEntries[1]).IsEqualTo(transport.SentEntries[0]);
    }

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

    private static RemoteCache<long> CreateCache(IClientPool pool) =>
        new("demo", new EndpointFailover(["node-0"], "node-0", TimeSpan.FromSeconds(30), TimeProvider.System), pool, RemoteClientSessionFactory.CreateSerializer());

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

    private static async Task<long> GatedValueAsync(TaskCompletionSource gate, long value)
    {
        await gate.Task;
        return value;
    }

    private sealed class GetOrAddTransport : CallInvoker
    {
        private readonly Lock _gate = new();
        private readonly bool _holdReplies;
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly Queue<long> _values;

        internal GetOrAddTransport(long[] values, bool holdReplies = false)
        {
            _values = new Queue<long>(values);
            _holdReplies = holdReplies;
        }

        internal List<string> OperationIds { get; } = [];

        internal TaskCompletionSource Requested { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal List<string> SentEntries { get; } = [];

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

            GetOrAddAsyncResponse response;
            lock (_gate)
            {
                OperationIds.Add(getOrAdd.OperationId);
                SentEntries.Add(getOrAdd.Entry.Value.ToString());
                response = new GetOrAddAsyncResponse { Found = true, Value = new CacheValue { Int64Value = _values.Dequeue() } };
            }

            _ = Requested.TrySetResult();
            var typed = KitThrowHelper.Required(response as TResponse, "The get-or-add transport supports get-or-add responses only.");
            return new AsyncUnaryCall<TResponse>(
                _holdReplies ? ReplyAfterReleaseAsync(typed) : Task.FromResult(typed),
                Task.FromResult(new Metadata()),
                static () => new Status(StatusCode.OK, string.Empty),
                static () => [],
                static () => { });
        }

        public override TResponse BlockingUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
            throw new InvalidOperationException("The get-or-add transport supports asynchronous calls only.");

        internal void ReleaseReplies() => _release.SetResult();

        private async Task<TResponse> ReplyAfterReleaseAsync<TResponse>(TResponse response)
        {
            await _release.Task.ConfigureAwait(false);
            return response;
        }
    }
}
