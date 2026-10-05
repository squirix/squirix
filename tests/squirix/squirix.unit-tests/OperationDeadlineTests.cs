using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Time.Testing;
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

/// <summary>Covers the absolute operation deadline on the production client call path.</summary>
[Immutable]
public sealed class OperationDeadlineTests
{
    private static readonly TimeSpan ShortDeadline = TimeSpan.FromMilliseconds(400);
    private static readonly TimeSpan LongPerAttempt = TimeSpan.FromDays(40);
    private static readonly TimeSpan LongDeadline = TimeSpan.FromDays(41);
    private static readonly DateTimeOffset ClockStart = new(2000, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan CompletionBound = TimeSpan.FromSeconds(5);

    private enum TransportMode
    {
        Hang = 0,
        Unavailable = 1,
        Succeed = 2,
        GrpcDeadline = 3,
    }

    /// <summary>A caller token that is never cancelled does not lift the deadline.</summary>
    [Test]
    public async Task CallerTokenStillBoundedByDeadline()
    {
        using var callerSource = new CancellationTokenSource();
        await using var harness = new Harness("deadline-token", ShortDeadline, TransportMode.Hang, TransportMode.Hang);

        var operation = GetAsync(harness.Cache, callerSource.Token);
        _ = await Assert.That(await harness.FirstTransport.WaitForCallAsync()).IsTrue();
        var pending = !operation.IsCompleted;
        harness.Clock.Advance(ShortDeadline);
        var error = await AsyncAssert.ThrowsAsync<RpcException, bool>(operation);

        _ = await Assert.That(pending).IsTrue();
        _ = await Assert.That(error.StatusCode).IsEqualTo(StatusCode.DeadlineExceeded);
    }

    /// <summary>Every exported cache operation sends the operation deadline as an absolute UTC call deadline.</summary>
    [Test]
    public async Task EveryOperationSendsUtcDeadline()
    {
        Func<ICache<string>, CancellationToken, ValueTask>[] operations =
        [
            static (cache, ct) => new ValueTask(cache.AddAsync("key-a", "value", null, ct)),
            static (cache, ct) => new ValueTask(cache.GetEntryAsync("key-a", ct)),
            static (cache, ct) => new ValueTask(cache.GetExpirationAsync("key-a", ct)),
            static (cache, ct) => new ValueTask(cache.GetOrAddAsync("key-a", static (_, _) => Task.FromResult<string?>("value"), null, ct)),
            static (cache, ct) => new ValueTask(cache.GetValueAsync("key-a", ct)),
            static (cache, ct) => new ValueTask(cache.RemoveAsync("key-a", ct)),
            static (cache, ct) => new ValueTask(cache.RemoveExpirationAsync("key-a", ct)),
            static (cache, ct) => new ValueTask(cache.SetAsync("key-a", "value", null, ct)),
            static (cache, ct) => new ValueTask(cache.TouchAsync("key-a", TimeSpan.FromMinutes(1), ct)),
            static (cache, ct) => new ValueTask(cache.TouchAsync("key-a", DateTimeOffset.UtcNow.AddMinutes(1), ct)),
            static (cache, ct) => new ValueTask(cache.TryAddAsync("key-a", "value", null, ct)),
            static (cache, ct) => new ValueTask(cache.UpdateAsync("key-a", "value", ct)),
        ];

        foreach (var operation in operations)
        {
            await using var harness = new Harness("deadline-every-op", ShortDeadline, TransportMode.Succeed, TransportMode.Succeed);

            try
            {
                await operation(harness.Cache, CancellationToken.None);
            }
            catch (CacheConflictException)
            {
                // The scripted empty response may map to a conflict; only the captured call options matter here.
            }

            var captured = harness.AllCalls();
            _ = await Assert.That(captured.Count).IsGreaterThan(0);
            foreach (var call in captured)
            {
                _ = await Assert.That(call.Deadline).IsNotNull();
                _ = await Assert.That(call.Deadline!.Value.Kind).IsEqualTo(DateTimeKind.Utc);
            }
        }
    }

    /// <summary>Caller cancellation is still reported as cancellation, not as a deadline failure.</summary>
    [Test]
    public async Task CallerCancelSurfacesOperationCanceled()
    {
        using var callerSource = new CancellationTokenSource();
        await using var harness = new Harness("deadline-cancel", LongDeadline, TransportMode.Hang, TransportMode.Hang);

        var operation = GetAsync(harness.Cache, callerSource.Token);
        _ = await Assert.That(await harness.FirstTransport.WaitForCallAsync()).IsTrue();
        await callerSource.CancelAsync();

        _ = await AsyncAssert.ThrowsAnyAsync<OperationCanceledException, bool>(operation);
    }

    /// <summary>The deadline budget metric is recorded when the deadline ends a call.</summary>
    [Test]
    public async Task DeadlineBudgetMetricIsRecorded()
    {
        var peer = $"deadline-metric-{Guid.NewGuid():N}";
        using var sink = new MeasurementSink("Squirix");
        await using var harness = new Harness(peer, ShortDeadline, TransportMode.Hang, TransportMode.Hang);

        var operation = GetAsync(harness.Cache, CancellationToken.None);
        _ = await Assert.That(await harness.FirstTransport.WaitForCallAsync()).IsTrue();
        harness.Clock.Advance(ShortDeadline);
        _ = await AsyncAssert.ThrowsAsync<RpcException, bool>(operation);

        _ = await Assert.That(sink.HasEvent("squirix_rpc_timeouts_total", ("peer", peer), ("kind", "deadline_budget"))).IsTrue();
    }

    /// <summary>A gRPC deadline expiry that coincides with the budget is reported as budget expiry, not retried.</summary>
    [Test]
    public async Task GrpcDeadlineFirstRecordsBudgetMetric()
    {
        var peer = $"deadline-grpc-{Guid.NewGuid():N}";
        using var sink = new MeasurementSink("Squirix");
        await using var harness = new Harness(peer, ShortDeadline, TransportMode.GrpcDeadline, TransportMode.GrpcDeadline, 64, true);

        var error = await AsyncAssert.ThrowsAsync<RpcException, bool>(GetAsync(harness.Cache, CancellationToken.None));

        _ = await Assert.That(error.StatusCode).IsEqualTo(StatusCode.DeadlineExceeded);
        _ = await Assert.That(error.Status.Detail).IsEqualTo("Request deadline exceeded.");
        _ = await Assert.That(harness.FirstTransport.Calls.Count).IsEqualTo(1);
        _ = await Assert.That(sink.HasEvent("squirix_rpc_timeouts_total", ("peer", peer), ("kind", "deadline_budget"))).IsTrue();
        _ = await Assert.That(sink.HasEvent("squirix_rpc_timeouts_total", ("peer", peer), ("kind", "deadline_exceeded"))).IsFalse();
    }

    /// <summary>The same absolute UTC deadline is sent on every attempt and endpoint.</summary>
    [Test]
    public async Task DeadlineReachesCallOptionsAsUtc()
    {
        await using var harness = new Harness("deadline-options", ShortDeadline, TransportMode.Unavailable, TransportMode.Succeed);

        var expectedDeadline = harness.Clock.GetUtcNow().UtcDateTime + ShortDeadline;
        _ = await GetAsync(harness.Cache, CancellationToken.None);

        var captured = harness.AllCalls();
        _ = await Assert.That(captured.Count).IsGreaterThan(1);
        var first = captured[0].Deadline;
        _ = await Assert.That(first).IsNotNull();
        _ = await Assert.That(first!.Value.Kind).IsEqualTo(DateTimeKind.Utc);

        // Each attempt rebuilds the gRPC deadline from the shared budget; on a frozen clock every attempt sends exactly the operation deadline.
        foreach (var call in captured)
            _ = await Assert.That(call.Deadline).IsEqualTo(expectedDeadline);
    }

    /// <summary>Two hung endpoints end with a deadline failure far below the per-attempt timeout.</summary>
    [Test]
    public async Task HungEndpointsFailWithDeadlineExceeded()
    {
        await using var harness = new Harness("deadline-hung", ShortDeadline, TransportMode.Hang, TransportMode.Hang);

        var operation = GetAsync(harness.Cache, CancellationToken.None);
        _ = await Assert.That(await harness.FirstTransport.WaitForCallAsync()).IsTrue();
        var pending = !operation.IsCompleted;
        harness.Clock.Advance(ShortDeadline);
        var error = await AsyncAssert.ThrowsAsync<RpcException, bool>(operation);

        _ = await Assert.That(pending).IsTrue();
        _ = await Assert.That(error.StatusCode).IsEqualTo(StatusCode.DeadlineExceeded);
    }

    /// <summary>A mutation keeps one operation id across retries and endpoint failover.</summary>
    [Test]
    public async Task OperationIdStableAcrossRetries()
    {
        await using var harness = new Harness("deadline-opid", ShortDeadline, TransportMode.Unavailable, TransportMode.Succeed);

        await harness.Cache.SetAsync("key-a", "value", cancellationToken: CancellationToken.None);

        var captured = harness.AllCalls();
        _ = await Assert.That(captured.Count).IsGreaterThan(1);
        _ = await Assert.That(string.IsNullOrEmpty(captured[0].OperationId)).IsFalse();
        foreach (var call in captured)
            _ = await Assert.That(call.OperationId).IsEqualTo(captured[0].OperationId);
    }

    /// <summary>A call queued on a saturated peer fails with the deadline, not a raw cancellation.</summary>
    [Test]
    public async Task QueuedCallExpiresAsDeadlineExceeded()
    {
        using var holderSource = new CancellationTokenSource();
        await using var harness = new Harness("deadline-queued", LongDeadline, TransportMode.Hang, TransportMode.Hang, 1, true);
        var queuedCache = harness.CreateCache(ShortDeadline);

        var holder = GetAsync(harness.Cache, holderSource.Token);
        _ = await Assert.That(await harness.FirstTransport.WaitForCallAsync()).IsTrue();
        var callsBeforeQueued = harness.FirstTransport.Calls.Count;

        // The queued call takes its deadline when it starts, so advance until it expires; the holder budget is far longer.
        // The real-time pause only paces the polling and never decides the outcome.
        var queued = GetAsync(queuedCache, CancellationToken.None);
        var guard = TimeProvider.System.GetTimestamp();
        while (!queued.IsCompleted && TimeProvider.System.GetElapsedTime(guard) < CompletionBound)
        {
            harness.Clock.Advance(ShortDeadline);
            await Task.Delay(TimeSpan.FromMilliseconds(1), TimeProvider.System, CancellationToken.None);
        }

        var error = await AsyncAssert.ThrowsAsync<RpcException, bool>(queued);

        await holderSource.CancelAsync();
        _ = await AsyncAssert.ThrowsAnyAsync<OperationCanceledException, bool>(holder);

        _ = await Assert.That(error.StatusCode).IsEqualTo(StatusCode.DeadlineExceeded);
        _ = await Assert.That(error.Status.Detail).IsEqualTo("Request deadline exceeded.");
        _ = await Assert.That(harness.FirstTransport.Calls.Count).IsEqualTo(callsBeforeQueued);
    }

    /// <summary>A caller token cancelled while queued on the peer semaphore is cancellation, not a deadline failure.</summary>
    [Test]
    public async Task CallerCancelWhileQueuedIsCancellation()
    {
        var peer = $"deadline-queued-cancel-{Guid.NewGuid():N}";
        using var sink = new MeasurementSink("Squirix");
        using var holderSource = new CancellationTokenSource();
        using var queuedSource = new CancellationTokenSource();
        await using var harness = new Harness(peer, LongDeadline, TransportMode.Hang, TransportMode.Hang, 1, true);

        var holder = GetAsync(harness.Cache, holderSource.Token);
        _ = await Assert.That(await harness.FirstTransport.WaitForCallAsync()).IsTrue();
        var callsBeforeQueued = harness.FirstTransport.Calls.Count;

        var queued = GetAsync(harness.Cache, queuedSource.Token);
        await queuedSource.CancelAsync();
        _ = await AsyncAssert.ThrowsAnyAsync<OperationCanceledException, bool>(queued);

        await holderSource.CancelAsync();
        _ = await AsyncAssert.ThrowsAnyAsync<OperationCanceledException, bool>(holder);

        _ = await Assert.That(harness.FirstTransport.Calls.Count).IsEqualTo(callsBeforeQueued);
        _ = await Assert.That(sink.HasEvent("squirix_rpc_timeouts_total", ("peer", peer), ("kind", "deadline_budget"))).IsFalse();
    }

    private static async ValueTask<bool> GetAsync(RemoteCache<string> cache, CancellationToken cancellationToken)
    {
        _ = await cache.GetValueAsync("key-a", cancellationToken).ConfigureAwait(false);
        return true;
    }

    private sealed record CapturedCall(DateTime? Deadline, string OperationId);

    private sealed class Harness : IAsyncDisposable
    {
        private readonly IClientPool _pool;
        private readonly CallPolicy _policy;

        internal Harness(string peer, TimeSpan deadline, TransportMode first, TransportMode second, int maxConcurrentPerPeer = 64, bool singleEndpoint = false)
        {
            Clock = new FakeTimeProvider(ClockStart);
            FirstTransport = new ScriptedTransport(first, Clock);
            SecondTransport = new ScriptedTransport(second, Clock);
            _policy = new CallPolicy(LongPerAttempt, 3, TimeSpan.Zero, TimeSpan.Zero, maxConcurrentPerPeer, peer, Clock);
            SingleEndpoint = singleEndpoint;
            var firstClient = new SquirixCacheService.SquirixCacheServiceClient(FirstTransport);
            var secondClient = new SquirixCacheService.SquirixCacheServiceClient(SecondTransport);
            var expectations = new IClientPoolCreateExpectations();
            _ = expectations.Setups.ForNode(Arg.Any<string>()).Callback(nodeId => string.Equals(nodeId, "node-0", StringComparison.Ordinal) ? firstClient : secondClient);
            _ = expectations.Setups.PolicyFor(Arg.Any<string>()).ReturnValue(_policy);
            _ = expectations.Setups.BeginDrain();
            _ = expectations.Setups.DisposeAsync().ReturnValue(ValueTask.CompletedTask);
            _pool = expectations.Instance();
            Cache = CreateCache(deadline);
        }

        internal RemoteCache<string> Cache { get; }

        internal FakeTimeProvider Clock { get; }

        internal ScriptedTransport FirstTransport { get; }

        private bool SingleEndpoint { get; }

        private ScriptedTransport SecondTransport { get; }

        public async ValueTask DisposeAsync()
        {
            await _policy.DisposeAsync().ConfigureAwait(false);
            await _pool.DisposeAsync().ConfigureAwait(false);
            FirstTransport.Dispose();
            SecondTransport.Dispose();
        }

        internal List<CapturedCall> AllCalls() => [.. FirstTransport.Calls, .. SecondTransport.Calls];

        internal RemoteCache<string> CreateCache(TimeSpan deadline)
        {
            string[] nodes = SingleEndpoint ? ["node-0"] : ["node-0", "node-1"];
            return new RemoteCache<string>("demo", new EndpointFailover(nodes, "node-0", deadline, Clock), _pool, RemoteClientSessionFactory.CreateSerializer());
        }
    }

    private sealed class ScriptedTransport : CallInvoker, IDisposable
    {
        private readonly ConcurrentQueue<CapturedCall> _calls = new();
        private readonly FakeTimeProvider _clock;
        private readonly TransportMode _mode;
        private readonly SemaphoreSlim _firstCall = new(0);

        internal ScriptedTransport(TransportMode mode, FakeTimeProvider clock)
        {
            _mode = mode;
            _clock = clock;
        }

        internal List<CapturedCall> Calls => [.. _calls];

        public void Dispose() => _firstCall.Dispose();

        public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            string? host,
            CallOptions options) => throw new InvalidOperationException("The scripted transport supports unary calls only.");

        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            string? host,
            CallOptions options) => throw new InvalidOperationException("The scripted transport supports unary calls only.");

        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            string? host,
            CallOptions options,
            TRequest request) => throw new InvalidOperationException("The scripted transport supports unary calls only.");

        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
        {
            _calls.Enqueue(new CapturedCall(options.Deadline, ExtractOperationId(request)));
            _ = _firstCall.Release();

            var completion = new TaskCompletionSource<TResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
            switch (_mode)
            {
                case TransportMode.Hang:
                    _ = options.CancellationToken.Register(() => completion.TrySetCanceled(options.CancellationToken));
                    break;
                case TransportMode.GrpcDeadline:
                    FailAtDeadline(completion, options.Deadline!.Value);
                    break;
                case TransportMode.Unavailable:
                    completion.SetException(new RpcException(new Status(StatusCode.Unavailable, "endpoint down")));
                    break;
                case TransportMode.Succeed:
                    completion.SetResult(method.ResponseMarshaller.ContextualDeserializer(new EmptyDeserializationContext()));
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(method), _mode, "Unsupported transport mode.");
            }

            return new AsyncUnaryCall<TResponse>(
                completion.Task,
                Task.FromResult(new Metadata()),
                static () => new Status(StatusCode.OK, string.Empty),
                static () => [],
                static () => { });
        }

        public override TResponse BlockingUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
            throw new InvalidOperationException("The scripted transport supports asynchronous calls only.");

        internal Task<bool> WaitForCallAsync() => _firstCall.WaitAsync(CompletionBound, CancellationToken.None);

        private static string ExtractOperationId<TRequest>(TRequest request) => request switch
        {
            SetEntryAsyncRequest set => set.OperationId,
            _ => string.Empty,
        };

        private void FailAtDeadline<TResponse>(TaskCompletionSource<TResponse> completion, DateTime deadlineUtc)
        {
            // Like Grpc.Net.Client, fail only once the wall clock reached the deadline: move the fake clock there first.
            var remaining = deadlineUtc - _clock.GetUtcNow().UtcDateTime;
            if (remaining > TimeSpan.Zero)
                _clock.Advance(remaining);

            _ = completion.TrySetException(new RpcException(new Status(StatusCode.DeadlineExceeded, "Deadline Exceeded")));
        }
    }

    private sealed class EmptyDeserializationContext : DeserializationContext
    {
        public override int PayloadLength => 0;

        public override byte[] PayloadAsNewBuffer() => [];

        public override ReadOnlySequence<byte> PayloadAsReadOnlySequence() => ReadOnlySequence<byte>.Empty;
    }
}
