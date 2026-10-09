using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Rocks;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Errors;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Adapters.Grpc.Replication;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Grpc;

/// <summary>A forward is reported as unreachable only when none of the attempts its call policy made connected to the owner.</summary>
public sealed class ForwardAttemptsTests : DisposableServerUnitTestBase
{
    private const string Owner = "node-b";

    private readonly Meter _testMeter = new("test-forward-attempts");

    /// <summary>
    /// An attempt that may have reached the owner, followed by attempts that failed to connect, leaves the forward ambiguous: the last failure is
    /// a failed connect, yet the forward is not reported as unreachable.
    /// </summary>
    /// <param name="raw">Whether the transport failures surface raw instead of as the client status that carries them.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task EarlierSentAttemptIsNotUnreachable(bool raw, CancellationToken cancellationToken)
    {
        var attempt = 0;
        var invoker = new CapturingCallInvoker(failure: () =>
        {
            var cause = ++attempt == 1
                ? new HttpRequestException(HttpRequestError.ResponseEnded, "the response ended")
                : new HttpRequestException(HttpRequestError.ConnectionError, "connection refused");
            return raw ? cause : new RpcException(new Status(StatusCode.Unavailable, "start failed", cause));
        });
        var forwarder = CreateForwarder(invoker);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(forwarder.GetValueAsync(Owner, new GetValueAsyncRequest { CacheName = "c", Key = "k" }, cancellationToken));

        _ = await Assert.That(invoker.Requests.Count).IsEqualTo(3);
        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(failure.Status.Detail).IsNotEqualTo(ServerOpContract.OwnerUnreachableDetail);
        _ = await Assert.That(OwnerUnreachableFailure.IsLocal(failure)).IsFalse();
    }

    /// <summary>Every attempt failing to connect reports the forward as unreachable.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EveryAttemptRefusedIsUnreachable(CancellationToken cancellationToken)
    {
        var invoker = new CapturingCallInvoker(failure: static () => new HttpRequestException(HttpRequestError.ConnectionError, "connection refused"));
        var forwarder = CreateForwarder(invoker);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(forwarder.GetValueAsync(Owner, new GetValueAsyncRequest { CacheName = "c", Key = "k" }, cancellationToken));

        _ = await Assert.That(invoker.Requests.Count).IsEqualTo(3);
        _ = await Assert.That(OwnerUnreachableFailure.IsLocal(failure)).IsTrue();
    }

    /// <summary>A forward whose attempt was canceled, as by its per-attempt timeout, reaches the caller as a timeout and never as unreachable.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledAttemptIsATimeout(CancellationToken cancellationToken)
    {
        var forwarder = CreateForwarder(new CapturingCallInvoker(failure: static () => new OperationCanceledException("attempt canceled")));

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(forwarder.GetValueAsync(Owner, new GetValueAsyncRequest { CacheName = "c", Key = "k" }, cancellationToken));

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.DeadlineExceeded);
        _ = await Assert.That(OwnerUnreachableFailure.IsLocal(failure)).IsFalse();
    }

    /// <summary>
    /// A forward whose every connect to the owner timed out, as towards a host that drops connection attempts, is reported as unreachable:
    /// the connect timeout of the pool ends each attempt before its per-attempt timeout, and no attempt sent anything.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConnectTimeoutIsUnreachable(CancellationToken cancellationToken)
    {
        await using var pool = CreatePool(
            static () => new SocketsHttpHandler { ConnectCallback = static (_, ct) => NeverConnectsAsync(ct) },
            new Uri("https://localhost:6500"),
            TimeSpan.FromMilliseconds(50));
        var forwarder = CreateForwarder(pool, CreatePolicy(TimeSpan.FromSeconds(30)));

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(forwarder.GetValueAsync(Owner, new GetValueAsyncRequest { CacheName = "c", Key = "k" }, cancellationToken));

        _ = await Assert.That(OwnerUnreachableFailure.IsLocal(failure)).IsTrue();
        _ = await Assert.That(HasTimeoutCause(failure.Status.DebugException)).IsTrue();
    }

    /// <summary>
    /// Concurrent forwards to an owner whose host drops connection attempts all fail as unreachable well within their per-attempt timeout: they
    /// wait for one pending connection, and once its dial timed out, the next dials fail at once instead of each waiting a full dial bound.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentForwardsAreUnreachable(CancellationToken cancellationToken)
    {
        const int Forwards = 20;
        await using var pool = CreatePool(
            static () => new SocketsHttpHandler { ConnectCallback = static (_, ct) => NeverConnectsAsync(ct) },
            new Uri("https://localhost:6500"),
            TimeSpan.FromMilliseconds(200));
        var forwarder = CreateForwarder(pool, CreatePolicy(TimeSpan.FromSeconds(3), 1));

        var calls = new Task<GetValueAsyncResponse>[Forwards];
        for (var i = 0; i < Forwards; i++)
            calls[i] = forwarder.GetValueAsync(Owner, new GetValueAsyncRequest { CacheName = "c", Key = "k" }, cancellationToken);

        for (var i = 0; i < Forwards; i++)
        {
            var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(calls[i]);
            _ = await Assert.That(OwnerUnreachableFailure.IsLocal(failure)).IsTrue().Because($"forward {i} must fail as unreachable, not as {failure.Status}");
        }
    }

    /// <summary>
    /// A forward that connected and then hit its per-attempt timeout may have reached the owner, so it ends as a timeout and never as unreachable,
    /// even though the attempt was canceled like a slow connect would be.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TimeoutAfterConnectIsAmbiguous(CancellationToken cancellationToken)
    {
        await using var stream = new SilentStream();
        await using var pool = CreatePool(
            () => new SocketsHttpHandler { ConnectCallback = (_, _) => ValueTask.FromResult<Stream>(stream) },
            new Uri("http://localhost:6500"),
            TimeSpan.FromSeconds(1));
        var forwarder = CreateForwarder(pool, CreatePolicy(TimeSpan.FromSeconds(1), 1));

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(forwarder.GetValueAsync(Owner, new GetValueAsyncRequest { CacheName = "c", Key = "k" }, cancellationToken));

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.DeadlineExceeded);
        _ = await Assert.That(OwnerUnreachableFailure.IsLocal(failure)).IsFalse();
        _ = await Assert.That(stream.Written).IsGreaterThan(0L).Because("the connection was established and written to before the attempt timed out");
    }

    /// <summary>
    /// A forward written to a connection whose peer then goes silent, and failed by the keepalive pings closing that connection long before its
    /// per-attempt timeout, stays ambiguous: the peer may have received it, so it is neither a timeout nor reported as unreachable.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task KeepAliveAbortStaysAmbiguous(CancellationToken cancellationToken)
    {
        await using var stream = new SilentStream();
        await using var pool = CreatePool(
            () => new SocketsHttpHandler { ConnectCallback = (_, _) => ValueTask.FromResult<Stream>(stream) },
            new Uri("http://localhost:6500"),
            TimeSpan.FromSeconds(1));
        var forwarder = CreateForwarder(pool, CreatePolicy(TimeSpan.FromSeconds(30), 1));

        var started = Stopwatch.GetTimestamp();
        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(forwarder.GetValueAsync(Owner, new GetValueAsyncRequest { CacheName = "c", Key = "k" }, cancellationToken));
        var elapsed = Stopwatch.GetElapsedTime(started);

        // The delay, the ping timeout and the two heartbeat ticks of the handler add up to at most five seconds (the delay of one, the ping timeout of two and two ticks of one); the bound doubles it for slack for a loaded host.
        _ = await Assert.That(elapsed).IsLessThan(TimeSpan.FromSeconds(10)).Because($"the keepalive must close the connection long before the 30 s attempt timeout, took {elapsed}");
        _ = await Assert.That(failure.StatusCode).IsNotEqualTo(StatusCode.DeadlineExceeded);
        _ = await Assert.That(OwnerUnreachableFailure.IsLocal(failure)).IsFalse().Because($"the request was written, got {failure.Status}");
        _ = await Assert.That(stream.Written).IsGreaterThan(0L);
    }

    /// <summary>
    /// The pool gives every peer two handlers with the same long connect bound, TLS handshake included; only the forward channel's bounds its
    /// dial too, so replication and elections never get the short bound.
    /// </summary>
    [Test]
    public async Task OnlyForwardChannelBoundsDial()
    {
        var created = new List<SocketsHttpHandler>();
        await using (CreatePool(
            () =>
            {
                var handler = new SocketsHttpHandler { ConnectCallback = static (_, ct) => NeverConnectsAsync(ct) };
                created.Add(handler);
                return handler;
            },
            new Uri("https://localhost:6500"),
            TimeSpan.FromMilliseconds(300)))
        {
            _ = await Assert.That(created.Count).IsEqualTo(2);
            var bounded = 0;
            foreach (var handler in created)
            {
                _ = await Assert.That(handler.ConnectTimeout).IsEqualTo(TimeSpan.FromSeconds(5));
                bounded += handler.ConnectCallback?.Target is BoundedDial ? 1 : 0;
            }

            _ = await Assert.That(bounded).IsEqualTo(1);
        }
    }

    /// <summary>
    /// Only the forward channel pings its connections with HTTP/2 keepalive, whether or not a call is in flight, so a connection that went
    /// silent is found before the next forward; the channel of replication and elections sends no pings.
    /// </summary>
    [Test]
    public async Task OnlyForwardChannelPings()
    {
        var created = new List<SocketsHttpHandler>();
        await using (CreatePool(
            () =>
            {
                var handler = new SocketsHttpHandler { ConnectCallback = static (_, ct) => NeverConnectsAsync(ct) };
                created.Add(handler);
                return handler;
            },
            new Uri("https://localhost:6500"),
            TimeSpan.FromMilliseconds(300)))
        {
            _ = await Assert.That(created.Count).IsEqualTo(2);
            var pinging = 0;
            foreach (var handler in created)
            {
                if (handler.KeepAlivePingDelay == Timeout.InfiniteTimeSpan)
                    continue;

                pinging++;
                _ = await Assert.That(handler.KeepAlivePingDelay).IsEqualTo(TimeSpan.FromSeconds(1));
                _ = await Assert.That(handler.KeepAlivePingTimeout).IsEqualTo(TimeSpan.FromSeconds(2));
                _ = await Assert.That(handler.KeepAlivePingPolicy).IsEqualTo(HttpKeepAlivePingPolicy.Always);
                _ = await Assert.That(handler.ConnectCallback?.Target).IsTypeOf<BoundedDial>();
            }

            _ = await Assert.That(pinging).IsEqualTo(1);
        }
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static bool HasTimeoutCause(Exception? failure)
    {
        for (var current = failure; current != null; current = current.InnerException)
        {
            if (current is TimeoutException)
                return true;
        }

        return false;
    }

    /// <summary>Waits for a connect that never completes, as towards a host that drops connection attempts.</summary>
    /// <param name="cancellationToken">The connect token; the connect timeout cancels it.</param>
    /// <returns>A task that only ever fails with the cancellation.</returns>
    private static async ValueTask<Stream> NeverConnectsAsync(CancellationToken cancellationToken)
    {
        var never = new TaskCompletionSource<Stream>(TaskCreationOptions.RunContinuationsAsynchronously);
        return await never.Task.WaitAsync(cancellationToken);
    }

    private static IBackpressureGate CreateGate()
    {
        var expectations = new IBackpressureGateCreateExpectations();
        _ = expectations.Setups.AcquireAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                        .ReturnValue(ValueTask.FromResult((Decision.Accepted(), Lease.Empty)));
        return expectations.Instance();
    }

    private static IBackpressureClientIdResolver CreateClientIdResolver()
    {
        var expectations = new IBackpressureClientIdResolverCreateExpectations();
        _ = expectations.Setups.Resolve().ReturnValue("jwt:client");
        return expectations.Instance();
    }

    /// <summary>Creates a forwarder over a real pool whose owner policy is <paramref name="policy" />.</summary>
    /// <param name="pool">The pool that reaches the owner.</param>
    /// <param name="policy">The call policy of the owner.</param>
    /// <returns>The forwarder.</returns>
    private static OwnerRpcForwarder CreateForwarder(ServerClientPool pool, ServerCallPolicy policy)
    {
        var routed = new IServerClientPoolCreateExpectations();
        _ = routed.Setups.ForNode(Arg.Any<string>()).ReturnValue(pool.ForNode(Owner));
        _ = routed.Setups.PolicyFor(Arg.Any<string>()).ReturnValue(policy);
        return new OwnerRpcForwarder(routed.Instance(), CreateGate(), CreateClientIdResolver(), RingAgreements.Create());
    }

    /// <summary>Creates a forwarder whose call policy makes up to three attempts without backoff.</summary>
    /// <param name="invoker">The invoker of the owner client.</param>
    /// <returns>The forwarder.</returns>
    private OwnerRpcForwarder CreateForwarder(CapturingCallInvoker invoker)
    {
        var policy = CreatePolicy(TimeSpan.FromSeconds(30));
        var pool = new IServerClientPoolCreateExpectations();
        _ = pool.Setups.ForNode(Arg.Any<string>()).ReturnValue(new SquirixCacheService.SquirixCacheServiceClient(invoker));
        _ = pool.Setups.PolicyFor(Arg.Any<string>()).ReturnValue(policy);
        return new OwnerRpcForwarder(pool.Instance(), CreateGate(), CreateClientIdResolver(), RingAgreements.Create());
    }

    /// <summary>Creates a pool with one owner reached through the handler <paramref name="createHandler" /> creates, which the pool owns.</summary>
    /// <param name="createHandler">Creates the owned handler of the owner.</param>
    /// <param name="uri">The address of the owner.</param>
    /// <param name="connectTimeout">The connect timeout the pool sets on the forward handler.</param>
    /// <returns>The pool.</returns>
    private ServerClientPool CreatePool(Func<SocketsHttpHandler> createHandler, Uri uri, TimeSpan connectTimeout)
    {
        var args = new ServerClientPoolArgs
        {
            PolicyFactory = static _ => new IdleCallPolicy(),
            OwnedHandlerFactory = (_, _, _) => createHandler(),
            ForwardConnectTimeout = connectTimeout,
        };
        return new ServerClientPool([new ServerPeer { NodeId = Owner, Uri = uri }], args, new ServerClientPoolMetrics(_testMeter), NullLogger<ServerClientPool>.Instance);
    }

    private ServerCallPolicy CreatePolicy(TimeSpan perAttempt, int attempts = 3) => new(
        new ServerCallPolicyInstrumentation(new ServerCallPolicyMetrics(_testMeter), new ServerRpcTimeoutMetrics(_testMeter)),
        attempts,
        64,
        Owner,
        TimeProvider.System,
        new CallPolicyTimeouts(perAttempt, TimeSpan.Zero, TimeSpan.Zero));

    /// <summary>A connection that takes every byte written to it and never answers, until it is disposed.</summary>
    private sealed class SilentStream : Stream
    {
        private readonly TaskCompletionSource _closed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private long _written;

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        /// <summary>Gets the number of bytes written to the connection.</summary>
        internal long Written => Interlocked.Read(ref _written);

        public override void Flush()
        {
        }

        public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await _closed.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            return 0;
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => _ = Interlocked.Add(ref _written, count);

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            _ = Interlocked.Add(ref _written, buffer.Length);
            return ValueTask.CompletedTask;
        }

        protected override void Dispose(bool disposing)
        {
            _ = _closed.TrySetResult();
            base.Dispose(disposing);
        }
    }
}
