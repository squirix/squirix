using System;
using System.Diagnostics.Metrics;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Rocks;
using Squirix.Server.Adapters.Wire;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Errors;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Testing;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Wire;

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
        var invoker = new CapturingCallInvoker(
            failure: () =>
            {
                var cause = ++attempt == 1
                    ? new HttpRequestException(HttpRequestError.ResponseEnded, "the response ended")
                    : new HttpRequestException(HttpRequestError.ConnectionError, "connection refused");
                return raw ? cause : new ServerCallException(FrameStatus.Unavailable, "start failed", cause);
            });
        var forwarder = CreateForwarder(invoker);

        var failure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(forwarder.GetValueAsync(Owner, new GetValueAsyncRequest { CacheName = "c", Key = "k" }, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(invoker.Requests.Count).IsEqualTo(3);
        _ = await Assert.That(failure.StatusCode).IsEqualTo(FrameStatus.Unavailable);
        _ = await Assert.That(failure.Message).IsNotEqualTo(ServerOpContract.OwnerUnreachableDetail);
        _ = await Assert.That(OwnerUnreachableFailure.IsLocal(failure)).IsFalse();
    }

    /// <summary>Every attempt failing to connect reports the forward as unreachable.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EveryAttemptRefusedIsUnreachable(CancellationToken cancellationToken)
    {
        var invoker = new CapturingCallInvoker(failure: static () => new HttpRequestException(HttpRequestError.ConnectionError, "connection refused"));
        var forwarder = CreateForwarder(invoker);

        var failure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(forwarder.GetValueAsync(Owner, new GetValueAsyncRequest { CacheName = "c", Key = "k" }, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(invoker.Requests.Count).IsEqualTo(3);
        _ = await Assert.That(OwnerUnreachableFailure.IsLocal(failure)).IsTrue();
    }

    /// <summary>
    /// A hop whose TLS handshake failed sent nothing, so it surfaces as the unreachable failure raised on this node although the client gave it
    /// Internal, not Unavailable; an Internal status the owner sent is relayed unchanged.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HandshakeFailureMapsToUnavailable(CancellationToken cancellationToken)
    {
        var handshakeInvoker = new CapturingCallInvoker(
            failure: static () => new ServerCallException(
                    FrameStatus.Internal,
                    "Cannot start peer call.",
                    new HttpRequestException(HttpRequestError.SecureConnectionError, "The SSL connection could not be established.", new ObjectDisposedException("credential"))));
        var handshake = CreateForwarder(handshakeInvoker);
        var owner = CreateForwarder(new CapturingCallInvoker(failure: static () => new ServerCallException(FrameStatus.Internal, "owner failed")));
        var request = new GetValueAsyncRequest { CacheName = "c", Key = "k" };

        var handshakeFailure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(handshake.GetValueAsync(Owner, request, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        var ownerFailure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(owner.GetValueAsync(Owner, request, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(handshakeInvoker.Requests.Count).IsEqualTo(3);
        _ = await Assert.That(OwnerUnreachableFailure.IsLocal(handshakeFailure)).IsTrue();
        _ = await Assert.That((ownerFailure.StatusCode, ownerFailure.Message)).IsEqualTo((FrameStatus.Internal, "owner failed"));
    }

    /// <summary>A forward whose attempt was canceled, as by its per-attempt timeout, reaches the caller as a timeout and never as unreachable.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledAttemptIsATimeout(CancellationToken cancellationToken)
    {
        var forwarder = CreateForwarder(new CapturingCallInvoker(failure: static () => new OperationCanceledException("attempt canceled")));

        var failure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(forwarder.GetValueAsync(Owner, new GetValueAsyncRequest { CacheName = "c", Key = "k" }, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(failure.StatusCode).IsEqualTo(FrameStatus.DeadlineExceeded);
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
        using var peer = new NativePeerFixture();
        await using var pool = peer.CreatePool(TimeSpan.FromMilliseconds(50), static ct => NeverConnectsAsync(ct));
        var forwarder = CreateForwarder(pool, CreatePolicy(TimeSpan.FromSeconds(30), TimeProvider.System));

        var failure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(forwarder.GetValueAsync(Owner, new GetValueAsyncRequest { CacheName = "c", Key = "k" }, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(OwnerUnreachableFailure.IsLocal(failure)).IsTrue();
        _ = await Assert.That(HasTimeoutCause(failure.InnerException)).IsTrue();
    }

    /// <summary>
    /// A forward that starts with only the leader refusal margin left, towards a host that drops connection attempts, ends as unreachable at the
    /// default connect timeout, before its deadline, and never as a deadline. The deadline and attempt timeout both exceed the dial bound.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConnectFailureLandsBeforeMarginDeadline(CancellationToken cancellationToken)
    {
        var clock = TimeProvider.System;
        var dialStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var peer = new NativePeerFixture();
        await using var pool = peer.CreatePool(TopologyOptions.DefaultForwardConnectTimeout, ct => NeverConnectsAsync(ct, dialStarted));
        var forwarder = CreateForwarder(pool, CreatePolicy(ForwardingCallPolicyDefaults.TimeoutPerAttempt, clock));

        ServerCallException failure;
        using (ServerRpcDeadlineContext.Push(clock.GetUtcNow().UtcDateTime + ElectionTimerOptions.LeaderRefusalMargin, clock))
        {
            var forward = forwarder.GetValueAsync(Owner, new GetValueAsyncRequest { CacheName = "c", Key = "k" }, cancellationToken);
            await dialStarted.Task.WaitAsync(cancellationToken);
            failure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(forward, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        }

        _ = await Assert.That(OwnerUnreachableFailure.IsLocal(failure)).IsTrue();
        _ = await Assert.That(failure.StatusCode).IsNotEqualTo(FrameStatus.DeadlineExceeded);
    }

    /// <summary>
    /// Concurrent forwards to an owner whose host drops connection attempts all fail as unreachable well within their per-attempt timeout: they
    /// wait for one pending connection, and once its dial timed out, the next dials fail at once instead of each waiting a full dial bound.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentForwardsAreUnreachable(CancellationToken cancellationToken)
    {
        const int forwards = 20;
        using var peer = new NativePeerFixture();
        await using var pool = peer.CreatePool(TimeSpan.FromMilliseconds(200), static ct => NeverConnectsAsync(ct));
        var forwarder = CreateForwarder(pool, CreatePolicy(TimeSpan.FromSeconds(3), TimeProvider.System, 1));

        var calls = new Task<GetValueAsyncResponse>[forwards];
        for (var i = 0; i < forwards; i++)
            calls[i] = forwarder.GetValueAsync(Owner, new GetValueAsyncRequest { CacheName = "c", Key = "k" }, cancellationToken);

        for (var i = 0; i < forwards; i++)
        {
            var failure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(calls[i], TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
            _ = await Assert.That(OwnerUnreachableFailure.IsLocal(failure)).IsTrue().Because($"forward {i} must fail as unreachable, not as {failure.StatusCode}: {failure.Message}");
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
        using var peer = new NativePeerFixture();
        await using var pool = peer.CreatePool();
        var clock = new FakeTimeProvider();
        var attempt = TimeSpan.FromSeconds(1);
        var forwarder = CreateForwarder(pool, CreatePolicy(attempt, clock, 1));
        var call = forwarder.GetValueAsync(Owner, new GetValueAsyncRequest { CacheName = "c", Key = "k" }, cancellationToken);
        await using var stream = await peer.AcceptTlsAsync(cancellationToken);
        var (path, _, length) = await NativePeerFixture.ReadAsync(stream, cancellationToken);
        _ = await Assert.That(path).IsEqualTo("owner/GetValue");
        _ = await Assert.That(length).IsGreaterThan(0);
        clock.Advance(attempt);
        var failure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(call, TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        _ = await Assert.That(failure.StatusCode).IsEqualTo(FrameStatus.DeadlineExceeded);
        _ = await Assert.That(OwnerUnreachableFailure.IsLocal(failure)).IsFalse();
    }

    /// <summary>A peer closing after receipt leaves the result ambiguous before the attempt deadline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisconnectAfterSentRequestStaysAmbiguous(CancellationToken cancellationToken)
    {
        using var peer = new NativePeerFixture();
        await using var pool = peer.CreatePool();
        var forwarder = CreateForwarder(pool, CreatePolicy(TimeSpan.FromSeconds(30), TimeProvider.System, 1));
        var call = forwarder.GetValueAsync(Owner, new GetValueAsyncRequest { CacheName = "c", Key = "k" }, cancellationToken);
        await using var stream = await peer.AcceptTlsAsync(cancellationToken);
        var (path, _, length) = await NativePeerFixture.ReadAsync(stream, cancellationToken);
        _ = await Assert.That(path).IsEqualTo("owner/GetValue");
        _ = await Assert.That(length).IsGreaterThan(0);
        await stream.DisposeAsync();
        var failure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(call, TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        _ = await Assert.That(failure.StatusCode).IsNotEqualTo(FrameStatus.DeadlineExceeded);
        _ = await Assert.That(OwnerUnreachableFailure.IsLocal(failure)).IsFalse();
    }

    /// <summary>The short forward dial bound does not cancel a pending replication dial.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ChannelsUseSeparateDialBounds(CancellationToken cancellationToken)
    {
        using var peer = new NativePeerFixture();
        await using var pool = peer.CreatePool(TimeSpan.FromMilliseconds(25), static ct => NeverConnectsAsync(ct));
        using var caller = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var lease = pool.LeaseChannel(Owner, caller.Token);
        var replication = NativePeerFixture.PingAsync(lease.Client, lease.Token);
        var forward = NativePeerFixture.PingAsync(pool.ForNode(Owner), cancellationToken);

        var failure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(forward, TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        _ = await Assert.That(HasTimeoutCause(failure)).IsTrue();
        _ = await Assert.That(replication.IsCompleted).IsFalse();
        await caller.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(replication, TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
    }

    /// <summary>Both native roles carry an explicit ping and only forwarding supplies the ring fingerprint.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BothChannelsAnswerExplicitPeerPing(CancellationToken cancellationToken)
    {
        using var peer = new NativePeerFixture();
        await using var pool = peer.CreatePool();
        using var lease = pool.LeaseChannel(Owner, cancellationToken);

        var forward = await peer.ExchangePingAsync(pool.ForNode(Owner), cancellationToken);
        var replica = await peer.ExchangePingAsync(lease.Client, lease.Token);

        _ = await Assert.That(forward).IsEqualTo(("ping", NativePeerFixture.Fingerprint, 0));
        _ = await Assert.That(replica).IsEqualTo(("ping", string.Empty, 0));
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static bool HasTimeoutCause(Exception? failure)
    {
        for (var current = failure; current != null; current = current.InnerException)
        {
            if (current is SocketException { SocketErrorCode: SocketError.TimedOut })
                return true;
        }

        return false;
    }

    /// <summary>Waits for a connect that never completes, as towards a host that drops connection attempts.</summary>
    /// <param name="cancellationToken">The connect token; the connect timeout cancels it.</param>
    /// <param name="dialStarted">Completed once the dial started, so its timeout is armed; <see langword="null" /> when no test waits for it.</param>
    /// <returns>A task that only ever fails with the cancellation.</returns>
    private static async ValueTask<EndPoint> NeverConnectsAsync(CancellationToken cancellationToken, TaskCompletionSource? dialStarted = null)
    {
        _ = dialStarted?.TrySetResult();
        var never = new TaskCompletionSource<EndPoint>(TaskCreationOptions.RunContinuationsAsynchronously);
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
        var policy = CreatePolicy(TimeSpan.FromSeconds(30), TimeProvider.System);
        var pool = new IServerClientPoolCreateExpectations();
        _ = pool.Setups.ForNode(Arg.Any<string>()).ReturnValue(invoker);
        _ = pool.Setups.PolicyFor(Arg.Any<string>()).ReturnValue(policy);
        return new OwnerRpcForwarder(pool.Instance(), CreateGate(), CreateClientIdResolver(), RingAgreements.Create());
    }

    private ServerCallPolicy CreatePolicy(TimeSpan perAttempt, TimeProvider clock, int attempts = 3) => new(
        new ServerCallPolicyInstrumentation(new ServerCallPolicyMetrics(_testMeter), new ServerRpcTimeoutMetrics(_testMeter)),
        clock,
        attempts,
        64,
        Owner,
        new CallPolicyTimeouts(perAttempt, TimeSpan.Zero, TimeSpan.Zero));
}
