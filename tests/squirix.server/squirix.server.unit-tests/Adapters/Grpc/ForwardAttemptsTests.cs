using System;
using System.Diagnostics.Metrics;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Rocks;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Cluster;
using Squirix.Server.Errors;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
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

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

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

    /// <summary>Creates a forwarder whose call policy makes up to three attempts without backoff.</summary>
    /// <param name="invoker">The invoker of the owner client.</param>
    /// <returns>The forwarder.</returns>
    private OwnerRpcForwarder CreateForwarder(CapturingCallInvoker invoker)
    {
        var policy = CreatePolicy();
        var pool = new IServerClientPoolCreateExpectations();
        _ = pool.Setups.ForNode(Arg.Any<string>()).ReturnValue(new SquirixCacheService.SquirixCacheServiceClient(invoker));
        _ = pool.Setups.PolicyFor(Arg.Any<string>()).ReturnValue(policy);
        return new OwnerRpcForwarder(pool.Instance(), CreateGate(), CreateClientIdResolver(), RingAgreements.Create());
    }

    private ServerCallPolicy CreatePolicy() => new(
        new ServerCallPolicyInstrumentation(new ServerCallPolicyMetrics(_testMeter), new ServerRpcTimeoutMetrics(_testMeter)),
        3,
        64,
        Owner,
        TimeProvider.System,
        new CallPolicyTimeouts(TimeSpan.FromSeconds(30), TimeSpan.Zero, TimeSpan.Zero));
}
