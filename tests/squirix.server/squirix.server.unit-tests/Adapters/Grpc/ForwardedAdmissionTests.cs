using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Cluster;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Invocation;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Grpc;

/// <summary>Two nodes forwarding to each other while both are saturated fail fast instead of waiting on each other's queues.</summary>
public sealed class ForwardedAdmissionTests : DisposableServerUnitTestBase
{
    private const string ClientId = "jwt:client";
    private const string Owner = "node-b";

    private readonly Meter _meterA = new("test-forwarded-a");
    private readonly Meter _meterB = new("test-forwarded-b");

    /// <summary>
    /// Both entry nodes hold their only slot while their forwarded request reaches the other node, which is full too.
    /// Each owner refuses at once, so both callers fail without a queue timeout and both slots come back.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SymmetricCrossForwardFailsFast(CancellationToken cancellationToken)
    {
        using var sinkA = new NodeMeasurementSink(_meterA);
        using var sinkB = new NodeMeasurementSink(_meterB);
        var clock = new FakeTimeProvider();
        using var gateA = new AdmissionGate(CreateOptions(), new BackpressureMetrics(_meterA), clock);
        using var gateB = new AdmissionGate(CreateOptions(), new BackpressureMetrics(_meterB), clock);
        using var go = new SemaphoreSlim(0);
        var toB = new OwnerGateInvoker(gateB, go);
        var toA = new OwnerGateInvoker(gateA, go);
        var forwarderA = CreateForwarder(gateA, toB);
        var forwarderB = CreateForwarder(gateB, toA);

        var first = forwarderA.GetValueAsync(Owner, new GetValueAsyncRequest { CacheName = "c", Key = "k" }, cancellationToken);
        var second = forwarderB.GetValueAsync(Owner, new GetValueAsyncRequest { CacheName = "c", Key = "k" }, cancellationToken);
        await toB.Entered.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        await toA.Entered.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        _ = await Assert.That(gateA.InFlight).IsEqualTo(1);
        _ = await Assert.That(gateB.InFlight).IsEqualTo(1);

        _ = go.Release(2);
        var failureA = await NodeAsyncAssert.ThrowsAsync<RpcException>(first.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken));
        var failureB = await NodeAsyncAssert.ThrowsAsync<RpcException>(second.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken));

        _ = await Assert.That(failureA.StatusCode).IsEqualTo(StatusCode.ResourceExhausted);
        _ = await Assert.That(failureB.StatusCode).IsEqualTo(StatusCode.ResourceExhausted);
        _ = await Assert.That(toB.Decisions[0]).IsEqualTo("forwarded_no_slot");
        _ = await Assert.That(toA.Decisions[0]).IsEqualTo("forwarded_no_slot");
        _ = await Assert.That(gateA.InFlight).IsEqualTo(0);
        _ = await Assert.That(gateB.InFlight).IsEqualTo(0);
        _ = await Assert.That(gateA.QueueDepth).IsEqualTo(0);
        _ = await Assert.That(gateB.QueueDepth).IsEqualTo(0);
        _ = await Assert.That(sinkA.HasEvent("squirix_backpressure_queue_timeouts_total")).IsFalse();
        _ = await Assert.That(sinkB.HasEvent("squirix_backpressure_queue_timeouts_total")).IsFalse();
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        _meterA.Dispose();
        _meterB.Dispose();
    }

    private static AdmissionOptions CreateOptions() => new()
    {
        MaxInFlight = 1,
        MaxQueue = 1,
        SlowdownThreshold = 1,
        MaxSlowdownDelay = TimeSpan.Zero,
        MaxQueueWait = TimeSpan.FromMinutes(1),
    };

    private static IBackpressureClientIdResolver CreateClientIdResolver()
    {
        var expectations = new IBackpressureClientIdResolverCreateExpectations();
        _ = expectations.Setups.Resolve().ReturnValue(ClientId);
        return expectations.Instance();
    }

    private OwnerRpcForwarder CreateForwarder(AdmissionGate entryGate, OwnerGateInvoker invoker)
    {
        var poolExpectations = new IServerClientPoolCreateExpectations();
        _ = poolExpectations.Setups.ForNode(Arg.Any<string>()).ReturnValue(new SquirixCacheService.SquirixCacheServiceClient(invoker));
        _ = poolExpectations.Setups.PolicyFor(Arg.Any<string>()).ReturnValue(CreatePolicy());
        return new OwnerRpcForwarder(poolExpectations.Instance(), entryGate, CreateClientIdResolver(), RingAgreements.Create());
    }

    private ServerCallPolicy CreatePolicy() => new(
        new ServerCallPolicyInstrumentation(new ServerCallPolicyMetrics(_meterA), new ServerRpcTimeoutMetrics(_meterA)),
        3,
        64,
        Owner,
        TimeProvider.System,
        new CallPolicyTimeouts(TimeSpan.FromSeconds(30), TimeSpan.Zero, TimeSpan.Zero));

    /// <summary>Runs every call as the owner node does: after a barrier it admits the call on the owner gate as an internal owner invocation.</summary>
    private sealed class OwnerGateInvoker : CallInvoker
    {
        private readonly SemaphoreSlim _barrier;
        private readonly AdmissionGate _ownerGate;

        internal OwnerGateInvoker(AdmissionGate ownerGate, SemaphoreSlim barrier)
        {
            _ownerGate = ownerGate;
            _barrier = barrier;
        }

        /// <summary>Gets the reject reason of every owner admission, or <see langword="null" /> when it was accepted, in call order.</summary>
        internal List<string?> Decisions { get; } = [];

        /// <summary>Gets a task that completes once the first call reached this owner.</summary>
        internal TaskCompletionSource EnteredSource { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Task Entered => EnteredSource.Task;

        /// <inheritdoc />
        public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            string? host,
            CallOptions options) => throw new NotSupportedException();

        /// <inheritdoc />
        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            string? host,
            CallOptions options) => throw new NotSupportedException();

        /// <inheritdoc />
        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            string? host,
            CallOptions options,
            TRequest request) => throw new NotSupportedException();

        /// <inheritdoc />
        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
            new(
                RunOwnerAsync<TResponse>(options.CancellationToken),
                Task.FromResult(new Metadata()),
                static () => new Status(StatusCode.OK, string.Empty),
                static () => [],
                static () => { });

        /// <inheritdoc />
        public override TResponse BlockingUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
            throw new NotSupportedException();

        private static TResponse CreateResponse<TResponse>() =>
            new GetValueAsyncResponse() is TResponse response ? response : throw new InvalidOperationException("Only GetValue responses are supported.");

        private async Task<TResponse> RunOwnerAsync<TResponse>(CancellationToken cancellationToken)
        {
            _ = EnteredSource.TrySetResult();
            await _barrier.WaitAsync(cancellationToken).ConfigureAwait(false);

            using var scope = RemoteInvocationContext.EnterRemoteInvocation(true);
            var (decision, lease) = await _ownerGate.AcquireAsync("cache", "get", SharedClientIdResolver.Instance.Resolve(), cancellationToken).ConfigureAwait(false);
            using (lease)
            {
                Decisions.Add(decision.RejectReason);
                return decision.IsAccepted
                    ? CreateResponse<TResponse>()
                    : throw new RpcException(new Status(StatusCode.ResourceExhausted, decision.RejectReason ?? "unknown"));
            }
        }
    }
}
