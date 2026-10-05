using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Invocation;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Unit tests for admitting requests forwarded by an entry node only to a free slot.</summary>
[Immutable]
public sealed class BackpressureForwardedAdmissionTests : DisposableServerUnitTestBase
{
    private const string Forwarded = HttpContextClientIdResolver.InternalOwnerClientId;

    private readonly Meter _testMeter = new("test");

    /// <summary>Verifies a forwarded request takes a free slot and the lease gives it back.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardedRequestTakesFreeSlot(CancellationToken cancellationToken)
    {
        using var gate = CreateGate(CreateOptions());

        var (decision, lease) = await gate.AcquireAsync("grpc", "get", Forwarded, cancellationToken);
        _ = await Assert.That(decision.IsAccepted).IsTrue();
        _ = await Assert.That(gate.InFlight).IsEqualTo(1);

        lease.Dispose();
        _ = await Assert.That(gate.InFlight).IsEqualTo(0);

        var (again, againLease) = await gate.AcquireAsync("grpc", "get", Forwarded, cancellationToken);
        againLease.Dispose();
        _ = await Assert.That(again.IsAccepted).IsTrue();
    }

    /// <summary>Verifies a forwarded request that finds no free slot is rejected at once without joining the queue.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardedRequestRejectedWithoutQueue(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        using var gate = new AdmissionGate(CreateOptions(), new BackpressureMetrics(meter), new FakeTimeProvider());
        var held = (await gate.AcquireAsync("grpc", "get", "rest:client-0", cancellationToken)).Lease;

        var pending = gate.AcquireAsync("grpc", "get", Forwarded, cancellationToken);

        _ = await Assert.That(pending.IsCompletedSuccessfully).IsTrue();
        var (decision, lease) = await pending;
        lease.Dispose();
        _ = await Assert.That(decision.RejectReason).IsEqualTo("forwarded_no_slot");
        _ = await Assert.That(gate.QueueDepth).IsEqualTo(0);
        _ = await Assert.That(gate.InFlight).IsEqualTo(1);
        _ = await Assert.That(sink.HasEvent("squirix_backpressure_reject_total", ("transport", "grpc"), ("op", "get"), ("reason", "forwarded_no_slot"))).IsTrue();
        _ = await Assert.That(sink.HasEvent("squirix_backpressure_queue_timeouts_total")).IsFalse();

        held.Dispose();
    }

    /// <summary>Verifies a forwarded request does not overtake a request already queued, even when it arrives as a slot frees up.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardedRequestDoesNotOvertake(CancellationToken cancellationToken)
    {
        using var gate = CreateGate(CreateOptions());
        var held = (await gate.AcquireAsync("grpc", "get", "rest:client-0", cancellationToken)).Lease;
        var queued = StartAcquireAsync(gate, "rest:client-1", cancellationToken);
        _ = await Assert.That(gate.QueueDepth).IsEqualTo(1);

        held.Dispose();
        var (forwardedDecision, forwardedLease) = await gate.AcquireAsync("grpc", "get", Forwarded, cancellationToken);
        forwardedLease.Dispose();
        var (queuedDecision, queuedLease) = await queued.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);

        _ = await Assert.That(forwardedDecision.RejectReason).IsEqualTo("forwarded_no_slot");
        _ = await Assert.That(queuedDecision.IsAccepted).IsTrue();

        queuedLease.Dispose();
        _ = await Assert.That(gate.InFlight).IsEqualTo(0);
        _ = await Assert.That(gate.QueueDepth).IsEqualTo(0);
    }

    /// <summary>Verifies a forwarded request is not slowed down even when the gate is past its slowdown threshold.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardedRequestSkipsSlowdown(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        using var gate = new AdmissionGate(
            CreateOptions(2, TimeSpan.FromSeconds(5)),
            new BackpressureMetrics(meter),
            new FakeTimeProvider());
        var held = (await gate.AcquireAsync("grpc", "get", Forwarded, cancellationToken)).Lease;

        var pending = gate.AcquireAsync("grpc", "get", Forwarded, cancellationToken);

        _ = await Assert.That(pending.IsCompletedSuccessfully).IsTrue();
        var (decision, lease) = await pending;
        _ = await Assert.That(decision.IsAccepted).IsTrue();
        _ = await Assert.That(sink.HasEvent("squirix_backpressure_slowdown_total")).IsFalse();

        lease.Dispose();
        held.Dispose();
    }

    /// <summary>Verifies a forwarded request still counts against the node rate limit.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardedRequestRespectsNodeRateLimit(CancellationToken cancellationToken)
    {
        using var gate = CreateGate(CreateOptions(4, TimeSpan.Zero, 1));

        var (first, firstLease) = await gate.AcquireAsync("grpc", "get", Forwarded, cancellationToken);
        var (second, secondLease) = await gate.AcquireAsync("grpc", "get", Forwarded, cancellationToken);
        firstLease.Dispose();
        secondLease.Dispose();

        _ = await Assert.That(first.IsAccepted).IsTrue();
        _ = await Assert.That(second.RejectReason).IsEqualTo("node_rate_limit");
        _ = await Assert.That(gate.InFlight).IsEqualTo(0);
    }

    /// <summary>Verifies forwarded requests keep no per-client state, with per-client limits both off and on.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardedRequestsTrackNoClients(CancellationToken cancellationToken)
    {
        using var shared = CreateGate(CreateOptions());
        using var limited = CreateGate(CreateOptions(4, TimeSpan.Zero, null, 1));

        var sharedLease = (await shared.AcquireAsync("grpc", "get", Forwarded, cancellationToken)).Lease;
        var firstLease = (await limited.AcquireAsync("grpc", "get", Forwarded, cancellationToken)).Lease;
        var (second, secondLease) = await limited.AcquireAsync("grpc", "get", Forwarded, cancellationToken);

        _ = await Assert.That(shared.TrackedClients).IsEqualTo(0);
        _ = await Assert.That(limited.TrackedClients).IsEqualTo(0);
        _ = await Assert.That(second.IsAccepted).IsTrue();

        sharedLease.Dispose();
        firstLease.Dispose();
        secondLease.Dispose();
        _ = await Assert.That(limited.InFlight).IsEqualTo(0);
    }

    /// <summary>Verifies a forwarded request on a disposed gate fails like any other admission.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardedRequestOnDisposedGateThrows(CancellationToken cancellationToken)
    {
        var gate = CreateGate(CreateOptions());
        gate.Dispose();

        var pending = gate.AcquireAsync("grpc", "get", Forwarded, cancellationToken).AsTask();

        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(pending);
    }

    /// <summary>Verifies the shared-mode resolver returns the internal id only inside an internal owner invocation.</summary>
    [Test]
    public async Task SharedResolverMarksInternalOwnerCalls()
    {
        var outside = SharedClientIdResolver.Instance.Resolve();
        string inside;
        using (RemoteInvocationContext.EnterRemoteInvocation(true))
            inside = SharedClientIdResolver.Instance.Resolve();

        string external;
        using (RemoteInvocationContext.EnterRemoteInvocation())
            external = SharedClientIdResolver.Instance.Resolve();

        _ = await Assert.That(outside).IsEqualTo(HttpContextClientIdResolver.MissingHttpContextClientId);
        _ = await Assert.That(inside).IsEqualTo(HttpContextClientIdResolver.InternalOwnerClientId);
        _ = await Assert.That(external).IsEqualTo(HttpContextClientIdResolver.MissingHttpContextClientId);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static AdmissionOptions CreateOptions(
        int maxInFlight = 1,
        TimeSpan maxSlowdownDelay = default,
        int? nodeRateLimit = null,
        int? perClientMaxInFlight = null)
    {
        return new AdmissionOptions
        {
            MaxInFlight = maxInFlight,
            MaxQueue = 4,
            SlowdownThreshold = 1,
            MaxSlowdownDelay = maxSlowdownDelay,
            MaxQueueWait = TimeSpan.FromMinutes(1),
            NodeRateLimitPerSecond = nodeRateLimit,
            NodeRateLimitBurst = nodeRateLimit,
            PerClientMaxInFlight = perClientMaxInFlight,
        };
    }

    private static Task<(Decision Decision, Lease Lease)> StartAcquireAsync(AdmissionGate gate, string clientId, CancellationToken cancellationToken) =>
        gate.AcquireAsync("grpc", "get", clientId, cancellationToken).AsTask();

    private AdmissionGate CreateGate(AdmissionOptions options) => new(options, new BackpressureMetrics(_testMeter), new FakeTimeProvider());
}
