using System;
using System.Diagnostics.Metrics;
using System.Runtime.InteropServices;
using System.Security.Claims;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Unit tests for node-level backpressure admission control.</summary>
[Immutable]
public sealed class BackpressureGateTests : DisposableServerUnitTestBase
{
    private readonly Meter _testMeter = new("test");

    /// <summary>Verifies disabled backpressure returns an accepted empty lease and emits bypass metrics.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AcquireBypassesDisabledBackpressure(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                Enabled = false,
                MaxInFlight = 1,
                MaxQueue = 0,
                SlowdownThreshold = 1,
                RejectThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(200),
            },
            new BackpressureMetrics(meter));

        var (decision, lease) = await gate.AcquireAsync("rest", "insert", "rest:client-a", cancellationToken);
        lease.Dispose();

        _ = await Assert.That(decision.IsAccepted).IsTrue();
        _ = await Assert.That(sink.HasEvent("squirix_backpressure_bypass_total", ("transport", "rest"), ("op", "insert"))).IsTrue();
    }

    /// <summary>Verifies internal owner-routed calls are exempt from per-client limits but still count against node capacity.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InternalClientSkipsPerClientLimits(CancellationToken cancellationToken)
    {
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 4,
                MaxQueue = 0,
                SlowdownThreshold = 4,
                RejectThreshold = 4,
                MaxSlowdownDelay = TimeSpan.Zero,
                PerClientMaxInFlight = 1,
                PerClientRateLimitPerSecond = 1,
                PerClientRateLimitBurst = 1,
            },
            new BackpressureMetrics(_testMeter),
            new FakeTimeProvider());
        var decisions = new bool[5];
        var leases = new Lease[5];
        string? lastRejectReason = null;

        for (var i = 0; i < decisions.Length; i++)
        {
            var (decision, lease) = await gate.AcquireAsync("grpc", "get", HttpContextClientIdResolver.InternalOwnerClientId, cancellationToken);
            decisions[i] = decision.IsAccepted;
            leases[i] = lease;
            lastRejectReason = decision.RejectReason;
        }

        foreach (var lease in leases)
            lease.Dispose();

        _ = await Assert.That(decisions[0] && decisions[1] && decisions[2] && decisions[3]).IsTrue();
        _ = await Assert.That(decisions[4]).IsFalse();
        _ = await Assert.That(lastRejectReason).IsEqualTo("queue_full");
    }

    /// <summary>Verifies internal owner-routed calls still count against the node rate limit.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InternalClientHitsNodeRateLimit(CancellationToken cancellationToken)
    {
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 4,
                MaxQueue = 0,
                SlowdownThreshold = 4,
                RejectThreshold = 4,
                MaxSlowdownDelay = TimeSpan.Zero,
                PerClientMaxInFlight = 1,
                NodeRateLimitPerSecond = 1,
                NodeRateLimitBurst = 1,
            },
            new BackpressureMetrics(_testMeter),
            new FakeTimeProvider());

        var (first, firstLease) = await gate.AcquireAsync("grpc", "get", HttpContextClientIdResolver.InternalOwnerClientId, cancellationToken);
        firstLease.Dispose();
        var (second, secondLease) = await gate.AcquireAsync("grpc", "get", HttpContextClientIdResolver.InternalOwnerClientId, cancellationToken);
        secondLease.Dispose();

        _ = await Assert.That(first.IsAccepted).IsTrue();
        _ = await Assert.That(second.RejectReason).IsEqualTo("node_rate_limit");
    }

    /// <summary>Verifies a caller whose JWT subject is literally the internal id is still limited per client.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InternalSubjectStillRateLimited(CancellationToken cancellationToken)
    {
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 4,
                MaxQueue = 0,
                SlowdownThreshold = 4,
                RejectThreshold = 4,
                MaxSlowdownDelay = TimeSpan.Zero,
                PerClientRateLimitPerSecond = 1,
                PerClientRateLimitBurst = 1,
            },
            new BackpressureMetrics(_testMeter),
            new FakeTimeProvider());
        var clientId = new HttpContextClientIdResolver(CreateAccessorFor("internal")).Resolve();

        var (first, firstLease) = await gate.AcquireAsync("grpc", "get", clientId, cancellationToken);
        firstLease.Dispose();
        var (second, secondLease) = await gate.AcquireAsync("grpc", "get", clientId, cancellationToken);
        secondLease.Dispose();

        _ = await Assert.That(clientId).IsEqualTo("jwt:internal");
        _ = await Assert.That(first.IsAccepted).IsTrue();
        _ = await Assert.That(second.RejectReason).IsEqualTo("client_rate_limit");
    }

    /// <summary>Verifies admission succeeds immediately while slots are available.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AcquireSucceedsWithFreeCapacity(CancellationToken cancellationToken)
    {
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 2,
                MaxQueue = 1,
                SlowdownThreshold = 2,
                RejectThreshold = 2,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(200),
            },
            new BackpressureMetrics(_testMeter));

        var (decision, lease) = await gate.AcquireAsync("grpc", "get", "grpc:client-a", cancellationToken);
        using (lease)
        {
            _ = await Assert.That(decision.IsAccepted).IsTrue();
            _ = await Assert.That(decision.RejectReason).IsNull();
        }
    }

    /// <summary>Verifies releasing a lease after the gate was disposed does not throw and keeps the in-flight gauge consistent.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeaseReleaseAfterGateDisposeDoesNotThrow(CancellationToken cancellationToken)
    {
        var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 1,
                MaxQueue = 1,
                SlowdownThreshold = 1,
                RejectThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(200),
            },
            new BackpressureMetrics(_testMeter));
        var (decision, lease) = await gate.AcquireAsync("grpc", "get", "grpc:client-a", cancellationToken);
        _ = await Assert.That(decision.IsAccepted).IsTrue();

        gate.Dispose();
        lease.Dispose();
        lease.Dispose();
    }

    /// <summary>Verifies a request queued when the gate is disposed is rejected promptly instead of failing with a disposal error or waiting out the queue budget.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QueuedRequestIsRejectedOnGateDispose(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 1,
                MaxQueue = 1,
                SlowdownThreshold = 1,
                RejectThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMinutes(1),
            },
            new BackpressureMetrics(meter));
        var (_, held) = await gate.AcquireAsync("rest", "get", "rest:client-a", cancellationToken);
        var queued = gate.AcquireAsync("rest", "get", "rest:client-b", cancellationToken).AsTask();
        _ = await Assert.That(queued.IsCompleted).IsFalse();

        gate.Dispose();
        gate.Dispose();
        var (decision, lease) = await queued.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        lease.Dispose();
        held.Dispose();

        _ = await Assert.That(decision.IsAccepted).IsFalse();
        _ = await Assert.That(decision.RejectReason).IsEqualTo("gate_disposed");
        _ = await Assert.That(sink.HasEvent("squirix_backpressure_reject_total", ("transport", "rest"), ("op", "get"), ("reason", "gate_disposed"))).IsTrue();
    }

    /// <summary>Verifies concurrent acquire and release does not exceed configured in-flight capacity.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrencyCannotExceedConfiguredCap(CancellationToken cancellationToken)
    {
        const int maxInFlight = 3;
        var backpressureOptions = new AdmissionOptions
        {
            MaxInFlight = maxInFlight,
            MaxQueue = 64,
            SlowdownThreshold = maxInFlight,
            RejectThreshold = maxInFlight,
            MaxSlowdownDelay = TimeSpan.Zero,
            MaxQueueWait = TimeSpan.FromSeconds(2),
        };
        using var gate = new AdmissionGate(backpressureOptions, new BackpressureMetrics(_testMeter));
        IBackpressureGate gateForClients = gate;
        var current = new int[1];
        var observedMax = new int[1];
        var clients = new Task[24];
        for (var i = 0; i < clients.Length; i++)
            clients[i] = RunClientAsync(gateForClients, i, current, observedMax, cancellationToken);

        var runClients = Task.WhenAll(clients);

        await runClients;

        _ = await Assert.That(observedMax[0] <= maxInFlight).IsTrue();
    }

    /// <summary>Verifies disposing the same lease twice releases its slot exactly once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeaseDoubleDisposeReleasesOnce(CancellationToken cancellationToken)
    {
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 1,
                MaxQueue = 1,
                SlowdownThreshold = 1,
                RejectThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(200),
            },
            new BackpressureMetrics(_testMeter));

        var lease = (await gate.AcquireAsync("grpc", "get", "grpc:client-a", cancellationToken)).Lease;
        lease.Dispose();
        lease.Dispose();

        var (decision, secondLease) = await gate.AcquireAsync("grpc", "get", "grpc:client-a", cancellationToken);
        using (secondLease)
            _ = await Assert.That(decision.IsAccepted).IsTrue();
    }

    /// <summary>Verifies a lease releases the client entry it acquired, so a replacement entry keeps its own in-flight count and per-client limit.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeaseReleasesAcquiredClientEntry(CancellationToken cancellationToken)
    {
        var time = new FakeTimeProvider();
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 6,
                MaxQueue = 1,
                SlowdownThreshold = 3,
                RejectThreshold = 6,
                MaxSlowdownDelay = TimeSpan.FromSeconds(5),
                MaxQueueWait = TimeSpan.FromSeconds(10),
                PerClientMaxInFlight = 2,
            },
            new BackpressureMetrics(_testMeter),
            time);

        var first = (await gate.AcquireAsync("grpc", "get", "grpc:x", cancellationToken)).Lease;
        var other1 = (await gate.AcquireAsync("grpc", "get", "grpc:y1", cancellationToken)).Lease;
        var other2 = (await gate.AcquireAsync("grpc", "get", "grpc:y2", cancellationToken)).Lease;

        // The slowed request has already resolved its client entry; releasing the first lease then detaches that entry.
        var slowed = gate.AcquireAsync("grpc", "get", "grpc:x", cancellationToken).AsTask();
        _ = await Assert.That(slowed.IsCompleted).IsFalse();
        first.Dispose();

        var (thirdDecision, third) = await gate.AcquireAsync("grpc", "get", "grpc:x", cancellationToken);
        time.Advance(TimeSpan.FromSeconds(10));
        var (slowedDecision, slowedLease) = await slowed.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        slowedLease.Dispose();
        other1.Dispose();
        other2.Dispose();

        var (fourthDecision, fourth) = await gate.AcquireAsync("grpc", "get", "grpc:x", cancellationToken);
        var (fifthDecision, fifth) = await gate.AcquireAsync("grpc", "get", "grpc:x", cancellationToken);
        third.Dispose();
        fourth.Dispose();
        fifth.Dispose();

        _ = await Assert.That(thirdDecision.IsAccepted).IsTrue();
        _ = await Assert.That(slowedDecision.IsAccepted).IsTrue();
        _ = await Assert.That(fourthDecision.IsAccepted).IsTrue();
        _ = await Assert.That(fifthDecision.RejectReason).IsEqualTo("client_concurrency_limit");
    }

    /// <summary>Verifies requests are rejected once the hard threshold is reached while another request is queued.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QueueFullRejectsImmediately(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 1,
                MaxQueue = 1,
                SlowdownThreshold = 1,
                RejectThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(200),
            },
            new BackpressureMetrics(meter));

        var first = (await gate.AcquireAsync("grpc", "insert", "grpc:client-a", cancellationToken)).Lease;
        using var secondCts = new CancellationTokenSource();

        // With MaxSlowdownDelay = 0 the queued acquire runs synchronously up to the queue-slot
        // await, so client-b is already counted in the queue depth (incremented before that
        // await suspends) by the time the task is created - no wall-clock delay is needed.
        var secondAcquire = gate.AcquireAsync("grpc", "insert", "grpc:client-b", secondCts.Token).AsTask();

        var (decision, rejectedLease) = await gate.AcquireAsync("grpc", "insert", "grpc:client-c", cancellationToken);
        rejectedLease.Dispose();

        // Check the reason first so a recurring flake reports the observed rejection instead of
        // a bare boolean failure.
        _ = await Assert.That(decision.RejectReason).IsEqualTo("hard_threshold");
        _ = await Assert.That(decision.IsAccepted).IsFalse();
        _ = await Assert.That(sink.HasEvent("squirix_backpressure_reject_total", ("transport", "grpc"), ("op", "insert"), ("reason", "hard_threshold"))).IsTrue();

        await secondCts.CancelAsync();
        await secondAcquire.WaitUntilAsync(static t => t.IsCompleted, cancellationToken);
        _ = await Assert.That(secondAcquire.IsCanceled).IsTrue();
        first.Dispose();
    }

    /// <summary>Verifies a queued request is rejected after exceeding the configured queue wait budget.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QueueTimeoutRejectsAndEmitsMetrics(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 1,
                MaxQueue = 1,
                SlowdownThreshold = 1,
                RejectThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(40),
            },
            new BackpressureMetrics(meter));

        using var lease = (await gate.AcquireAsync("rest", "get", "rest:client-a", cancellationToken)).Lease;

        var (decision, queuedLease) = await gate.AcquireAsync("rest", "get", "rest:client-b", cancellationToken);
        queuedLease.Dispose();

        _ = await Assert.That(decision.IsAccepted).IsFalse();
        _ = await Assert.That(decision.RejectReason).IsEqualTo("queue_wait_timeout");
        _ = await Assert.That(sink.HasEvent("squirix_backpressure_reject_total", ("transport", "rest"), ("op", "get"), ("reason", "queue_wait_timeout"))).IsTrue();
        _ = await Assert.That(sink.HasEvent("squirix_backpressure_queue_timeouts_total", ("transport", "rest"), ("op", "get"))).IsTrue();
    }

    /// <summary>Verifies a queued acquire completes after a held lease is released.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QueuedAcquireCompletesAfterLeaseRelease(CancellationToken cancellationToken)
    {
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 1,
                MaxQueue = 1,
                SlowdownThreshold = 1,
                RejectThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(500),
            },
            new BackpressureMetrics(_testMeter));

        var first = (await gate.AcquireAsync("grpc", "insert", "grpc:client-a", cancellationToken)).Lease;
        var queuedTask = gate.AcquireAsync("grpc", "insert", "grpc:client-b", cancellationToken).AsTask();

        _ = await Assert.That(queuedTask.IsCompleted).IsFalse();
        first.Dispose();

        var (decision, secondLease) = await queuedTask;
        using (secondLease)
            _ = await Assert.That(decision.IsAccepted).IsTrue();
    }

    /// <summary>Verifies queued admission observes caller cancellation and records queue cancellation metrics.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QueuedAcquireObservesCallerCancellation(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 1,
                MaxQueue = 1,
                SlowdownThreshold = 1,
                RejectThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromSeconds(2),
            },
            new BackpressureMetrics(meter));

        using var heldLease = (await gate.AcquireAsync("rest", "remove", "rest:client-a", cancellationToken)).Lease;
        using var cts = new CancellationTokenSource();
        var queuedTask = gate.AcquireAsync("rest", "remove", "rest:client-b", cts.Token).AsTask();

        _ = await Assert.That(queuedTask.IsCompleted).IsFalse();
        await cts.CancelAsync();

        await queuedTask.WaitUntilAsync(static t => t.IsCompleted, cancellationToken);
        _ = await Assert.That(queuedTask.IsCanceled).IsTrue();
        _ = await Assert.That(sink.HasEvent("squirix_backpressure_queue_cancellations_total", ("transport", "rest"), ("op", "remove"))).IsTrue();
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static IHttpContextAccessor CreateAccessorFor(string subject)
    {
        var context = new DefaultHttpContext { User = new ClaimsPrincipal(new ClaimsIdentity([new Claim("sub", subject)], "Bearer")) };
        var expectations = new IHttpContextAccessorCreateExpectations();
        _ = expectations.Setups.HttpContext.Gets().ReturnValue(context);
        _ = expectations.Setups.HttpContext.Sets(Arg.Any<HttpContext?>());
        return expectations.Instance();
    }

    private static async Task RunClientAsync(IBackpressureGate gate, int clientIndex, int[] current, int[] observedMax, CancellationToken cancellationToken)
    {
        var (decision, lease) = await gate.AcquireAsync("grpc", "insert", $"grpc:client-{NodeInvariantIndexStrings.Format(clientIndex)}", cancellationToken);
        if (!decision.IsAccepted)
            return;

        using (lease)
        {
            var now = Interlocked.Increment(ref MemoryMarshal.GetArrayDataReference(current));
            UpdateMax(now, ref MemoryMarshal.GetArrayDataReference(observedMax));

            try
            {
                await Task.Yield();
            }
            finally
            {
                _ = Interlocked.Decrement(ref MemoryMarshal.GetArrayDataReference(current));
            }
        }
    }

    private static void UpdateMax(int candidate, ref int target)
    {
        while (true)
        {
            var current = Volatile.Read(ref target);
            if (candidate <= current)
                return;

            if (Interlocked.CompareExchange(ref target, candidate, current) == current)
                return;
        }
    }
}
