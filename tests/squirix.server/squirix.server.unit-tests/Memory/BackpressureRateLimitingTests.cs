using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Unit tests for node-level and per-client backpressure rate limiting.</summary>
[Immutable]
public sealed class BackpressureRateLimitingTests : DisposableServerUnitTestBase
{
    private readonly Meter _testMeter = new("test");

    /// <summary>Verifies node-level rate limiting rejects excess requests and emits a node-scoped metric.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NodeRateLimitRejectsAndEmitsScopeMetric(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 4,
                MaxQueue = 0,
                SlowdownThreshold = 4,
                RejectThreshold = 4,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(100),
                NodeRateLimitPerSecond = 1,
                NodeRateLimitBurst = 1,
            },
            new BackpressureMetrics(meter));

        using var first = (await gate.AcquireAsync("rest", "get", "rest:client-a", cancellationToken)).Lease;

        var (decision, rejectedLease) = await gate.AcquireAsync("rest", "get", "rest:client-b", cancellationToken);
        rejectedLease.Dispose();

        _ = await Assert.That(decision.IsAccepted).IsFalse();
        _ = await Assert.That(decision.RejectReason).IsEqualTo("node_rate_limit");
        _ = await Assert.That(sink.HasEvent("squirix_backpressure_rate_limit_reject_total", ("transport", "rest"), ("op", "get"), ("scope", "node"))).IsTrue();
    }

    /// <summary>Verifies a single client cannot monopolize node slots beyond its configured concurrency budget.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PerClientCapRejectsWhenNodeExhausted(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 4,
                PerClientMaxInFlight = 1,
                PerClientMaxQueue = 0,
                MaxQueue = 4,
                SlowdownThreshold = 4,
                RejectThreshold = 4,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(100),
            },
            new BackpressureMetrics(meter));

        using var first = (await gate.AcquireAsync("grpc", "get", "grpc:client-a", cancellationToken)).Lease;

        var (decision, rejectedLease) = await gate.AcquireAsync("grpc", "get", "grpc:client-a", cancellationToken);
        rejectedLease.Dispose();

        _ = await Assert.That(decision.IsAccepted).IsFalse();
        _ = await Assert.That(decision.RejectReason).IsEqualTo("client_queue_full");
        _ = await Assert.That(sink.HasEvent("squirix_backpressure_reject_total", ("transport", "grpc"), ("op", "get"), ("reason", "client_queue_full"))).IsTrue();
    }

    /// <summary>Verifies per-client rate limiting rejects one client without blocking unrelated clients.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PerClientRateLimitIsolatedByClient(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 4,
                MaxQueue = 0,
                SlowdownThreshold = 4,
                RejectThreshold = 4,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(100),
                PerClientRateLimitPerSecond = 1,
                PerClientRateLimitBurst = 1,
            },
            new BackpressureMetrics(meter));

        using var first = (await gate.AcquireAsync("grpc", "get", "grpc:client-a", cancellationToken)).Lease;

        var (rejectedDecision, rejectedLease) = await gate.AcquireAsync("grpc", "get", "grpc:client-a", cancellationToken);
        rejectedLease.Dispose();

        using var secondClient = (await gate.AcquireAsync("grpc", "get", "grpc:client-b", cancellationToken)).Lease;

        _ = await Assert.That(rejectedDecision.IsAccepted).IsFalse();
        _ = await Assert.That(rejectedDecision.RejectReason).IsEqualTo("client_rate_limit");
        _ = await Assert.That(sink.HasEvent("squirix_backpressure_rate_limit_reject_total", ("transport", "grpc"), ("op", "get"), ("scope", "client"))).IsTrue();
    }

    /// <summary>Verifies the slowdown counter is emitted when load crosses the soft threshold.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SlowdownCounterIncrementsPastThreshold(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 2,
                MaxQueue = 1,
                SlowdownThreshold = 1,
                RejectThreshold = 2,
                MaxSlowdownDelay = TimeSpan.FromMilliseconds(5),
                MaxQueueWait = TimeSpan.FromMilliseconds(100),
            },
            new BackpressureMetrics(meter));

        using var first = (await gate.AcquireAsync("rest", "put", "rest:client-a", cancellationToken)).Lease;
        using var second = (await gate.AcquireAsync("rest", "put", "rest:client-b", cancellationToken)).Lease;

        _ = await Assert.That(sink.HasEvent("squirix_backpressure_slowdown_total", ("transport", "rest"), ("op", "put"))).IsTrue();
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();
}
