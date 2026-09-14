using System;
using System.Diagnostics.Metrics;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Unit tests for node-level and per-client backpressure rate limiting.</summary>
[Immutable]
public sealed class BackpressureRateLimitingTests : DisposableServerUnitTestBase
{
    private readonly Meter _testMeter = new("test");

    /// <summary>Verifies node-level rate limiting rejects excess requests and emits a node-scoped metric.</summary>
    [Fact]
    public async Task NodeRateLimitRejectsAndEmitsScopeMetric()
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

        using var first = (await gate.AcquireAsync("rest", "get", "rest:client-a", DefaultCancellationToken)).Lease;

        var (decision, rejectedLease) = await gate.AcquireAsync("rest", "get", "rest:client-b", DefaultCancellationToken);
        rejectedLease.Dispose();

        Assert.False(decision.IsAccepted);
        Assert.Equal("node_rate_limit", decision.RejectReason);
        Assert.True(sink.HasEvent("squirix_backpressure_rate_limit_reject_total", ("transport", "rest"), ("op", "get"), ("scope", "node")));
    }

    /// <summary>Verifies a single client cannot monopolize node slots beyond its configured concurrency budget.</summary>
    [Fact]
    public async Task PerClientCapRejectsWhenNodeExhausted()
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

        using var first = (await gate.AcquireAsync("grpc", "get", "grpc:client-a", DefaultCancellationToken)).Lease;

        var (decision, rejectedLease) = await gate.AcquireAsync("grpc", "get", "grpc:client-a", DefaultCancellationToken);
        rejectedLease.Dispose();

        Assert.False(decision.IsAccepted);
        Assert.Equal("client_queue_full", decision.RejectReason);
        Assert.True(sink.HasEvent("squirix_backpressure_reject_total", ("transport", "grpc"), ("op", "get"), ("reason", "client_queue_full")));
    }

    /// <summary>Verifies per-client rate limiting rejects one client without blocking unrelated clients.</summary>
    [Fact]
    public async Task PerClientRateLimitIsolatedByClient()
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

        using var first = (await gate.AcquireAsync("grpc", "get", "grpc:client-a", DefaultCancellationToken)).Lease;

        var (rejectedDecision, rejectedLease) = await gate.AcquireAsync("grpc", "get", "grpc:client-a", DefaultCancellationToken);
        rejectedLease.Dispose();

        using var secondClient = (await gate.AcquireAsync("grpc", "get", "grpc:client-b", DefaultCancellationToken)).Lease;

        Assert.False(rejectedDecision.IsAccepted);
        Assert.Equal("client_rate_limit", rejectedDecision.RejectReason);
        Assert.True(sink.HasEvent("squirix_backpressure_rate_limit_reject_total", ("transport", "grpc"), ("op", "get"), ("scope", "client")));
    }

    /// <summary>Verifies the slowdown counter is emitted when load crosses the soft threshold.</summary>
    [Fact]
    public async Task SlowdownCounterIncrementsPastThreshold()
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

        using var first = (await gate.AcquireAsync("rest", "put", "rest:client-a", DefaultCancellationToken)).Lease;
        using var second = (await gate.AcquireAsync("rest", "put", "rest:client-b", DefaultCancellationToken)).Lease;

        Assert.True(sink.HasEvent("squirix_backpressure_slowdown_total", ("transport", "rest"), ("op", "put")));
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();
}
