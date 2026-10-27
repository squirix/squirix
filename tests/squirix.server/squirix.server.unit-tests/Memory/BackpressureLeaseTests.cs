using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Unit tests for the release rules of admission leases.</summary>
[Immutable]
public sealed class BackpressureLeaseTests : DisposableServerUnitTestBase
{
    private readonly Meter _testMeter = new("test");

    /// <summary>Verifies a lease value disposed twice fails loudly and leaves the gate capacity intact.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DoubleDisposeThrowsAndKeepsCapacity(CancellationToken cancellationToken)
    {
        using var gate = CreateGate();
        var (_, lease) = await gate.AcquireAsync("grpc", "get", "grpc:client-a", cancellationToken);
        lease.Dispose();

        var failure = NodeExceptionAssert.For<InvalidOperationException>().Throws(lease, static l => l.Dispose());
        var inFlightAfterMisuse = gate.InFlight;
        var (admitted, admittedLease) = await gate.AcquireAsync("grpc", "get", "grpc:client-a", cancellationToken);
        var (rejected, rejectedLease) = await gate.AcquireAsync("grpc", "get", "grpc:client-b", cancellationToken);
        rejectedLease.Dispose();
        admittedLease.Dispose();

        _ = await Assert.That(failure).IsNotNull();
        _ = await Assert.That(inFlightAfterMisuse).IsEqualTo(0);
        _ = await Assert.That(admitted.IsAccepted).IsTrue();
        _ = await Assert.That(rejected.IsAccepted).IsFalse();
        _ = await Assert.That(gate.InFlight).IsEqualTo(0);
    }

    /// <summary>Verifies a copy of a lease disposed after the original also fails loudly.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CopiedLeaseDisposeAfterOriginalThrows(CancellationToken cancellationToken)
    {
        using var gate = CreateGate();
        var (_, lease) = await gate.AcquireAsync("grpc", "get", "grpc:client-a", cancellationToken);
        var copy = lease;
        lease.Dispose();

        var failure = NodeExceptionAssert.For<InvalidOperationException>().Throws(copy, static c => c.Dispose());

        _ = await Assert.That(failure).IsNotNull();
        _ = await Assert.That(gate.InFlight).IsEqualTo(0);
    }

    /// <summary>Verifies the empty lease releases nothing, however often it is disposed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EmptyLeaseDisposeIsNoOp(CancellationToken cancellationToken)
    {
        using var gate = CreateGate();
        var (_, held) = await gate.AcquireAsync("grpc", "get", "grpc:client-a", cancellationToken);
        var empty = Lease.Empty;
        var unset = default(Lease);

        empty.Dispose();
        empty.Dispose();
        unset.Dispose();
        unset.Dispose();

        _ = await Assert.That(gate.InFlight).IsEqualTo(1);
        held.Dispose();
        _ = await Assert.That(gate.InFlight).IsEqualTo(0);
    }

    private AdmissionGate CreateGate() =>
        new(
            new AdmissionOptions
            {
                MaxInFlight = 1,
                MaxQueue = 0,
                SlowdownThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
            },
            new BackpressureMetrics(_testMeter),
            new FakeTimeProvider());
}
