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

/// <summary>Unit tests for the per-client reservation and client entry lifetime of node-level backpressure.</summary>
[Immutable]
public sealed class BackpressureClientAdmissionTests : DisposableServerUnitTestBase
{
    private readonly Meter _testMeter = new("test");

    /// <summary>Verifies a request cancelled while slowed down does not leave its client entry behind.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancelledSlowdownLeavesNoClientEntry(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        using var gate = CreateGate(
            clock,
            new AdmissionOptions
            {
                MaxInFlight = 2,
                MaxQueue = 2,
                SlowdownThreshold = 1,
                MaxSlowdownDelay = TimeSpan.FromSeconds(1),
                PerClientMaxInFlight = 1,
            });
        var held = (await gate.AcquireAsync("rest", "get", "rest:client-b", cancellationToken)).Lease;
        using var cts = new CancellationTokenSource();
        var sleeping = StartAcquireAsync(gate, "rest:client-a", cts.Token);
        _ = await Assert.That(gate.TrackedClients).IsEqualTo(2);

        await cts.CancelAsync();
        await sleeping.WaitUntilAsync(static t => t.IsCompleted, cancellationToken);

        _ = await Assert.That(sleeping.IsCanceled).IsTrue();
        _ = await Assert.That(gate.TrackedClients).IsEqualTo(1);
        held.Dispose();
    }

    /// <summary>Verifies workers churning acquire and release on one client never hold more than the per-client limit at once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ChurnNeverExceedsClientLimit(CancellationToken cancellationToken)
    {
        using var gate = CreateGate(
            TimeProvider.System,
            new AdmissionOptions
            {
                MaxInFlight = 8,
                MaxQueue = 8,
                SlowdownThreshold = 8,
                MaxSlowdownDelay = TimeSpan.Zero,
                PerClientMaxInFlight = 1,
            });

        // Element 0 is the number of leases held right now, element 1 stays 0 unless more than one was ever held at once.
        var held = new int[2];
        await Parallel.ForEachAsync(
            [0, 1, 2, 3, 4, 5, 6, 7],
            cancellationToken,
            async (__, token) =>
            {
                for (var i = 0; i < 200; i++)
                {
                    var (decision, lease) = await gate.AcquireAsync("rest", "get", "rest:client-a", token);
                    if (!decision.IsAccepted)
                        continue;

                    var now = Interlocked.Increment(ref held[0]);
                    if (now > 1)
                        _ = Interlocked.Exchange(ref held[1], now);

                    await Task.Yield();
                    _ = Interlocked.Decrement(ref held[0]);
                    lease.Dispose();
                }
            });

        _ = await Assert.That(Volatile.Read(ref held[1])).IsEqualTo(0);
        _ = await Assert.That(gate.TrackedClients).IsEqualTo(0);
    }

    /// <summary>Verifies a queued request rejected by gate disposal gives its client entry back.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ClientReservationReleasedOnGateDisposal(CancellationToken cancellationToken)
    {
        using var gate = CreateGate(
            new FakeTimeProvider(),
            new AdmissionOptions
            {
                MaxInFlight = 1,
                MaxQueue = 1,
                SlowdownThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMinutes(1),
                PerClientMaxInFlight = 1,
            });
        var held = (await gate.AcquireAsync("rest", "get", "rest:client-b", cancellationToken)).Lease;
        var queued = gate.AcquireAsync("rest", "get", "rest:client-a", cancellationToken).AsTask();
        _ = await Assert.That(gate.TrackedClients).IsEqualTo(2);

        gate.Dispose();
        var (decision, lease) = await queued.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        lease.Dispose();

        _ = await Assert.That(decision.RejectReason).IsEqualTo("gate_disposed");
        _ = await Assert.That(gate.TrackedClients).IsEqualTo(1);
        held.Dispose();
        _ = await Assert.That(gate.TrackedClients).IsEqualTo(0);
    }

    /// <summary>Verifies requests rejected before any reservation do not leave client entries behind.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EarlyRejectionsLeaveNoClientEntries(CancellationToken cancellationToken)
    {
        using var gate = CreateGate(
            TimeProvider.System,
            new AdmissionOptions
            {
                MaxInFlight = 1,
                MaxQueue = 0,
                SlowdownThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
                PerClientMaxInFlight = 1,
            });
        var held = (await gate.AcquireAsync("rest", "get", "rest:client-b", cancellationToken)).Lease;
        var baseline = gate.TrackedClients;

        for (var i = 0; i < 5; i++)
        {
            var (decision, lease) = await gate.AcquireAsync("rest", "get", $"rest:client-{NodeInvariantIndexStrings.Format(i)}", cancellationToken);
            lease.Dispose();
            _ = await Assert.That(decision.RejectReason).IsEqualTo("queue_full");
        }

        _ = await Assert.That(gate.TrackedClients).IsEqualTo(baseline);
        held.Dispose();
    }

    /// <summary>Verifies a client at its limit leaves slowdown capacity for another client.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ClientLimitPreservesSlowdownCapacity(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        using var gate = CreateGate(
            clock,
            new AdmissionOptions
            {
                MaxInFlight = 2,
                MaxQueue = 1,
                SlowdownThreshold = 1,
                MaxSlowdownDelay = TimeSpan.FromSeconds(1),
                MaxQueueWait = TimeSpan.FromMinutes(1),
                PerClientMaxInFlight = 1,
            });
        using var held = (await gate.AcquireAsync("rest", "get", "rest:client-a", cancellationToken)).Lease;
        using var requests = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
#pragma warning disable CA2025 // Both requests are canceled and awaited in the finally block before disposal.
        var sameClient = StartAcquireAsync(gate, "rest:client-a", requests.Token);
        var queueDepthAfterSameClient = gate.QueueDepth;
        var otherClient = StartAcquireAsync(gate, "rest:client-b", requests.Token);
#pragma warning restore CA2025
        var completed = Task.WhenAll(sameClient, otherClient);
        try
        {
            clock.Advance(TimeSpan.FromSeconds(1));
            var results = await completed;
            using var sameClientLease = results[0].Lease;
            using var otherClientLease = results[1].Lease;

            _ = await Assert.That(results[0].Decision.RejectReason).IsEqualTo("client_concurrency_limit");
            _ = await Assert.That(queueDepthAfterSameClient).IsEqualTo(0);
            _ = await Assert.That(results[1].Decision.IsAccepted).IsTrue();
        }
        finally
        {
            await requests.CancelAsync();
            Task drained = completed;
            await drained.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }
    }

    /// <summary>Verifies parallel requests of one client admit exactly one and reject the rest at the per-client limit.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ParallelRequestsAdmitExactlyOne(CancellationToken cancellationToken)
    {
        using var gate = CreateGate(
            TimeProvider.System,
            new AdmissionOptions
            {
                MaxInFlight = 16,
                MaxQueue = 16,
                SlowdownThreshold = 16,
                MaxSlowdownDelay = TimeSpan.Zero,
                PerClientMaxInFlight = 1,
            });
        var results = new (Decision Decision, Lease Lease)[16];
        await Parallel.ForEachAsync(
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            cancellationToken,
            async (index, token) => results[index] = await gate.AcquireAsync("rest", "get", "rest:client-a", token));
        var admitted = 0;
        var limited = 0;
        foreach (var (decision, lease) in results)
        {
            lease.Dispose();
            if (decision.IsAccepted)
                admitted++;
            else if (string.Equals(decision.RejectReason, "client_concurrency_limit", StringComparison.Ordinal))
                limited++;
        }

        _ = await Assert.That(admitted).IsEqualTo(1);
        _ = await Assert.That(limited).IsEqualTo(15);
        _ = await Assert.That(gate.TrackedClients).IsEqualTo(0);
    }

    /// <summary>A request rejected while slowdown holds the queue leaves no client state behind.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QueueRejectionReleasesClientTest(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        using var gate = CreateGate(
            clock,
            new AdmissionOptions
            {
                MaxInFlight = 1,
                MaxQueue = 1,
                SlowdownThreshold = 1,
                MaxSlowdownDelay = TimeSpan.FromSeconds(1),
                MaxQueueWait = TimeSpan.FromMinutes(1),
                PerClientMaxInFlight = 1,
            });
        var held = (await gate.AcquireAsync("rest", "get", "rest:client-b", cancellationToken)).Lease;
        using var attemptACts = new CancellationTokenSource();
        using var attemptCCts = new CancellationTokenSource();
        var attemptA = StartAcquireAsync(gate, "rest:client-a", attemptACts.Token);
        var attemptC = StartAcquireAsync(gate, "rest:client-c", attemptCCts.Token);

        clock.Advance(TimeSpan.FromSeconds(1));
        var tasks = new[] { attemptA, attemptC };
        await tasks.WaitUntilAsync(static t => t[0].IsCompleted || t[1].IsCompleted, cancellationToken);
        var rejected = attemptA.IsCompleted ? attemptA : attemptC;
        var queued = attemptA.IsCompleted ? attemptC : attemptA;
        var (decision, rejectedLease) = await rejected;
        rejectedLease.Dispose();
        _ = await Assert.That(decision.RejectReason).IsEqualTo("queue_full");

        await attemptACts.CancelAsync();
        await attemptCCts.CancelAsync();
        await attemptA.WaitUntilAsync(static t => t.IsCompleted, cancellationToken);
        await attemptC.WaitUntilAsync(static t => t.IsCompleted, cancellationToken);
        _ = await Assert.That(queued.IsCanceled).IsTrue();
        held.Dispose();

        var (retryA, leaseA) = await gate.AcquireAsync("rest", "get", "rest:client-a", cancellationToken);
        leaseA.Dispose();
        var (retryC, leaseC) = await gate.AcquireAsync("rest", "get", "rest:client-c", cancellationToken);
        leaseC.Dispose();

        _ = await Assert.That(retryA.IsAccepted).IsTrue();
        _ = await Assert.That(retryC.IsAccepted).IsTrue();
        _ = await Assert.That(gate.TrackedClients).IsEqualTo(0);
    }

    /// <summary>Slowdown shares the queue capacity and cancellation releases its reservation.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SlowdownRespectsCapacityTest(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        using var gate = CreateGate(clock, new AdmissionOptions
        {
            MaxInFlight = 2,
            MaxQueue = 1,
            SlowdownThreshold = 1,
            MaxSlowdownDelay = TimeSpan.FromSeconds(1),
            PerClientMaxInFlight = 1,
        });
        using var held = (await gate.AcquireAsync("rest", "get", "holder", cancellationToken)).Lease;
        using var cancellation = new CancellationTokenSource();
        var waits = new Task<(Decision Decision, Lease Lease)>[64];
        try
        {
#pragma warning disable CA2025 // Every started wait is canceled and awaited in the finally block before disposal.
            _ = Parallel.For(0, waits.Length, i => waits[i] = StartAcquireAsync(gate, $"client-{i}", cancellation.Token));
#pragma warning restore CA2025

            var pending = 0;
            foreach (var wait in waits)
            {
                if (!wait.IsCompleted)
                    pending++;
            }

            _ = await Assert.That(pending).IsEqualTo(1);
            _ = await Assert.That(gate.InFlight).IsEqualTo(1);
            _ = await Assert.That(gate.QueueDepth).IsEqualTo(1);
            _ = await Assert.That(gate.TrackedClients).IsEqualTo(2);
        }
        finally
        {
            await cancellation.CancelAsync();
            Task drained = Task.WhenAll(waits);
            await drained.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }

        _ = await Assert.That(gate.TrackedClients).IsEqualTo(1);
        _ = await Assert.That(gate.QueueDepth).IsEqualTo(0);
    }

    /// <summary>Disabling queueing skips slowdown while an immediate slot remains available.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ZeroQueueSkipsSlowdownTest(CancellationToken cancellationToken)
    {
        using var gate = CreateGate(new FakeTimeProvider(), new AdmissionOptions
        {
            MaxInFlight = 2,
            MaxQueue = 0,
            SlowdownThreshold = 1,
            MaxSlowdownDelay = TimeSpan.FromSeconds(1),
        });
        using var held = (await gate.AcquireAsync("rest", "get", "holder", cancellationToken)).Lease;
        var (decision, lease) = await StartAcquireAsync(gate, "second", cancellationToken).WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        lease.Dispose();
        _ = await Assert.That(decision.IsAccepted).IsTrue();
        _ = await Assert.That(gate.QueueDepth).IsEqualTo(0);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static Task<(Decision Decision, Lease Lease)> StartAcquireAsync(AdmissionGate gate, string clientId, CancellationToken cancellationToken) =>
        gate.AcquireAsync("rest", "get", clientId, cancellationToken).AsTask();

    private AdmissionGate CreateGate(TimeProvider timeProvider, AdmissionOptions options) => new(options, new BackpressureMetrics(_testMeter), timeProvider);
}
