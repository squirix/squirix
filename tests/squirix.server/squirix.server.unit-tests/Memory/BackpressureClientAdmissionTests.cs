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

    /// <summary>Verifies a request that loses the authoritative queue check after reserving gives its client reservation back.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QueueStepRejectionReleasesReservation(CancellationToken cancellationToken)
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

    /// <summary>Verifies a queued request rejected by gate disposal gives its client entry back.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ClientReservationReleasedOnGateDisposal(CancellationToken cancellationToken)
    {
        using var gate = CreateGate(
            TimeProvider.System,
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

    /// <summary>Verifies a request that slept on an entry evicted meanwhile cannot take a second place for the same client.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EvictedEntryDoesNotAdmitSecondRequest(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        using var gate = CreateGate(
            clock,
            new AdmissionOptions
            {
                MaxInFlight = 4,
                MaxQueue = 4,
                SlowdownThreshold = 1,
                MaxSlowdownDelay = TimeSpan.FromSeconds(1),
                MaxQueueWait = TimeSpan.FromMinutes(1),
                PerClientMaxInFlight = 1,
            });
        var first = (await gate.AcquireAsync("rest", "get", "rest:client-a", cancellationToken)).Lease;
        var sleeping = StartAcquireAsync(gate, "rest:client-a", cancellationToken);
        _ = await Assert.That(sleeping.IsCompleted).IsFalse();

        first.Dispose();
        var (thirdDecision, thirdLease) = await gate.AcquireAsync("rest", "get", "rest:client-a", cancellationToken);
        _ = await Assert.That(thirdDecision.IsAccepted).IsTrue();

        clock.Advance(TimeSpan.FromSeconds(1));
        var (sleepingDecision, sleepingLease) = await sleeping.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        sleepingLease.Dispose();
        thirdLease.Dispose();

        _ = await Assert.That(sleepingDecision.RejectReason).IsEqualTo("client_concurrency_limit");
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
            async (_, token) =>
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

    /// <summary>Verifies idle client entries with a refilled bucket, including those left by rate-limit rejects, are swept by a later admission.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SweepEvictsRefilledIdleClients(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        using var gate = CreateGate(clock, RateLimitedOptions(1, 2));
        for (var i = 0; i < 10; i++)
        {
            var clientId = $"rest:client-{NodeInvariantIndexStrings.Format(i)}";
            var attempts = i % 2 == 0 ? 3 : 1;
            for (var attempt = 0; attempt < attempts; attempt++)
            {
                var (decision, lease) = await gate.AcquireAsync("rest", "get", clientId, cancellationToken);
                lease.Dispose();
                _ = await Assert.That(decision.IsAccepted || string.Equals(decision.RejectReason, "client_rate_limit", StringComparison.Ordinal)).IsTrue();
            }
        }

        _ = await Assert.That(gate.TrackedClients).IsEqualTo(10);

        clock.Advance(TimeSpan.FromSeconds(3));
        await TriggerSweepAsync(gate, cancellationToken);

        _ = await Assert.That(gate.TrackedClients).IsEqualTo(0);
    }

    /// <summary>Verifies a client entry with an admitted request is never swept even when its bucket has refilled.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SweepKeepsClientWithRequestInFlight(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        using var gate = CreateGate(clock, RateLimitedOptions(1, 4));
        var first = (await gate.AcquireAsync("rest", "get", "rest:client-a", cancellationToken)).Lease;

        clock.Advance(TimeSpan.FromSeconds(1));
        await TriggerSweepAsync(gate, cancellationToken);
        _ = await Assert.That(gate.TrackedClients).IsEqualTo(1);

        var second = (await gate.AcquireAsync("rest", "get", "rest:client-a", cancellationToken)).Lease;
        var third = (await gate.AcquireAsync("rest", "get", "rest:client-a", cancellationToken)).Lease;
        first.Dispose();
        second.Dispose();
        third.Dispose();
        _ = await Assert.That(gate.TrackedClients).IsEqualTo(1);

        clock.Advance(TimeSpan.FromSeconds(3));
        await TriggerSweepAsync(gate, cancellationToken);
        _ = await Assert.That(gate.TrackedClients).IsEqualTo(0);
    }

    /// <summary>Verifies a sweep examines a bounded batch, so many idle entries are evicted over several sweeps.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SweepEvictsInBoundedBatches(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        using var gate = CreateGate(clock, RateLimitedOptions(1, 2));
        for (var i = 0; i < 600; i++)
        {
            var clientId = $"rest:client-{NodeInvariantIndexStrings.Format(i)}";
            (await gate.AcquireAsync("rest", "get", clientId, cancellationToken)).Lease.Dispose();
            (await gate.AcquireAsync("rest", "get", clientId, cancellationToken)).Lease.Dispose();
        }

        // The first sweep sees the new entries, still short of burst, and keeps them; later sweeps have the minimum budget.
        clock.Advance(TimeSpan.FromSeconds(1));
        await TriggerSweepAsync(gate, cancellationToken);
        _ = await Assert.That(gate.TrackedClients).IsEqualTo(600);

        clock.Advance(TimeSpan.FromSeconds(2));
        await TriggerSweepAsync(gate, cancellationToken);
        _ = await Assert.That(gate.TrackedClients).IsEqualTo(344);

        clock.Advance(TimeSpan.FromSeconds(1));
        await TriggerSweepAsync(gate, cancellationToken);
        _ = await Assert.That(gate.TrackedClients).IsEqualTo(88);

        clock.Advance(TimeSpan.FromSeconds(1));
        await TriggerSweepAsync(gate, cancellationToken);
        _ = await Assert.That(gate.TrackedClients).IsEqualTo(0);
    }

    /// <summary>Verifies a sweep examines enough entries to keep up when many entries were added since the previous one.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SweepKeepsUpWithClientChurn(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        using var gate = CreateGate(clock, RateLimitedOptions(1, 2));
        clock.Advance(TimeSpan.FromSeconds(1));
        await TriggerSweepAsync(gate, cancellationToken);

        for (var i = 0; i < 600; i++)
            (await gate.AcquireAsync("rest", "get", $"rest:client-{NodeInvariantIndexStrings.Format(i)}", cancellationToken)).Lease.Dispose();

        _ = await Assert.That(gate.TrackedClients).IsEqualTo(600);

        clock.Advance(TimeSpan.FromSeconds(1));
        await TriggerSweepAsync(gate, cancellationToken);
        _ = await Assert.That(gate.TrackedClients).IsEqualTo(0);
    }

    /// <summary>Verifies a request that resolved its entry before a sweep retired it re-resolves, so the per-client limit still holds.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SweptEntryKeepsClientLimit(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        using var gate = CreateGate(
            clock,
            new AdmissionOptions
            {
                MaxInFlight = 4,
                MaxQueue = 4,
                SlowdownThreshold = 1,
                MaxSlowdownDelay = TimeSpan.FromSeconds(5),
                PerClientMaxInFlight = 1,
                PerClientRateLimitPerSecond = 1,
                PerClientRateLimitBurst = 2,
            });
        var held = (await gate.AcquireAsync("rest", "get", "rest:client-b", cancellationToken)).Lease;
        var sleeping = StartAcquireAsync(gate, "rest:client-a", cancellationToken);
        _ = await Assert.That(gate.TrackedClients).IsEqualTo(2);

        // The sweep runs synchronously inside this call, before it sleeps in its own slowdown.
        clock.Advance(TimeSpan.FromSeconds(1));
        var trigger = StartAcquireAsync(gate, HttpContextClientIdResolver.InternalOwnerClientId, cancellationToken);
        _ = await Assert.That(gate.TrackedClients).IsEqualTo(1);

        clock.Advance(TimeSpan.FromSeconds(5));
        var (firstDecision, firstLease) = await sleeping.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        _ = await Assert.That(firstDecision.IsAccepted).IsTrue();

        var second = StartAcquireAsync(gate, "rest:client-a", cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(5));
        var (secondDecision, secondLease) = await second.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        var (_, triggerLease) = await trigger.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);

        _ = await Assert.That(secondDecision.RejectReason).IsEqualTo("client_concurrency_limit");
        secondLease.Dispose();
        triggerLease.Dispose();
        firstLease.Dispose();
        held.Dispose();
    }

    /// <summary>Verifies a client entry whose bucket has not refilled to burst is kept, so its rate limit is not reset.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SweepKeepsClientWithUnrefilledBucket(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        using var gate = CreateGate(clock, RateLimitedOptions(1, 4));
        for (var i = 0; i < 4; i++)
            (await gate.AcquireAsync("rest", "get", "rest:client-a", cancellationToken)).Lease.Dispose();

        clock.Advance(TimeSpan.FromSeconds(2));
        await TriggerSweepAsync(gate, cancellationToken);
        _ = await Assert.That(gate.TrackedClients).IsEqualTo(1);

        clock.Advance(TimeSpan.FromSeconds(5));
        await TriggerSweepAsync(gate, cancellationToken);
        _ = await Assert.That(gate.TrackedClients).IsEqualTo(0);
    }

    /// <summary>Verifies sweeps running between churning acquires and releases never let one client exceed its concurrency limit.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ChurnWithSweepsNeverExceedsClientLimit(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        using var gate = CreateGate(
            clock,
            new AdmissionOptions
            {
                MaxInFlight = 8,
                MaxQueue = 8,
                SlowdownThreshold = 8,
                MaxSlowdownDelay = TimeSpan.Zero,
                PerClientMaxInFlight = 1,
                PerClientRateLimitPerSecond = 1000,
                PerClientRateLimitBurst = 1000,
            });

        var held = new int[2];
        await Parallel.ForEachAsync(
            [0, 1, 2, 3, 4, 5, 6, 7],
            cancellationToken,
            async (_, token) =>
            {
                for (var i = 0; i < 200; i++)
                {
                    clock.Advance(TimeSpan.FromSeconds(2));
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
        clock.Advance(TimeSpan.FromSeconds(2));
        await TriggerSweepAsync(gate, cancellationToken);
        _ = await Assert.That(gate.TrackedClients).IsEqualTo(0);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static AdmissionOptions RateLimitedOptions(int perSecond, int burst) => new()
    {
        MaxInFlight = 16,
        MaxQueue = 16,
        SlowdownThreshold = 16,
        MaxSlowdownDelay = TimeSpan.Zero,
        PerClientMaxInFlight = 4,
        PerClientRateLimitPerSecond = perSecond,
        PerClientRateLimitBurst = burst,
    };

    /// <summary>Triggers the sweep with an internal owner call, which is never tracked, so it adds no client entry.</summary>
    /// <param name="gate">The gate to sweep.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the call returns.</returns>
    private static async Task TriggerSweepAsync(AdmissionGate gate, CancellationToken cancellationToken) =>
        (await gate.AcquireAsync("rest", "get", HttpContextClientIdResolver.InternalOwnerClientId, cancellationToken)).Lease.Dispose();

    private static Task<(Decision Decision, Lease Lease)> StartAcquireAsync(AdmissionGate gate, string clientId, CancellationToken cancellationToken) =>
        gate.AcquireAsync("rest", "get", clientId, cancellationToken).AsTask();

    private AdmissionGate CreateGate(TimeProvider timeProvider, AdmissionOptions options) => new(options, new BackpressureMetrics(_testMeter), timeProvider);
}
