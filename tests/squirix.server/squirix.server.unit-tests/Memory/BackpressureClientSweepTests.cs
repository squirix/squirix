using System;
using System.Collections.Concurrent;
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

/// <summary>Unit tests for the sweep that evicts idle per-client entries of node-level backpressure.</summary>
[Immutable]
public sealed class BackpressureClientSweepTests : DisposableServerUnitTestBase
{
    private static readonly int[] DecayingTrackedCounts = [3000, 3000, 1500, 750, 376, 120, 0];

    private readonly Meter _testMeter = new("test");

    /// <summary>Verifies a sweep examines at most the maximum batch and carries the remainder over to the next sweeps without waiting.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CappedSweepCarriesRemainderOver(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        using var gate = CreateGate(clock, RateLimitedOptions(1, 2));
        for (var i = 0; i < 40000; i++)
            (await gate.AcquireAsync("rest", "get", $"rest:client-{NodeInvariantIndexStrings.Format(i)}", cancellationToken)).Lease.Dispose();

        clock.Advance(TimeSpan.FromSeconds(1));
        await TriggerSweepAsync(gate, cancellationToken);
        _ = await Assert.That(gate.TrackedClients).IsEqualTo(40000 - 16384);

        // The clock stays put: a capped sweep leaves the next one due at once.
        await TriggerSweepAsync(gate, cancellationToken);
        _ = await Assert.That(gate.TrackedClients).IsEqualTo(40000 - (2 * 16384));

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
            async (__, token) =>
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

    /// <summary>Verifies a sweep after the gate sweeper was disposed neither throws nor touches entries.</summary>
    [Test]
    public async Task SweepAfterDisposeIsHarmless()
    {
        var clock = new FakeTimeProvider();
        var clients = new ConcurrentDictionary<string, AdmissionGate.ClientState>(StringComparer.Ordinal);
        var sweeper = new AdmissionGate.ClientSweeper(clients, clock);
        _ = clients.TryAdd("rest:client-a", new AdmissionGate.ClientState(RateLimitedOptions(1, 2), clock));
        clock.Advance(TimeSpan.FromSeconds(5));
        sweeper.SweepIfDue();
        _ = await Assert.That(clients.Count).IsEqualTo(0);

        _ = clients.TryAdd("rest:client-b", new AdmissionGate.ClientState(RateLimitedOptions(1, 2), clock));
        sweeper.Dispose();
        clock.Advance(TimeSpan.FromSeconds(5));
        sweeper.SweepIfDue();
        sweeper.Dispose();

        _ = await Assert.That(clients.Count).IsEqualTo(1);
    }

    /// <summary>Verifies a burst whose buckets refill slower than the sweep interval keeps a large budget instead of draining at the minimum rate.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SweepBudgetDecaysWhileBucketsRefill(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        using var gate = CreateGate(clock, RateLimitedOptions(1, 4));
        for (var i = 0; i < 3000; i++)
        {
            var clientId = $"rest:client-{NodeInvariantIndexStrings.Format(i)}";
            for (var attempt = 0; attempt < 3; attempt++)
                (await gate.AcquireAsync("rest", "get", clientId, cancellationToken)).Lease.Dispose();
        }

        // Buckets are full again only after the third second, so the first two sweeps keep everything.
        foreach (var tracked in DecayingTrackedCounts)
        {
            clock.Advance(TimeSpan.FromSeconds(1));
            await TriggerSweepAsync(gate, cancellationToken);
            _ = await Assert.That(gate.TrackedClients).IsEqualTo(tracked);
        }
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

    private static Task<(Decision Decision, Lease Lease)> StartAcquireAsync(AdmissionGate gate, string clientId, CancellationToken cancellationToken) =>
        gate.AcquireAsync("rest", "get", clientId, cancellationToken).AsTask();

    /// <summary>Triggers the sweep with an internal owner call, which is never tracked, so it adds no client entry.</summary>
    /// <param name="gate">The gate to sweep.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the call returns.</returns>
    private static async Task TriggerSweepAsync(AdmissionGate gate, CancellationToken cancellationToken) =>
        (await gate.AcquireAsync("rest", "get", HttpContextClientIdResolver.InternalOwnerClientId, cancellationToken)).Lease.Dispose();

    private AdmissionGate CreateGate(TimeProvider timeProvider, AdmissionOptions options) => new(options, new BackpressureMetrics(_testMeter), timeProvider);
}
