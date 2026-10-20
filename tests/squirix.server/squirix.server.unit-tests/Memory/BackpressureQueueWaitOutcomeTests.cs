using System;
using System.Diagnostics.Metrics;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Unit tests for how a queued admission ends: timeout, caller cancellation, disposal and the race between a release and the timeout.</summary>
[Immutable]
public sealed class BackpressureQueueWaitOutcomeTests
{
    private const string Transport = "rest";
    private const string Operation = "get";
    private const int RaceIterations = 300;
    private static readonly AsyncLocal<bool> TrackedFlow = new();
    private static readonly TimeSpan MaxQueueWait = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(10);

    /// <summary>A queue wait timeout rejects the request as an ordinary outcome and raises no first-chance cancellation on the gate path.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QueueWaitTimeoutRejectsWithoutException(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        using var gate = CreateGate(meter, clock);
        var held = (await gate.AcquireAsync(Transport, Operation, "rest:client-0", cancellationToken)).Lease;

        // The marker flows with the execution context of this test only, so exceptions of tests running in parallel are not counted.
        TrackedFlow.Value = true;
        var queued = StartAcquireAsync(gate, "rest:client-1", cancellationToken);
        _ = await Assert.That(gate.QueueDepth).IsEqualTo(1);

        var gateCancellations = 0;

        void OnFirstChance(object? sender, FirstChanceExceptionEventArgs args)
        {
            if (args.Exception is OperationCanceledException && TrackedFlow.Value)
                _ = Interlocked.Increment(ref gateCancellations);
        }

        AppDomain.CurrentDomain.FirstChanceException += OnFirstChance;
        try
        {
            clock.Advance(MaxQueueWait);
            var (decision, lease) = await queued.WaitAsync(TestTimeout, TimeProvider.System, cancellationToken);
            lease.Dispose();
            _ = await Assert.That(decision.RejectReason).IsEqualTo("queue_wait_timeout");
        }
        finally
        {
            AppDomain.CurrentDomain.FirstChanceException -= OnFirstChance;
        }

        _ = await Assert.That(Volatile.Read(ref gateCancellations)).IsEqualTo(0);
        _ = await Assert.That(sink.HasEvent("squirix_backpressure_queue_timeouts_total", ("transport", Transport), ("op", Operation))).IsTrue();
        _ = await Assert.That(sink.HasEvent("squirix_backpressure_reject_total", ("transport", Transport), ("op", Operation), ("reason", "queue_wait_timeout"))).IsTrue();
        _ = await Assert.That(gate.QueueDepth).IsEqualTo(0);

        held.Dispose();
        _ = await Assert.That(gate.InFlight).IsEqualTo(0);
    }

    /// <summary>A caller that cancels while queued still observes the cancellation and the gate counts it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CallerCancelWhileQueuedIsCounted(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        using var gate = CreateGate(meter, clock);
        var held = (await gate.AcquireAsync(Transport, Operation, "rest:client-0", cancellationToken)).Lease;
        using var callerCts = new CancellationTokenSource();
        var queued = StartAcquireAsync(gate, "rest:client-1", callerCts.Token);

        await callerCts.CancelAsync();
        await queued.WaitUntilAsync(static t => t.IsCompleted, cancellationToken);
        _ = await Assert.That(queued.IsCanceled).IsTrue();

        _ = await Assert.That(sink.HasEvent("squirix_backpressure_queue_cancellations_total", ("transport", Transport), ("op", Operation))).IsTrue();
        _ = await Assert.That(sink.HasEvent("squirix_backpressure_queue_timeouts_total", ("transport", Transport), ("op", Operation))).IsFalse();
        _ = await Assert.That(gate.QueueDepth).IsEqualTo(0);

        held.Dispose();
        _ = await Assert.That(gate.InFlight).IsEqualTo(0);
    }

    /// <summary>A request queued when the gate is disposed is rejected as disposed instead of failing the caller.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposalWhileQueuedRejectsAsDisposed(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        using var gate = CreateGate(meter, clock);
        var held = (await gate.AcquireAsync(Transport, Operation, "rest:client-0", cancellationToken)).Lease;
        var queued = StartAcquireAsync(gate, "rest:client-1", cancellationToken);

        DisposeGate(gate);
        var (decision, lease) = await queued;
        lease.Dispose();
        held.Dispose();

        _ = await Assert.That(decision.RejectReason).IsEqualTo("gate_disposed");
        _ = await Assert.That(sink.HasEvent("squirix_backpressure_reject_total", ("transport", Transport), ("op", Operation), ("reason", "gate_disposed"))).IsTrue();
    }

    /// <summary>A timeout racing the release of the only slot admits or rejects the waiter but never leaks or duplicates the slot.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TimeoutRacingReleaseKeepsSlotCount(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        using var meter = new Meter("Squirix");
        using var gate = CreateGate(meter, clock);

        for (var iteration = 0; iteration < RaceIterations; iteration++)
        {
            var held = (await gate.AcquireAsync(Transport, Operation, "rest:client-0", cancellationToken).AsTask().WaitAsync(TestTimeout, TimeProvider.System, cancellationToken)).Lease;
            var queued = StartAcquireAsync(gate, "rest:client-1", cancellationToken);
            var race = new RaceState(held, clock);
            var releasing = Task.Factory.StartNew(static state => (state as RaceState)?.Release(), race, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            var expiring = Task.Factory.StartNew(static state => (state as RaceState)?.Expire(), race, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            race.Start.Set();
            await Task.WhenAll(releasing, expiring).WaitAsync(TestTimeout, TimeProvider.System, cancellationToken);
            race.Start.Dispose();
            var (decision, lease) = await queued.WaitAsync(TestTimeout, TimeProvider.System, cancellationToken);
            _ = await Assert.That(decision.IsAccepted || string.Equals(decision.RejectReason, "queue_wait_timeout", StringComparison.Ordinal)).IsTrue();
            lease.Dispose();

            _ = await Assert.That(gate.InFlight).IsEqualTo(0);
            _ = await Assert.That(gate.QueueDepth).IsEqualTo(0);
        }

        // Exactly one permit must remain: the first request is admitted at once and the second has to queue.
        var first = (await gate.AcquireAsync(Transport, Operation, "rest:client-0", cancellationToken)).Lease;
        var second = StartAcquireAsync(gate, "rest:client-1", cancellationToken);
        _ = await Assert.That(second.IsCompleted).IsFalse();

        first.Dispose();
        var (decision2, lease2) = await second.WaitAsync(TestTimeout, TimeProvider.System, cancellationToken);
        _ = await Assert.That(decision2.IsAccepted).IsTrue();
        lease2.Dispose();
        _ = await Assert.That(gate.InFlight).IsEqualTo(0);
    }

    private static AdmissionGate CreateGate(Meter meter, TimeProvider timeProvider) =>
        new(
            new AdmissionOptions
            {
                MaxInFlight = 1,
                MaxQueue = 2,
                SlowdownThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = MaxQueueWait,
            },
            new BackpressureMetrics(meter),
            timeProvider);

    private static void DisposeGate(AdmissionGate gate) => gate.Dispose();

    private static Task<(Decision Decision, Lease Lease)> StartAcquireAsync(AdmissionGate gate, string clientId, CancellationToken cancellationToken) =>
        gate.AcquireAsync(Transport, Operation, clientId, cancellationToken).AsTask();

    private sealed class RaceState
    {
        private readonly FakeTimeProvider _clock;
        private readonly Lease _held;

        internal RaceState(Lease held, FakeTimeProvider clock)
        {
            _held = held;
            _clock = clock;
        }

        internal ManualResetEventSlim Start { get; } = new(false);

        internal void Release()
        {
            Start.Wait(CancellationToken.None);
            _held.Dispose();
        }

        internal void Expire()
        {
            Start.Wait(CancellationToken.None);
            _clock.Advance(MaxQueueWait);
        }
    }
}
