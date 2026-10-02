using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The idempotency store ages, sweeps and evicts its records on the server clock, safe from wall-clock steps.</summary>
[Immutable]
public sealed class IdempotencyStoreClockTests : DisposableServerUnitTestBase
{
    private static readonly byte[] ResponseBytes = IdempotencyResponseCodec.SerializeResponseBytes(new TryAddAsyncResponse { Added = true });

    private static readonly TimeSpan Retention = TimeSpan.FromMinutes(15);

    private readonly Meter _testMeter = new("test");

    /// <summary>A record replays while the server clock is within the retention and stops once it passes, with no real delay.</summary>
    [Test]
    public async Task RecordReplaysUntilRetentionPasses()
    {
        var clock = new SteppedWallClock();
        var store = CreateStore(clock, 16);
        store.RecordSuccess("op-1", "fp-1", ResponseBytes, null);

        clock.Advance(TimeSpan.FromMinutes(1));
        var withinRetention = store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out _);
        clock.Advance(Retention);
        var pastRetention = store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out _);

        _ = await Assert.That(withinRetention).IsTrue();
        _ = await Assert.That(pastRetention).IsFalse();
    }

    /// <summary>A forward wall-clock jump past the retention does not purge a record created a second ago, so its retry still replays.</summary>
    [Test]
    public async Task ForwardWallJumpKeepsRecord()
    {
        var clock = new SteppedWallClock();
        var store = CreateStore(clock, 16);
        store.RecordSuccess("op-1", "fp-1", ResponseBytes, null);

        clock.Advance(TimeSpan.FromSeconds(1));
        clock.StepWallClock(TimeSpan.FromMinutes(20));

        _ = await Assert.That(store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out _)).IsTrue();
    }

    /// <summary>After a backward wall-clock step, capacity eviction drops the record inserted first, not the one created after the step.</summary>
    [Test]
    public async Task BackwardWallStepEvictsFirstInserted()
    {
        var clock = new SteppedWallClock();
        var store = CreateStore(clock, 2);
        store.RecordSuccess("op-1", "fp-1", ResponseBytes, null);
        clock.Advance(TimeSpan.FromSeconds(1));
        clock.StepWallClock(TimeSpan.FromMinutes(-10));
        store.RecordSuccess("op-2", "fp-2", ResponseBytes, null);
        clock.Advance(TimeSpan.FromSeconds(1));

        store.RecordSuccess("op-3", "fp-3", ResponseBytes, null);

        _ = await Assert.That(store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out _)).IsFalse();
        _ = await Assert.That(store.TryReplay("op-2", "fp-2", TryAddAsyncResponse.Parser, out _)).IsTrue();
    }

    /// <summary>After a backward wall-clock step, an outcome past the minimum retention on monotonic time is still evicted for a new operation.</summary>
    [Test]
    public async Task BackwardWallStepKeepsEvictionLive()
    {
        var clock = new SteppedWallClock();
        var store = CreateStore(clock, 1);
        store.RecordSuccess("op-1", "fp-1", ResponseBytes, null);
        clock.StepWallClock(TimeSpan.FromHours(-1));
        clock.Advance(TimeSpan.FromMinutes(2));

        var reserved = store.ReserveIntent("op-2", "fp-2", null, out _);

        _ = await Assert.That(reserved).IsEqualTo(IdempotencyReserveResult.Acquired);
        _ = await Assert.That(store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out _)).IsFalse();
    }

    /// <summary>A forward wall-clock step does not age an outcome past the minimum retention: the new operation is refused.</summary>
    [Test]
    public async Task ForwardWallStepKeepsYoungOutcome()
    {
        var clock = new SteppedWallClock();
        var store = CreateStore(clock, 1);
        store.RecordSuccess("op-1", "fp-1", ResponseBytes, null);
        clock.StepWallClock(TimeSpan.FromMinutes(10));

        var refused = NodeExceptionAssert.For<SquirixException>().Throws(store, static s => s.ReserveIntent("op-2", "fp-2", null, out _));

        _ = await Assert.That(refused.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);
        _ = await Assert.That(store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out _)).IsTrue();
    }

    /// <summary>A record restored from disk has no monotonic origin, so it ages by the wall time of the server clock.</summary>
    [Test]
    public async Task RestoredRecordAgesByWallTime()
    {
        var clock = new SteppedWallClock();
        var store = CreateStore(clock, 16);
        store.RestoreRecord("op-1", "fp-1", ResponseBytes, clock.GetUtcNow().UtcDateTime);

        clock.StepWallClock(Retention + TimeSpan.FromMinutes(1));

        _ = await Assert.That(store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out _)).IsFalse();
    }

    /// <summary>A snapshot exports records in insertion order, so a restored store evicts the oldest first even after a removal reused a slot.</summary>
    [Test]
    public async Task RestoreKeepsInsertionOrderForEviction()
    {
        var clock = new SteppedWallClock();
        var source = CreateStore(clock, 16);
        source.RecordSuccess("op-a", "fp-a", ResponseBytes, null);
        _ = source.ReserveIntent("op-b", "fp-b", null, out _);
        source.RecordSuccess("op-c", "fp-c", ResponseBytes, null);
        source.ReleaseIntent("op-b", "fp-b", null);
        source.RecordSuccess("op-d", "fp-d", ResponseBytes, null);
        var exported = new List<PersistedIdempotencyRecord>();
        IIdempotencySnapshotExporter exporter = source;
        exporter.ExportSnapshot(exported, clock.GetUtcNow().UtcDateTime);

        var restored = CreateStore(clock, 3);
        restored.RestoreSnapshotRecords(exported);
        restored.RecordSuccess("op-e", "fp-e", ResponseBytes, null);
        restored.RecordSuccess("op-f", "fp-f", ResponseBytes, null);

        _ = await Assert.That(restored.TryReplay("op-a", "fp-a", TryAddAsyncResponse.Parser, out _)).IsFalse();
        _ = await Assert.That(restored.TryReplay("op-c", "fp-c", TryAddAsyncResponse.Parser, out _)).IsFalse();
        _ = await Assert.That(restored.TryReplay("op-d", "fp-d", TryAddAsyncResponse.Parser, out _)).IsTrue();
    }

    /// <summary>The background sweep ticks on the server clock and drops expired records without a new access.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BackgroundSweepTicksOnServerClock(CancellationToken cancellationToken)
    {
        var clock = new SteppedWallClock();
        var store = CreateStore(clock, 16);
        var options = new IdempotencyOptions { Retention = Retention, BackgroundSweepInterval = TimeSpan.FromMinutes(1) };
        store.RecordSuccess("op-1", "fp-1", ResponseBytes, null);
        using var sweep = new IdempotencyStoreSweepService(store, Options.Create(options), NullLogger<IdempotencyStoreSweepService>.Instance, clock);

        await sweep.StartAsync(cancellationToken);
        await clock.WaitForTimerAsync(cancellationToken);
        clock.Advance(Retention + options.BackgroundSweepInterval);
        await store.WaitUntilAsync(static s => s.RecordCount == 0, cancellationToken);
        await sweep.StopAsync(cancellationToken);

        _ = await Assert.That(store.RecordCount).IsEqualTo(0);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private RpcMutationIdempotencyStore CreateStore(TimeProvider clock, int capacity) =>
        new(new IdempotencyOptions { Retention = Retention, MaxInFlightRecords = capacity }, "local", new IdempotencyMetrics(_testMeter), clock);

    /// <summary>A fake clock whose wall time can step while its monotonic timestamp moves forward only; it signals each timer created on it.</summary>
    [ThreadSafe]
    private sealed class SteppedWallClock : FakeTimeProvider
    {
        private readonly SemaphoreSlim _timerCreated = new(0);
        private long _wallOffsetTicks;

        internal SteppedWallClock()
            : base(new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero))
        {
        }

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = base.CreateTimer(callback, state, dueTime, period);
            _ = _timerCreated.Release();
            return timer;
        }

        public override DateTimeOffset GetUtcNow() => base.GetUtcNow().AddTicks(Interlocked.Read(ref _wallOffsetTicks));

        /// <summary>Reads the unstepped time, since the base derives timestamps from the virtual wall time; the monotonic clock never moves back.</summary>
        /// <returns>The monotonic timestamp.</returns>
        public override long GetTimestamp() => base.GetUtcNow().UtcTicks;

        internal void StepWallClock(TimeSpan step) => _ = Interlocked.Add(ref _wallOffsetTicks, step.Ticks);

        /// <summary>Waits until a timer is created on this clock, so a test advances it only once a wait is armed.</summary>
        /// <param name="cancellationToken">The test cancellation token.</param>
        /// <returns>A task that completes once a timer exists.</returns>
        /// <exception cref="TimeoutException">No timer was created within the real-time bound.</exception>
        internal async Task WaitForTimerAsync(CancellationToken cancellationToken)
        {
            if (!await _timerCreated.WaitAsync(TimeSpan.FromSeconds(10), cancellationToken))
                throw new TimeoutException("no timer was created on the clock.");
        }
    }
}
