using System;
using System.Diagnostics.Metrics;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Expiry and eviction order tests for <see cref="RpcMutationIdempotencyStore" />.</summary>
[Immutable]
public sealed class RpcMutationIdempotencyExpiryTests : DisposableServerUnitTestBase
{
    private static readonly TimeSpan Retention = TimeSpan.FromMinutes(10);
    private static readonly TimeSpan MinRetention = TimeSpan.FromMinutes(1);

    private static readonly byte[] ResponseBytes = IdempotencyResponseCodec.SerializeResponseBytes(new TryAddAsyncResponse { Added = true });

    private readonly Meter _testMeter = new("test");

    /// <summary>A sweep removes only the records past the retention, oldest first.</summary>
    [Test]
    public async Task SweepRemovesOnlyExpiredRecordsInOrder()
    {
        var clock = new FakeTimeProvider();
        var store = CreateStore(clock, 100);
        store.RecordSuccess("op-1", "fp-1", ResponseBytes, null);
        clock.Advance(TimeSpan.FromMinutes(4));
        store.RecordSuccess("op-2", "fp-2", ResponseBytes, null);
        clock.Advance(TimeSpan.FromMinutes(4));
        store.RecordSuccess("op-3", "fp-3", ResponseBytes, null);

        clock.Advance(TimeSpan.FromMinutes(2) + TimeSpan.FromSeconds(1));
        store.SweepExpired();
        var afterFirst = store.RecordCount;
        clock.Advance(TimeSpan.FromMinutes(4));
        store.SweepExpired();
        var afterSecond = store.RecordCount;
        clock.Advance(TimeSpan.FromMinutes(4));
        store.SweepExpired();

        _ = await Assert.That(afterFirst).IsEqualTo(2);
        _ = await Assert.That(afterSecond).IsEqualTo(1);
        _ = await Assert.That(store.RecordCount).IsEqualTo(0);
    }

    /// <summary>A reservation ages from its completion, not from its acquisition.</summary>
    [Test]
    public async Task CompletionRestartsAging()
    {
        var clock = new FakeTimeProvider();
        var store = CreateStore(clock, 100);
        var execution = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = store.ReserveIntent("op-1", "fp-1", execution, out _);
        clock.Advance(TimeSpan.FromMinutes(6));
        store.RecordSuccess("op-1", "fp-1", ResponseBytes, execution);

        clock.Advance(TimeSpan.FromMinutes(6));
        var replayedBefore = store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out _);
        clock.Advance(TimeSpan.FromMinutes(5));
        var replayedAfter = store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out _);

        _ = await Assert.That(replayedBefore).IsTrue();
        _ = await Assert.That(replayedAfter).IsFalse();
    }

    /// <summary>Stamping and holding an outcome do not change when a reservation expires.</summary>
    [Test]
    public async Task StampAndHoldKeepReservationAging()
    {
        var clock = new FakeTimeProvider();
        var store = CreateStore(clock, 100);
        var execution = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = store.ReserveIntent("op-1", "fp-1", execution, out _);
        clock.Advance(TimeSpan.FromMinutes(6));
        store.MarkStamped("op-1", execution);
        store.HoldAppendedOutcome("op-1", "fp-1", ResponseBytes, execution);

        clock.Advance(TimeSpan.FromMinutes(5));
        store.SweepExpired();

        _ = await Assert.That(store.RecordCount).IsEqualTo(0);
        _ = await Assert.That(store.ExecutionCount).IsEqualTo(0);
    }

    /// <summary>A restored record ages from its own wall date whatever the order it was restored in.</summary>
    [Test]
    public async Task RestoredRecordsExpireByTheirOwnDate()
    {
        var clock = new FakeTimeProvider();
        var store = CreateStore(clock, 100);
        var now = clock.GetUtcNow().UtcDateTime;
        store.RestoreRecord("op-new", "fp-new", ResponseBytes, now.AddMinutes(-1));
        store.RestoreRecord("op-old", "fp-old", ResponseBytes, now.AddMinutes(-9));

        clock.Advance(TimeSpan.FromMinutes(2));
        store.SweepExpired();

        _ = await Assert.That(store.RecordCount).IsEqualTo(1);
        _ = await Assert.That(store.TryReplay("op-new", "fp-new", TryAddAsyncResponse.Parser, out _)).IsTrue();
        _ = await Assert.That(store.TryReplay("op-old", "fp-old", TryAddAsyncResponse.Parser, out _)).IsFalse();
    }

    /// <summary>A restored record replaced by a newer one does not expire with the entry of the record it replaced.</summary>
    [Test]
    public async Task ReplacedRestoredRecordKeepsNewAging()
    {
        var clock = new FakeTimeProvider();
        var store = CreateStore(clock, 100);
        var now = clock.GetUtcNow().UtcDateTime;
        store.RestoreRecord("op-1", "fp-1", ResponseBytes, now.AddMinutes(-9));
        store.RestoreRecord("op-1", "fp-1", ResponseBytes, now);

        clock.Advance(TimeSpan.FromMinutes(2));
        store.SweepExpired();

        _ = await Assert.That(store.RecordCount).IsEqualTo(1);
        _ = await Assert.That(store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out _)).IsTrue();
    }

    /// <summary>A key reserved again after its release does not expire with the entry of the released reservation.</summary>
    [Test]
    public async Task ReacquiredKeyIgnoresReleasedEntry()
    {
        var clock = new FakeTimeProvider();
        var store = CreateStore(clock, 100);
        var first = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = store.ReserveIntent("op-1", "fp-1", first, out _);
        store.ReleaseIntent("op-1", "fp-1", first);
        clock.Advance(TimeSpan.FromMinutes(6));
        var second = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = store.ReserveIntent("op-1", "fp-1", second, out _);

        clock.Advance(TimeSpan.FromMinutes(6));
        store.SweepExpired();

        _ = await Assert.That(store.RecordCount).IsEqualTo(1);
        _ = await Assert.That(store.ExecutionCount).IsEqualTo(1);
    }

    /// <summary>An outcome recorded again after it expired lives a full retention from the new record.</summary>
    [Test]
    public async Task RerecordedKeyAgesFromNewRecord()
    {
        var clock = new FakeTimeProvider();
        var store = CreateStore(clock, 100);
        store.RecordSuccess("op-1", "fp-1", ResponseBytes, null);
        clock.Advance(Retention + TimeSpan.FromSeconds(1));
        store.RecordSuccess("op-1", "fp-1", ResponseBytes, null);

        clock.Advance(Retention - TimeSpan.FromSeconds(2));
        store.SweepExpired();

        _ = await Assert.That(store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out _)).IsTrue();
    }

    /// <summary>A reservation in flight is neither swept before the retention nor evicted by a stream of new operations.</summary>
    [Test]
    public async Task InFlightReservationSurvivesEvictions()
    {
        var clock = new FakeTimeProvider();
        var store = CreateStore(clock, 2);
        var execution = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = store.ReserveIntent("op-0", "fp-0", execution, out _);

        for (var i = 1; i <= 5; i++)
        {
            var id = $"op-{NodeInvariantIndexStrings.FormatD4(i)}";
            _ = store.ReserveIntent(id, "fp", null, out _);
            store.RecordSuccess(id, "fp", ResponseBytes, null);
            clock.Advance(MinRetention + TimeSpan.FromSeconds(1));
        }

        var retry = store.ReserveIntent("op-0", "fp-0", null, out var joined);

        _ = await Assert.That(retry).IsEqualTo(IdempotencyReserveResult.AlreadyStarted);
        _ = await Assert.That(ReferenceEquals(joined, execution.Task)).IsTrue();
    }

    /// <summary>Eviction takes the outcome inserted first among those past the minimum retention.</summary>
    [Test]
    public async Task EvictionTakesOldestEligibleOutcome()
    {
        var clock = new FakeTimeProvider();
        var store = CreateStore(clock, 3);
        store.RecordSuccess("op-1", "fp-1", ResponseBytes, null);
        clock.Advance(TimeSpan.FromSeconds(30));
        store.RecordSuccess("op-2", "fp-2", ResponseBytes, null);
        clock.Advance(TimeSpan.FromSeconds(60));
        store.RecordSuccess("op-3", "fp-3", ResponseBytes, null);
        clock.Advance(TimeSpan.FromSeconds(10));

        _ = store.ReserveIntent("op-4", "fp-4", null, out _);

        _ = await Assert.That(store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out _)).IsFalse();
        _ = await Assert.That(store.TryReplay("op-2", "fp-2", TryAddAsyncResponse.Parser, out _)).IsTrue();
        _ = await Assert.That(store.TryReplay("op-3", "fp-3", TryAddAsyncResponse.Parser, out _)).IsTrue();
    }

    /// <summary>Eviction follows insertion order across outcomes restored from disk and outcomes recorded in this process.</summary>
    [Test]
    public async Task EvictionOrdersRestoredAndLiveOutcomes()
    {
        var clock = new FakeTimeProvider();
        var store = CreateStore(clock, 2);
        var now = clock.GetUtcNow().UtcDateTime;
        store.RecordSuccess("op-live", "fp-live", ResponseBytes, null);
        store.RestoreRecord("op-restored", "fp-restored", ResponseBytes, now.AddMinutes(-5));
        clock.Advance(MinRetention + TimeSpan.FromSeconds(1));

        _ = store.ReserveIntent("op-new", "fp-new", null, out _);

        _ = await Assert.That(store.TryReplay("op-live", "fp-live", TryAddAsyncResponse.Parser, out _)).IsFalse();
        _ = await Assert.That(store.TryReplay("op-restored", "fp-restored", TryAddAsyncResponse.Parser, out _)).IsTrue();

        var secondStore = CreateStore(clock, 2);
        var later = clock.GetUtcNow().UtcDateTime;
        secondStore.RestoreRecord("op-restored", "fp-restored", ResponseBytes, later.AddMinutes(-5));
        secondStore.RecordSuccess("op-live", "fp-live", ResponseBytes, null);
        clock.Advance(MinRetention + TimeSpan.FromSeconds(1));

        _ = secondStore.ReserveIntent("op-new", "fp-new", null, out _);

        _ = await Assert.That(secondStore.TryReplay("op-restored", "fp-restored", TryAddAsyncResponse.Parser, out _)).IsFalse();
        _ = await Assert.That(secondStore.TryReplay("op-live", "fp-live", TryAddAsyncResponse.Parser, out _)).IsTrue();
    }

    /// <summary>Stale order entries left by released reservations stay bounded by the store size.</summary>
    [Test]
    public async Task StaleOrderEntriesStayBounded()
    {
        var clock = new FakeTimeProvider();
        var store = CreateStore(clock, 100);
        _ = store.ReserveIntent("op-keep", "fp", null, out _);

        for (var i = 0; i < 2000; i++)
        {
            var id = $"op-{NodeInvariantIndexStrings.FormatD4(i)}";
            var execution = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _ = store.ReserveIntent(id, "fp", execution, out _);
            store.ReleaseIntent(id, "fp", execution);
        }

        store.SweepExpired();

        _ = await Assert.That(store.RecordCount).IsEqualTo(1);
        _ = await Assert.That(store.OrderEntryCount).IsLessThanOrEqualTo(70);
    }

    /// <summary>A future-dated restored head neither blocks nor slows the eviction of the older restored outcomes behind it.</summary>
    [Test]
    public async Task FutureDatedHeadDoesNotBlockEviction()
    {
        const int old = 300;
        var clock = new FakeTimeProvider();
        var store = CreateStore(clock, old + 1);
        var now = clock.GetUtcNow().UtcDateTime;
        store.RestoreRecord("op-future", "fp-future", ResponseBytes, now.AddMinutes(5));
        for (var i = 0; i < old; i++)
        {
            var id = $"op-{NodeInvariantIndexStrings.FormatD4(i)}";
            store.RestoreRecord(id, "fp", ResponseBytes, now.AddMinutes(-5));
        }

        for (var i = 0; i < old; i++)
        {
            _ = store.ReserveIntent($"new-{NodeInvariantIndexStrings.FormatD4(i)}", "fp", null, out _);

            // Evictions follow insertion order: after i + 1 reservations exactly the first i + 1 old outcomes are gone.
            _ = await Assert.That(store.TryReplay($"op-{NodeInvariantIndexStrings.FormatD4(i)}", "fp", TryAddAsyncResponse.Parser, out _)).IsFalse();
            if (i + 1 < old)
                _ = await Assert.That(store.TryReplay($"op-{NodeInvariantIndexStrings.FormatD4(i + 1)}", "fp", TryAddAsyncResponse.Parser, out _)).IsTrue();
        }

        _ = await Assert.That(store.RecordCount).IsEqualTo(old + 1);
        _ = await Assert.That(store.TryReplay("op-future", "fp-future", TryAddAsyncResponse.Parser, out _)).IsTrue();
        _ = await Assert.That(store.OrderEntryCount).IsLessThanOrEqualTo(5 * ((2 * (old + 1)) + 64));
    }

    /// <summary>A refused reservation leaves young restored outcomes in place and costs no scan of them on the next call.</summary>
    [Test]
    public async Task YoungRestoredOutcomesRefuseWithoutScan()
    {
        const int count = 500;
        var clock = new FakeTimeProvider();
        var store = CreateStore(clock, count);
        var now = clock.GetUtcNow().UtcDateTime;
        for (var i = 0; i < count; i++)
            store.RestoreRecord($"op-{NodeInvariantIndexStrings.FormatD4(i)}", "fp", ResponseBytes, now.AddSeconds(-10));

        for (var i = 0; i < 3; i++)
        {
            var refused = NodeExceptionAssert.For<SquirixException>().Throws(store, static s => s.ReserveIntent("new", "fp", null, out _));
            _ = await Assert.That(refused.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);
        }

        clock.Advance(MinRetention);
        var reserved = store.ReserveIntent("new", "fp", null, out _);

        _ = await Assert.That(reserved).IsEqualTo(IdempotencyReserveResult.Acquired);
        _ = await Assert.That(store.TryReplay("op-0000", "fp", TryAddAsyncResponse.Parser, out _)).IsFalse();
        _ = await Assert.That(store.TryReplay("op-0001", "fp", TryAddAsyncResponse.Parser, out _)).IsTrue();
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private RpcMutationIdempotencyStore CreateStore(TimeProvider clock, int capacity) =>
        new(new IdempotencyOptions { MaxInFlightRecords = capacity, Retention = Retention, MinRetention = MinRetention }, "test-node", new IdempotencyMetrics(_testMeter), clock);
}
