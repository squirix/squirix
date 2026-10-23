using System;
using System.Diagnostics.Metrics;
using System.Threading.Tasks;
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

/// <summary>Clock step and boundary tests for the expiry and eviction order of <see cref="RpcMutationIdempotencyStore" />.</summary>
[Immutable]
public sealed class RpcMutationIdempotencyBoundaryTests : DisposableServerUnitTestBase
{
    private static readonly TimeSpan Retention = TimeSpan.FromMinutes(10);
    private static readonly TimeSpan MinRetention = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan Tick = TimeSpan.FromMilliseconds(1);

    private static readonly byte[] ResponseBytes = IdempotencyResponseCodec.SerializeResponseBytes(new TryAddAsyncResponse { Added = true });

    private readonly Meter _testMeter = new("test");

    /// <summary>After a backward wall step a record outlives its monotonic age until its wall age passes too.</summary>
    [Test]
    public async Task BackwardWallStepDelaysExpiry()
    {
        var clock = new WallSteppedClock();
        var store = CreateStore(clock, 100);
        store.RecordSuccess("op-1", "fp-1", ResponseBytes, null);
        clock.Advance(Retention + TimeSpan.FromMinutes(1));
        clock.StepWallClock(TimeSpan.FromMinutes(-5));

        store.SweepExpired();
        var afterStep = store.RecordCount;
        clock.Advance(TimeSpan.FromMinutes(4));
        store.SweepExpired();
        var atBoundary = store.RecordCount;
        clock.Advance(Tick);
        store.SweepExpired();

        _ = await Assert.That(afterStep).IsEqualTo(1);
        _ = await Assert.That(atBoundary).IsEqualTo(1);
        _ = await Assert.That(store.RecordCount).IsEqualTo(0);
    }

    /// <summary>After a forward wall step a record survives until its monotonic age passes.</summary>
    [Test]
    public async Task ForwardWallStepWaitsForMonotonicAge()
    {
        var clock = new WallSteppedClock();
        var store = CreateStore(clock, 100);
        store.RecordSuccess("op-1", "fp-1", ResponseBytes, null);
        clock.StepWallClock(TimeSpan.FromHours(1));

        store.SweepExpired();
        var afterStep = store.RecordCount;
        clock.Advance(Retention);
        store.SweepExpired();
        var atBoundary = store.RecordCount;
        clock.Advance(Tick);
        store.SweepExpired();

        _ = await Assert.That(afterStep).IsEqualTo(1);
        _ = await Assert.That(atBoundary).IsEqualTo(1);
        _ = await Assert.That(store.RecordCount).IsEqualTo(0);
    }

    /// <summary>An outcome held before completion dates from the hold on the wall clock and from the completion on the monotonic clock.</summary>
    [Test]
    public async Task HeldOutcomeAgesFromHoldAndCompletion()
    {
        var clock = new WallSteppedClock();
        var store = CreateStore(clock, 100);
        var execution = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = store.ReserveIntent("op-1", "fp-1", execution, out _);
        clock.Advance(TimeSpan.FromMinutes(4));
        store.HoldAppendedOutcome("op-1", "fp-1", ResponseBytes, execution);
        clock.Advance(TimeSpan.FromMinutes(1));
        store.RecordSuccess("op-1", "fp-1", ResponseBytes, execution);

        clock.Advance(TimeSpan.FromMinutes(9) + TimeSpan.FromSeconds(30));
        var beforeMonotonic = store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out _);
        clock.Advance(TimeSpan.FromMinutes(1));
        var afterMonotonic = store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out _);

        _ = await Assert.That(beforeMonotonic).IsTrue();
        _ = await Assert.That(afterMonotonic).IsFalse();
    }

    /// <summary>A future-dated restored record does not hold back the expiry of an older restored record.</summary>
    [Test]
    public async Task FutureDatedRestoredKeepsOlderExpiring()
    {
        var clock = new WallSteppedClock();
        var store = CreateStore(clock, 100);
        var now = clock.GetUtcNow().UtcDateTime;
        store.RestoreRecord("op-future", "fp", ResponseBytes, now.AddMinutes(5));
        store.RestoreRecord("op-old", "fp", ResponseBytes, now.AddMinutes(-9));

        clock.Advance(TimeSpan.FromMinutes(2));
        store.SweepExpired();

        _ = await Assert.That(store.RecordCount).IsEqualTo(1);
        _ = await Assert.That(store.TryReplay("op-future", "fp", TryAddAsyncResponse.Parser, out _)).IsTrue();
    }

    /// <summary>Restoring the same operation again and again keeps the order structures bounded by the live record.</summary>
    [Test]
    public async Task ReplacedRestoredRecordsCompactOrder()
    {
        var clock = new WallSteppedClock();
        var store = CreateStore(clock, 100);
        var now = clock.GetUtcNow().UtcDateTime;
        for (var i = 0; i < 1000; i++)
            store.RestoreRecord("op-1", "fp", ResponseBytes, now.AddSeconds(-(i % 60)));

        store.SweepExpired();

        _ = await Assert.That(store.RecordCount).IsEqualTo(1);
        _ = await Assert.That(store.OrderEntryCount).IsLessThanOrEqualTo(3 * (2 + 64 + 1));
    }

    /// <summary>A young restored outcome at the head does not shield an older one behind it from eviction.</summary>
    [Test]
    public async Task YoungRestoredHeadKeepsOlderEvictable()
    {
        var clock = new WallSteppedClock();
        var store = CreateStore(clock, 2);
        var now = clock.GetUtcNow().UtcDateTime;
        store.RestoreRecord("op-young", "fp", ResponseBytes, now.AddSeconds(-10));
        store.RestoreRecord("op-old", "fp", ResponseBytes, now.AddMinutes(-5));

        var reserved = store.ReserveIntent("op-new", "fp", null, out _);

        _ = await Assert.That(reserved).IsEqualTo(IdempotencyReserveResult.Acquired);
        _ = await Assert.That(store.TryReplay("op-young", "fp", TryAddAsyncResponse.Parser, out _)).IsTrue();
        _ = await Assert.That(store.TryReplay("op-old", "fp", TryAddAsyncResponse.Parser, out _)).IsFalse();
    }

    /// <summary>A record is retained exactly for the retention and gone right after it.</summary>
    [Test]
    public async Task RetentionBoundaryIsExclusive()
    {
        var clock = new WallSteppedClock();
        var store = CreateStore(clock, 100);
        var now = clock.GetUtcNow().UtcDateTime;
        store.RecordSuccess("op-live", "fp", ResponseBytes, null);
        store.RestoreRecord("op-restored", "fp", ResponseBytes, now);

        clock.Advance(Retention);
        store.SweepExpired();
        var atBoundary = store.RecordCount;
        clock.Advance(Tick);
        store.SweepExpired();

        _ = await Assert.That(atBoundary).IsEqualTo(2);
        _ = await Assert.That(store.RecordCount).IsEqualTo(0);
    }

    /// <summary>An outcome becomes evictable only once it is older than the minimum retention.</summary>
    [Test]
    public async Task MinRetentionBoundaryIsExclusive()
    {
        var clock = new WallSteppedClock();
        var store = CreateStore(clock, 1);
        store.RecordSuccess("op-1", "fp", ResponseBytes, null);
        clock.Advance(MinRetention);

        var refused = NodeExceptionAssert.For<SquirixException>().Throws(store, static s => s.ReserveIntent("op-2", "fp", null, out _));
        clock.Advance(Tick);
        var reserved = store.ReserveIntent("op-2", "fp", null, out _);

        _ = await Assert.That(refused.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);
        _ = await Assert.That(reserved).IsEqualTo(IdempotencyReserveResult.Acquired);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private RpcMutationIdempotencyStore CreateStore(TimeProvider clock, int capacity) =>
        new(new IdempotencyOptions { MaxInFlightRecords = capacity, Retention = Retention, MinRetention = MinRetention }, "test-node", new IdempotencyMetrics(_testMeter), clock);
}
