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

/// <summary>Capacity and eviction tests for <see cref="RpcMutationIdempotencyStore" />.</summary>
[Immutable]
public sealed class RpcMutationIdempotencyStoreCapTests : DisposableServerUnitTestBase
{
    private static readonly TimeSpan MinRetention = TimeSpan.FromMinutes(1);

    private static readonly byte[] ResponseBytes = IdempotencyResponseCodec.SerializeResponseBytes(new TryAddAsyncResponse { Added = true });

    private readonly Meter _testMeter = new("test");

    /// <summary>Expired records are removed before capacity enforcement on new inserts.</summary>
    [Test]
    public async Task ExpiredRecordsPrunedBeforeCapCheck()
    {
        const int cap = 2;
        var store = new RpcMutationIdempotencyStore(
            new IdempotencyOptions { MaxInFlightRecords = cap, Retention = TimeSpan.FromMinutes(15) },
            "test-node",
            new IdempotencyMetrics(_testMeter));
        store.RecordSuccess("op-1", "fp-1", ResponseBytes, null);
        store.RecordSuccess("op-2", "fp-2", ResponseBytes, null);
        store.RestoreRecord("op-stale", "fp-stale", ResponseBytes, DateTime.UtcNow.AddMinutes(-20));

        store.RecordSuccess("op-3", "fp-3", ResponseBytes, null);

        _ = await Assert.That(store.RecordCount).IsEqualTo(cap);
        _ = await Assert.That(store.TryReplay("op-stale", "fp-stale", TryAddAsyncResponse.Parser, out _)).IsFalse();
        _ = await Assert.That(store.TryReplay("op-3", "fp-3", TryAddAsyncResponse.Parser, out _)).IsTrue();
    }

    /// <summary>At capacity a new operation evicts the oldest outcome past the minimum retention.</summary>
    [Test]
    public async Task NewOpEvictsAgedOutcomeAtCapacity()
    {
        var clock = new FakeTimeProvider();
        var store = CreateStore(clock, 2);
        store.RecordSuccess("op-1", "fp-1", ResponseBytes, null);
        store.RecordSuccess("op-2", "fp-2", ResponseBytes, null);
        clock.Advance(MinRetention + TimeSpan.FromSeconds(1));

        var reserved = store.ReserveIntent("op-3", "fp-3", null, out _);

        _ = await Assert.That(reserved).IsEqualTo(IdempotencyReserveResult.Acquired);
        _ = await Assert.That(store.RecordCount).IsEqualTo(2);
        _ = await Assert.That(store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out _)).IsFalse();
        _ = await Assert.That(store.TryReplay("op-2", "fp-2", TryAddAsyncResponse.Parser, out _)).IsTrue();
    }

    /// <summary>A new operation is refused as retryable while every outcome is younger than the minimum retention.</summary>
    [Test]
    public async Task NewOpRefusedWhileOutcomesAreYoung()
    {
        var clock = new FakeTimeProvider();
        var store = CreateStore(clock, 2);
        store.RecordSuccess("op-1", "fp-1", ResponseBytes, null);
        store.RecordSuccess("op-2", "fp-2", ResponseBytes, null);
        clock.Advance(MinRetention - TimeSpan.FromSeconds(1));

        var refused = NodeExceptionAssert.For<SquirixException>().Throws(store, static s => s.ReserveIntent("op-3", "fp-3", null, out _));

        _ = await Assert.That(refused.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);
        _ = await Assert.That(store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out _)).IsTrue();
        _ = await Assert.That(store.TryReplay("op-2", "fp-2", TryAddAsyncResponse.Parser, out _)).IsTrue();
    }

    /// <summary>A reservation in flight is never evicted: the new operation is refused and a retry still joins the first attempt.</summary>
    [Test]
    public async Task InFlightReservationIsNeverEvicted()
    {
        var clock = new FakeTimeProvider();
        var store = CreateStore(clock, 1);
        var execution = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = store.ReserveIntent("op-1", "fp-1", execution, out _);
        clock.Advance(MinRetention + TimeSpan.FromSeconds(1));

        var refused = NodeExceptionAssert.For<SquirixException>().Throws(store, static s => s.ReserveIntent("op-2", "fp-2", null, out _));
        var retry = store.ReserveIntent("op-1", "fp-1", null, out var joined);

        _ = await Assert.That(refused.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);
        _ = await Assert.That(retry).IsEqualTo(IdempotencyReserveResult.AlreadyStarted);
        _ = await Assert.That(ReferenceEquals(joined, execution.Task)).IsTrue();
    }

    /// <summary>At capacity a new operation passes over an older reservation and evicts the aged outcome behind it.</summary>
    [Test]
    public async Task NewOpSkipsReservationForAgedOutcome()
    {
        var clock = new FakeTimeProvider();
        var store = CreateStore(clock, 2);
        var execution = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = store.ReserveIntent("op-1", "fp-1", execution, out _);
        store.RecordSuccess("op-2", "fp-2", ResponseBytes, null);
        clock.Advance(MinRetention + TimeSpan.FromSeconds(1));

        var reserved = store.ReserveIntent("op-3", "fp-3", null, out _);
        var retry = store.ReserveIntent("op-1", "fp-1", null, out var joined);

        _ = await Assert.That(reserved).IsEqualTo(IdempotencyReserveResult.Acquired);
        _ = await Assert.That(store.TryReplay("op-2", "fp-2", TryAddAsyncResponse.Parser, out _)).IsFalse();
        _ = await Assert.That(retry).IsEqualTo(IdempotencyReserveResult.AlreadyStarted);
        _ = await Assert.That(ReferenceEquals(joined, execution.Task)).IsTrue();
    }

    /// <summary>Restoring and recording outcomes never refuse: a store full of reservations grows past its capacity.</summary>
    [Test]
    public async Task RestoreNeverRefusesAtCapacity()
    {
        var clock = new FakeTimeProvider();
        var store = CreateStore(clock, 1);
        _ = store.ReserveIntent("op-1", "fp-1", null, out _);

        store.RestoreStarted("op-2", "fp-2", clock.GetUtcNow().UtcDateTime);
        store.RestoreRecord("op-3", "fp-3", ResponseBytes, clock.GetUtcNow().UtcDateTime);
        store.RecordSuccess("op-4", "fp-4", ResponseBytes, null);

        _ = await Assert.That(store.RecordCount).IsEqualTo(3);
        _ = await Assert.That(store.TryReplay("op-4", "fp-4", TryAddAsyncResponse.Parser, out _)).IsTrue();
    }

    /// <summary>An attempt whose reservation expired and was re-acquired by a retry neither releases nor settles the retry's reservation.</summary>
    [Test]
    public async Task ExpiredAttemptLeavesRetryReservation()
    {
        var clock = new FakeTimeProvider();
        var store = CreateStore(clock, 16);
        var first = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var retry = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = store.ReserveIntent("op-1", "fp-1", first, out _);
        clock.Advance(TimeSpan.FromHours(2));
        _ = store.ReserveIntent("op-1", "fp-1", retry, out _);

        store.ReleaseIntent("op-1", "fp-1", first);
        store.RecordSuccess("op-1", "fp-1", ResponseBytes, first);
        var next = store.ReserveIntent("op-1", "fp-1", null, out var joined);

        _ = await Assert.That(next).IsEqualTo(IdempotencyReserveResult.AlreadyStarted);
        _ = await Assert.That(ReferenceEquals(joined, retry.Task)).IsTrue();
    }

    /// <summary>Snapshot restore rejects null records with a contract exception.</summary>
    [Test]
    public async Task RestoreSnapshotRecordsRejectsNullElement()
    {
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "test-node", new IdempotencyMetrics(_testMeter));

        var ex = NodeExceptionAssert.For<ArgumentException>().Throws(store, static s => s.RestoreSnapshotRecords([null]));

        _ = await Assert.That(ex.Message).Contains("must not be null", StringComparison.Ordinal);
    }

    /// <summary>Replacing an existing operation id does not grow the record count.</summary>
    [Test]
    public async Task SuccessReplaceKeepsRecordCountFlat()
    {
        const int cap = 2;
        var store = new RpcMutationIdempotencyStore(
            new IdempotencyOptions { MaxInFlightRecords = cap, Retention = TimeSpan.FromHours(1) },
            "test-node",
            new IdempotencyMetrics(_testMeter));
        store.RecordSuccess("op-1", "fp-1", ResponseBytes, null);
        store.RecordSuccess("op-2", "fp-2", ResponseBytes, null);

        store.RecordSuccess("op-1", "fp-1", ResponseBytes, null);

        _ = await Assert.That(store.RecordCount).IsEqualTo(cap);
        _ = await Assert.That(store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out _)).IsTrue();
        _ = await Assert.That(store.TryReplay("op-2", "fp-2", TryAddAsyncResponse.Parser, out _)).IsTrue();
    }

    /// <summary>Background sweep removes expired records without a new access.</summary>
    [Test]
    public async Task SweepExpiredRemovesWithoutReadAccess()
    {
        var clock = new FakeTimeProvider();
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions { Retention = TimeSpan.FromMilliseconds(50) }, "local", new IdempotencyMetrics(_testMeter), clock);
        store.RecordSuccess("op-1", "fp-1", ResponseBytes, null);

        clock.Advance(TimeSpan.FromMinutes(1));
        store.SweepExpired();

        _ = await Assert.That(store.RecordCount).IsEqualTo(0);
    }

    /// <summary>Retention expiry drops a joinable execution with its record; the owner still wakes the retries already joined to it.</summary>
    [Test]
    public async Task ExpiryDropsJoinableExecution()
    {
        var clock = new FakeTimeProvider();
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions { Retention = TimeSpan.FromMinutes(15) }, "local", new IdempotencyMetrics(_testMeter), clock);
        var execution = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = store.ReserveIntent("op-1", "fp-1", execution, out _);
        _ = store.ReserveIntent("op-1", "fp-1", null, out var joined);

        clock.Advance(TimeSpan.FromHours(1));
        store.SweepExpired();
        var countAfterExpiry = store.ExecutionCount;
        store.CompleteExecution("op-1", execution);

        _ = await Assert.That(ReferenceEquals(joined, execution.Task)).IsTrue();
        _ = await Assert.That(countAfterExpiry).IsEqualTo(0);
        _ = await Assert.That(store.RecordCount).IsEqualTo(0);
        _ = await Assert.That(execution.Task.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>Completing a stale execution leaves the newer execution of a re-acquired reservation registered and joinable.</summary>
    [Test]
    public async Task StaleCompletionKeepsNewerExecution()
    {
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));
        var stale = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var current = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = store.ReserveIntent("op-1", "fp-1", stale, out _);
        store.ReleaseIntent("op-1", "fp-1", stale);
        var reacquired = store.ReserveIntent("op-1", "fp-1", current, out _);

        store.CompleteExecution("op-1", stale);
        var started = store.ReserveIntent("op-1", "fp-1", null, out var joined);
        store.CompleteExecution("op-1", current);

        _ = await Assert.That(reacquired).IsEqualTo(IdempotencyReserveResult.Acquired);
        _ = await Assert.That(started).IsEqualTo(IdempotencyReserveResult.AlreadyStarted);
        _ = await Assert.That(ReferenceEquals(joined, current.Task)).IsTrue();
        _ = await Assert.That(stale.Task.IsCompletedSuccessfully).IsTrue();
        _ = await Assert.That(store.ExecutionCount).IsEqualTo(0);
    }

    /// <summary>Flooding unique operation ids keeps the in-memory record count within the configured cap.</summary>
    [Test]
    public async Task UniqueOpIdFloodStaysWithinRecordCap()
    {
        const int cap = 8;
        var store = new RpcMutationIdempotencyStore(
            new IdempotencyOptions { MaxInFlightRecords = cap, Retention = TimeSpan.FromHours(1) },
            "test-node",
            new IdempotencyMetrics(_testMeter));

        for (var i = 0; i < cap * 3; i++)
            store.RecordSuccess($"op-{NodeInvariantIndexStrings.FormatD4(i)}", $"fp-{NodeInvariantIndexStrings.FormatD4(i)}", ResponseBytes, null);

        _ = await Assert.That(store.RecordCount).IsEqualTo(cap);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private RpcMutationIdempotencyStore CreateStore(TimeProvider clock, int capacity) =>
        new(new IdempotencyOptions { MaxInFlightRecords = capacity, Retention = TimeSpan.FromHours(1), MinRetention = MinRetention }, "test-node", new IdempotencyMetrics(_testMeter), clock);
}
