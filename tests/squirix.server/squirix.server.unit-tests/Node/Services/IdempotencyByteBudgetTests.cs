using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using ProtoString = Google.Protobuf.WellKnownTypes.StringValue;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The byte budget of <see cref="RpcMutationIdempotencyStore" />: what a record is charged, what makes room, and what never does.</summary>
[Immutable]
public sealed class IdempotencyByteBudgetTests : DisposableServerUnitTestBase
{
    private const int ResponseLength = 100;
    private const long Charge = IdempotencyOptions.RecordOverheadBytes + ResponseLength;
    private const long Mebibyte = 1024L * 1024;

    private static readonly TimeSpan MinRetention = new IdempotencyOptions().MinRetention;
    private static readonly TimeSpan Retention = TimeSpan.FromMinutes(15);

    private readonly Meter _testMeter = new("test");

    /// <summary>A reservation is charged the overhead, its outcome the overhead and the response, and expiry and release give the charge back.</summary>
    [Test]
    public async Task ChargeIsTakenAndGivenBack()
    {
        var clock = new FakeTimeProvider();
        var store = CreateStore(clock, 10 * Charge);

        var reservation = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = store.ReserveIntent("op-1", "fp", reservation, out _);
        var reserved = store.RecordBytes;
        store.RecordSuccess("op-1", "fp", Response(ResponseLength), reservation);
        var completed = store.RecordBytes;
        _ = store.ReserveIntent("op-2", "fp", null, out _);
        store.ReleaseIntent("op-2", "fp", null);
        var released = store.RecordBytes;
        clock.Advance(Retention + TimeSpan.FromSeconds(1));
        store.SweepExpired();

        _ = await Assert.That(reserved).IsEqualTo(IdempotencyOptions.RecordOverheadBytes);
        _ = await Assert.That(completed).IsEqualTo(Charge);
        _ = await Assert.That(released).IsEqualTo(Charge);
        _ = await Assert.That(store.RecordBytes).IsEqualTo(0L);
    }

    /// <summary>At the budget a new operation evicts the outcome inserted first among those past the minimum retention, and only as many as it needs.</summary>
    [Test]
    public async Task EvictsOldestAgedOutcomeFirst()
    {
        var clock = new FakeTimeProvider();
        var store = CreateStore(clock, 3 * Charge);
        store.RecordSuccess("op-1", "fp", Response(ResponseLength), null);
        store.RecordSuccess("op-2", "fp", Response(ResponseLength), null);
        store.RecordSuccess("op-3", "fp", Response(ResponseLength), null);
        clock.Advance(MinRetention + TimeSpan.FromSeconds(1));

        var reserved = store.ReserveIntent("op-4", "fp", null, out _);

        _ = await Assert.That(reserved).IsEqualTo(IdempotencyReserveResult.Acquired);
        _ = await Assert.That(store.RecordCount).IsEqualTo(3);
        _ = await Assert.That(store.RecordBytes).IsEqualTo((2 * Charge) + IdempotencyOptions.RecordOverheadBytes);
        _ = await Assert.That(store.TryReplay("op-1", "fp", ProtoString.Parser, out _)).IsFalse();
        _ = await Assert.That(store.TryReplay("op-2", "fp", ProtoString.Parser, out _)).IsTrue();
        _ = await Assert.That(store.TryReplay("op-3", "fp", ProtoString.Parser, out _)).IsTrue();
    }

    /// <summary>A new operation is refused retryably while every outcome is younger than the minimum retention, and admitted once the oldest is not.</summary>
    [Test]
    public async Task RefusesWhileOutcomesAreYoung()
    {
        var clock = new FakeTimeProvider();
        var store = CreateStore(clock, 2 * Charge);
        store.RecordSuccess("op-1", "fp", Response(ResponseLength), null);
        store.RecordSuccess("op-2", "fp", Response(ResponseLength), null);
        clock.Advance(MinRetention - TimeSpan.FromSeconds(1));

        var refused = NodeExceptionAssert.For<SquirixException>().Throws(store, static s => s.ReserveIntent("op-3", "fp", null, out _));
        clock.Advance(TimeSpan.FromSeconds(2));
        var admitted = store.ReserveIntent("op-3", "fp", null, out _);

        _ = await Assert.That(refused.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);
        _ = await Assert.That(admitted).IsEqualTo(IdempotencyReserveResult.Acquired);
    }

    /// <summary>
    /// A response too large for the budget is recorded, since its mutation already ran, and exceeds the budget; new operations are refused
    /// until outcomes age past the minimum retention, and then the aged outcomes are given up until the new one fits.
    /// </summary>
    [Test]
    public async Task LargeResponseExceedsBudgetThenDrains()
    {
        var clock = new FakeTimeProvider();
        const long budget = 4 * Charge;
        var store = CreateStore(clock, budget);
        store.RecordSuccess("op-1", "fp", Response(ResponseLength), null);
        store.RecordSuccess("op-2", "fp", Response(ResponseLength), null);
        var reservation = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = store.ReserveIntent("op-large", "fp", reservation, out _);

        store.RecordSuccess("op-large", "fp", Response(budget), reservation);
        var exceeded = store.RecordBytes;
        var refused = NodeExceptionAssert.For<SquirixException>().Throws(store, static s => s.ReserveIntent("op-new", "fp", null, out _));
        var replayed = store.TryReplay("op-large", "fp", ProtoString.Parser, out _);
        clock.Advance(MinRetention + TimeSpan.FromSeconds(1));
        var admitted = store.ReserveIntent("op-new", "fp", null, out _);

        _ = await Assert.That(exceeded).IsGreaterThan(budget);
        _ = await Assert.That(refused.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);
        _ = await Assert.That(replayed).IsTrue();
        _ = await Assert.That(admitted).IsEqualTo(IdempotencyReserveResult.Acquired);
        _ = await Assert.That(store.TryReplay("op-1", "fp", ProtoString.Parser, out _)).IsFalse();
        _ = await Assert.That(store.TryReplay("op-2", "fp", ProtoString.Parser, out _)).IsFalse();
    }

    /// <summary>A reservation is never evicted for bytes, however old: a new operation is refused instead, and the reservation still joins its retries.</summary>
    [Test]
    public async Task ReservationIsNeverEvicted()
    {
        var clock = new FakeTimeProvider();
        var store = CreateStore(clock, 2 * IdempotencyOptions.RecordOverheadBytes);
        _ = store.ReserveIntent("op-1", "fp", new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously), out _);
        _ = store.ReserveIntent("op-2", "fp", new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously), out _);
        clock.Advance(MinRetention + TimeSpan.FromMinutes(5));

        var refused = NodeExceptionAssert.For<SquirixException>().Throws(store, static s => s.ReserveIntent("op-3", "fp", null, out _));

        _ = await Assert.That(refused.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);
        _ = await Assert.That(store.ReserveIntent("op-1", "fp", null, out _)).IsEqualTo(IdempotencyReserveResult.AlreadyStarted);
        _ = await Assert.That(store.ReserveIntent("op-2", "fp", null, out _)).IsEqualTo(IdempotencyReserveResult.AlreadyStarted);
        _ = await Assert.That(store.RecordCount).IsEqualTo(2);
    }

    /// <summary>A restore over the byte budget admits every record, and the store drains as new operations evict aged outcomes.</summary>
    [Test]
    public async Task RestoreOverBudgetIsAdmittedThenDrains()
    {
        var clock = new FakeTimeProvider();
        var store = CreateStore(clock, 3 * Charge);
        var createdUtc = clock.GetUtcNow().UtcDateTime - MinRetention - TimeSpan.FromMinutes(1);
        var restored = new List<PersistedIdempotencyRecord?>();
        for (var i = 0; i < 8; i++)
            restored.Add(new PersistedIdempotencyRecord(string.Create(CultureInfo.InvariantCulture, $"restored-{i}"), "fp", Response(ResponseLength), createdUtc));

        store.RestoreSnapshotRecords(restored);
        var admittedBytes = store.RecordBytes;
        var reserved = store.ReserveIntent("op-new", "fp", null, out _);

        _ = await Assert.That(admittedBytes).IsEqualTo(8 * Charge);
        _ = await Assert.That(reserved).IsEqualTo(IdempotencyReserveResult.Acquired);
        _ = await Assert.That(store.RecordBytes).IsLessThanOrEqualTo(3 * Charge);
    }

    /// <summary>A held outcome of a started record is charged with it, and the completed record is charged once.</summary>
    [Test]
    public async Task HeldOutcomeIsChargedOnce()
    {
        var store = CreateStore(new FakeTimeProvider(), 10 * Charge);
        var reservation = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = store.ReserveIntent("op-1", "fp", reservation, out _);

        store.HoldAppendedOutcome("op-1", "fp", Response(ResponseLength), reservation);
        var held = store.RecordBytes;
        store.RecordSuccess("op-1", "fp", Response(ResponseLength), reservation);

        _ = await Assert.That(held).IsEqualTo(Charge);
        _ = await Assert.That(store.RecordBytes).IsEqualTo(Charge);
    }

    /// <summary>The bytes gauge reports the charge of the store of the registered node.</summary>
    [Test]
    public async Task BytesGaugeReportsCharge()
    {
        using var meter = new Meter("idempotency-bytes");
        var metrics = new IdempotencyMetrics(meter);
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions { MaxRecordBytes = 10 * Charge }, "test-node", metrics, new FakeTimeProvider());
        store.RecordSuccess("op-1", "fp", Response(ResponseLength), null);
        var measured = new long[] { -1 };
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, target) =>
        {
            if (ReferenceEquals(instrument.Meter, meter) && string.Equals(instrument.Name, "squirix_idempotency_bytes", StringComparison.Ordinal))
                target.EnableMeasurementEvents(instrument, measured);
        };
        listener.SetMeasurementEventCallback<long>(static (_, value, _, state) => Unsafe.As<long[]>(state)![0] = value);
        listener.Start();

        metrics.Register(new IdempotencyMetricRegistration("test-node", () => store.RecordCount, () => store.RecordBytes));
        listener.RecordObservableInstruments();

        _ = await Assert.That(measured[0]).IsEqualTo(Charge);
    }

    /// <summary>A byte budget that is not positive, or smaller than one record, fails validation naming the setting and the variable.</summary>
    /// <param name="bytes">The configured budget.</param>
    [Test]
    [Arguments(0L)]
    [Arguments(-1L)]
    [Arguments(IdempotencyOptions.RecordOverheadBytes - 1)]
    public async Task InvalidByteBudgetFailsValidation(long bytes)
    {
        var options = new IdempotencyOptions { MaxRecordBytes = bytes };

        var failure = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static o => o.Validate());

        _ = await Assert.That(failure.Message).Contains("MaxRecordBytes");
        _ = await Assert.That(failure.Message).Contains("SQUIRIX_IDEMPOTENCY_MAX_BYTES");
    }

    /// <summary>A budget of exactly one record passes validation.</summary>
    [Test]
    public async Task OneRecordBudgetPassesValidation()
    {
        var options = new IdempotencyOptions { MaxRecordBytes = IdempotencyOptions.RecordOverheadBytes };

        options.Validate();

        _ = await Assert.That(options.MaxRecordBytes).IsEqualTo(IdempotencyOptions.RecordOverheadBytes);
    }

    /// <summary>The default budget is 5 percent of the memory available to the process, between a 32 MiB floor and a 256 MiB ceiling.</summary>
    /// <param name="availableMebibytes">The memory available to the process.</param>
    /// <param name="expectedMebibytes">The expected default budget.</param>
    [Test]
    [Arguments(64L, 32L)]
    [Arguments(1024L, 51L)]
    [Arguments(102_400L, 256L)]
    public async Task DefaultBudgetFollowsProcessMemory(long availableMebibytes, long expectedMebibytes)
    {
        var provider = new IMemoryBudgetProviderCreateExpectations();
        _ = provider.Setups.GetTotalAvailableBytes().ReturnValue(availableMebibytes * Mebibyte);

        var resolved = IdempotencyOptions.ResolveDefaultMaxRecordBytes(provider.Instance());

        _ = await Assert.That(resolved / Mebibyte).IsEqualTo(expectedMebibytes);
    }

    /// <summary>The count cap default leaves the byte budget as the effective limit: it exceeds what the largest default budget holds at the least charge.</summary>
    [Test]
    public async Task DefaultCountExceedsLargestBudgetCapacity()
    {
        var options = new IdempotencyOptions();

        _ = await Assert.That(options.MaxInFlightRecords).IsGreaterThan(int.CreateChecked(IdempotencyOptions.DefaultMaxRecordBytesCeiling / IdempotencyOptions.RecordOverheadBytes));
        _ = await Assert.That(options.MaxRecordBytes).IsGreaterThanOrEqualTo(IdempotencyOptions.DefaultMaxRecordBytesFloor);
    }

    /// <summary>The environment variable sets the byte budget.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EnvironmentSetsByteBudget(CancellationToken cancellationToken)
    {
        using var variable = new TempEnvironmentVariable("SQUIRIX_IDEMPOTENCY_MAX_BYTES", "67108864");

        var loaded = await IdempotencyBootstrap.LoadAsync(cancellationToken);

        _ = await Assert.That(loaded.MaxRecordBytes).IsEqualTo(64 * Mebibyte);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    /// <summary>Builds a valid response of exactly <paramref name="length" /> bytes.</summary>
    /// <param name="length">The serialized length, at least 2 and at most 127 for the exact length; longer values are at least that long.</param>
    /// <returns>The serialized response.</returns>
    private static byte[] Response(long length) => new ProtoString { Value = new string('x', int.CreateChecked(Math.Max(length - 2, 0))) }.ToByteArray();

    private RpcMutationIdempotencyStore CreateStore(TimeProvider clock, long budget) =>
        new(new IdempotencyOptions { MaxRecordBytes = budget }, "test-node", new IdempotencyMetrics(_testMeter), clock);
}
