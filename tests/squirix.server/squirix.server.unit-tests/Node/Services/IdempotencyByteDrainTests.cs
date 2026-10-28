using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Threading.Tasks;
using Google.Protobuf;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
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

/// <summary>A store far over its byte budget drains in bounded steps, and an outcome whose reservation expired is still recorded.</summary>
[Immutable]
public sealed class IdempotencyByteDrainTests : DisposableServerUnitTestBase
{
    private const int ResponseLength = 100;
    private const long Charge = IdempotencyOptions.RecordOverheadBytes + ResponseLength;
    private const int RestoredRecords = 500;
    private const int MaxEvictionsPerCall = 64;

    private static readonly TimeSpan MinRetention = new IdempotencyOptions().MinRetention;
    private static readonly TimeSpan Retention = new IdempotencyOptions().Retention;

    private readonly Meter _testMeter = new("test");

    /// <summary>A restore far over a smaller budget drains over several calls: each gives up a bounded number of outcomes and then refuses retryably.</summary>
    [Test]
    public async Task LargeRestoreDrainsInBoundedSteps()
    {
        var clock = new FakeTimeProvider();
        var store = CreateStore(clock, 3 * Charge);
        var createdUtc = clock.GetUtcNow().UtcDateTime - MinRetention - TimeSpan.FromMinutes(1);
        var restored = new List<PersistedIdempotencyRecord?>();
        for (var i = 0; i < RestoredRecords; i++)
            restored.Add(new PersistedIdempotencyRecord(string.Create(CultureInfo.InvariantCulture, $"restored-{i}"), "fp", Response(ResponseLength), createdUtc));

        store.RestoreSnapshotRecords(restored);

        var refused = NodeExceptionAssert.For<SquirixException>().Throws(store, static s => s.ReserveIntent("op-new", "fp", null, out _));
        var afterFirstCall = store.RecordCount;
        var calls = 1;
        var admitted = IdempotencyReserveResult.AlreadyCompleted;
        while (admitted != IdempotencyReserveResult.Acquired)
        {
            try
            {
                admitted = store.ReserveIntent("op-new", "fp", null, out _);
            }
            catch (SquirixException)
            {
                // Refused after another bounded step of the drain.
            }

            calls++;
        }

        _ = await Assert.That(refused.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);
        _ = await Assert.That(afterFirstCall).IsEqualTo(RestoredRecords - MaxEvictionsPerCall);
        _ = await Assert.That(calls).IsGreaterThan(1);
        _ = await Assert.That(store.RecordBytes).IsLessThanOrEqualTo(3 * Charge);
    }

    /// <summary>An outcome whose reservation expired meanwhile is recorded even when the budget is full of young outcomes, and replays.</summary>
    [Test]
    public async Task OutcomeAfterExpiredReservationIsRecorded()
    {
        var clock = new FakeTimeProvider();
        var store = CreateStore(clock, 2 * Charge);
        var reservation = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = store.ReserveIntent("op-late", "fp", reservation, out _);
        clock.Advance(Retention - TimeSpan.FromSeconds(10));
        store.RecordSuccess("op-1", "fp", Response(ResponseLength), null);
        store.RecordSuccess("op-2", "fp", Response(ResponseLength), null);
        clock.Advance(MinRetention - TimeSpan.FromSeconds(10));
        store.SweepExpired();
        var expired = store.RecordCount;

        store.RecordSuccess("op-late", "fp", Response(ResponseLength), reservation);

        _ = await Assert.That(expired).IsEqualTo(2);
        _ = await Assert.That(store.RecordCount).IsEqualTo(3);
        _ = await Assert.That(store.RecordBytes).IsEqualTo(3 * Charge);
        _ = await Assert.That(store.TryReplay("op-late", "fp", ProtoString.Parser, out _)).IsTrue();
        _ = await Assert.That(store.TryReplay("op-1", "fp", ProtoString.Parser, out _)).IsTrue();
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static byte[] Response(long length) => new ProtoString { Value = new string('x', int.CreateChecked(Math.Max(length - 2, 0))) }.ToByteArray();

    private RpcMutationIdempotencyStore CreateStore(TimeProvider clock, long budget) =>
        new(new IdempotencyOptions { MaxRecordBytes = budget }, "test-node", new IdempotencyMetrics(_testMeter), clock);
}
