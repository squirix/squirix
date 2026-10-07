using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading.Tasks;
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

/// <summary>Model tests that pin <see cref="RpcMutationIdempotencyStore" /> expiry, eviction and snapshot order to the rules of a scanning reference model.</summary>
[Immutable]
public sealed class RpcMutationIdempotencyModelTests : DisposableServerUnitTestBase
{
    private const int Steps = 2000;
    private const int KeyCount = 12;
    private static readonly TimeSpan Retention = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan MinRetention = TimeSpan.FromMinutes(2);

    private static readonly byte[] ResponseBytes = IdempotencyResponseCodec.SerializeResponseBytes(new TryAddAsyncResponse { Added = true });

    private readonly Meter _testMeter = new("test");

    /// <summary>The store and the scanning model agree after every random step.</summary>
    /// <param name="seed">The seed of the step sequence.</param>
    [Test]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    [Arguments(4)]
    [Arguments(5)]
    [Arguments(6)]
    [Arguments(7)]
    [Arguments(8)]
    public async Task StoreMatchesScanningModel(int seed)
    {
        var random = new SplitMix64(Convert.ToUInt64(seed));
        var capacity = 3 + (seed % 6);
        var clock = new WallSteppedClock();
        var store = new RpcMutationIdempotencyStore(
            new IdempotencyOptions { MaxInFlightRecords = capacity, Retention = Retention, MinRetention = MinRetention },
            "test-node",
            new IdempotencyMetrics(_testMeter),
            clock);
        var model = new IdempotencyStoreOracle(clock, capacity, Retention, MinRetention);
        var tokens = new TaskCompletionSource?[KeyCount];

        string? mismatch = null;
        for (var step = 0; step < Steps && mismatch == null; step++)
        {
            var action = ApplyRandomStep(random, store, model, clock, tokens, random.Next(KeyCount));
            mismatch = action.StartsWith("reserve differs", StringComparison.Ordinal) ? action : Compare(store, model, clock);
            if (mismatch != null)
                mismatch = $"seed {seed} step {step} ({action}): {mismatch}";
        }

        _ = await Assert.That(mismatch).IsNull();
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static string Id(int key) => $"op-{NodeInvariantIndexStrings.FormatD4(key)}";

    private static DateTime RestoredDate(SplitMix64 random, FakeTimeProvider clock) =>
        clock.GetUtcNow().UtcDateTime.AddMinutes(random.Next(-10, 6)).AddSeconds(random.Next(3) == 0 ? 30 : 0);

    private static string ApplyRandomStep(SplitMix64 random, RpcMutationIdempotencyStore store, IdempotencyStoreOracle model, WallSteppedClock clock, TaskCompletionSource?[] tokens, int key)
    {
        var id = Id(key);
        var fingerprint = $"fp-{id}";
        var token = random.Next(4) == 0 ? null : tokens[key];
        switch (random.Next(100))
        {
            case < 20:
                return Reserve(store, model, tokens, key, random.Next(4) == 0 ? null : new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
            case < 28:
                store.ReleaseIntent(id, fingerprint, token);
                model.Release(id, token);
                return "release";
            case < 34:
                store.MarkStamped(id, token);
                model.Mark(id, token);
                return "stamp";
            case < 40:
                store.HoldAppendedOutcome(id, fingerprint, ResponseBytes, token);
                model.Hold(id, token);
                return "hold";
            case < 52:
                store.RecordSuccess(id, fingerprint, ResponseBytes, token);
                model.Success(id, token);
                return "success";
            case < 58:
                var restoredUtc = RestoredDate(random, clock);
                store.RestoreRecord(id, fingerprint, ResponseBytes, restoredUtc);
                model.Restore(id, true, restoredUtc);
                return "restore";
            case < 63:
                var startedUtc = RestoredDate(random, clock);
                store.RestoreStarted(id, fingerprint, startedUtc);
                model.Restore(id, false, startedUtc);
                return "restore started";
            case < 67:
                RestoreSnapshot(random, store, model, clock);
                return "restore snapshot";
            case < 70:
                if (tokens[key] is not { } execution)
                    return "complete execution";
                store.CompleteExecution(id, execution);
                model.CompleteExecution(id, execution);
                return "complete execution";
            case < 74:
                store.SweepExpired();
                model.SweepExpired();
                return "sweep";
            case < 90:
                clock.Advance(TimeSpan.FromSeconds(random.Next(0, 4) * 30));
                return "advance";
            default:
                clock.StepWallClock(TimeSpan.FromSeconds(random.Next(-12, 13) * 30));
                return "wall step";
        }
    }

    private static string Reserve(RpcMutationIdempotencyStore store, IdempotencyStoreOracle model, TaskCompletionSource?[] tokens, int key, TaskCompletionSource? execution)
    {
        var id = Id(key);
        var (expectedResult, expectedInFlight) = model.Reserve(id, execution);
        IdempotencyReserveResult? actual;
        Task? inFlight = null;
        try
        {
            actual = store.ReserveIntent(id, $"fp-{id}", execution, out inFlight);
        }
        catch (SquirixException)
        {
            actual = null;
        }

        if (actual == IdempotencyReserveResult.Acquired)
            tokens[key] = execution;

        return actual != expectedResult || !ReferenceEquals(inFlight, expectedInFlight) ? $"reserve differs: store {actual}, model {expectedResult}" : "reserve";
    }

    private static void RestoreSnapshot(SplitMix64 random, RpcMutationIdempotencyStore store, IdempotencyStoreOracle model, FakeTimeProvider clock)
    {
        var records = new List<PersistedIdempotencyRecord>();
        for (var i = random.Next(1, 4); i > 0; i--)
        {
            var id = Id(random.Next(KeyCount));
            var created = RestoredDate(random, clock);
            records.Add(random.Next(2) == 0 ? new PersistedIdempotencyRecord(id, $"fp-{id}", ResponseBytes, created) : new PersistedIdempotencyRecord(id, $"fp-{id}", created));
        }

        store.RestoreSnapshotRecords(records);
        model.RestoreSnapshot(records);
    }

    private static string? Compare(RpcMutationIdempotencyStore store, IdempotencyStoreOracle model, WallSteppedClock clock)
    {
        for (var key = 0; key < KeyCount; key++)
        {
            var id = Id(key);
            var replayed = store.TryReplay(id, $"fp-{id}", TryAddAsyncResponse.Parser, out _);
            if (replayed != model.Replay(id))
                return $"replay of {id} differs: store {replayed}";
        }

        if (store.RecordCount != model.RecordCount)
            return $"record count differs: store {store.RecordCount}, model {model.RecordCount}";

        if (store.ExecutionCount != model.ExecutionCount)
            return $"execution count differs: store {store.ExecutionCount}, model {model.ExecutionCount}";

        var exported = new List<PersistedIdempotencyRecord>();
        IIdempotencySnapshotExporter exporter = store;
        exporter.ExportSnapshot(exported, clock.GetUtcNow().UtcDateTime);
        var expected = model.Export();
        if (exported.Count != expected.Count)
            return $"export size differs: store {exported.Count}, model {expected.Count}";

        for (var i = 0; i < expected.Count; i++)
        {
            var actual = exported[i];
            if (!string.Equals(actual.OperationId, expected[i].Id, StringComparison.Ordinal) || actual.State == IdempotencyRecordState.Completed != expected[i].Completed || actual.CreatedUtc != expected[i].CreatedUtc)
                return $"export entry {i} differs: store {actual.OperationId}/{actual.State}/{actual.CreatedUtc:O}, model {expected[i].Id}/{expected[i].Completed}/{expected[i].CreatedUtc:O}";
        }

        return null;
    }
}
