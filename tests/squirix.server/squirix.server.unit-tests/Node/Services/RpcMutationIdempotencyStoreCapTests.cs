using System;
using System.Diagnostics.Metrics;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
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
        store.RecordSuccess("op-1", "fp-1", ResponseBytes);
        store.RecordSuccess("op-2", "fp-2", ResponseBytes);
        store.RestoreRecord("op-stale", "fp-stale", ResponseBytes, DateTime.UtcNow.AddMinutes(-20));

        store.RecordSuccess("op-3", "fp-3", ResponseBytes);

        _ = await Assert.That(store.RecordCount).IsEqualTo(cap);
        _ = await Assert.That(store.TryReplay("op-stale", "fp-stale", TryAddAsyncResponse.Parser, out _)).IsFalse();
        _ = await Assert.That(store.TryReplay("op-3", "fp-3", TryAddAsyncResponse.Parser, out _)).IsTrue();
    }

    /// <summary>Evicting the oldest record allows a new operation id to be stored at capacity.</summary>
    [Test]
    public async Task NewOpEvictsOldestRecordAtCapacity()
    {
        const int cap = 2;
        var store = new RpcMutationIdempotencyStore(
            new IdempotencyOptions { MaxInFlightRecords = cap, Retention = TimeSpan.FromHours(1) },
            "test-node",
            new IdempotencyMetrics(_testMeter));
        store.RecordSuccess("op-1", "fp-1", ResponseBytes);
        store.RecordSuccess("op-2", "fp-2", ResponseBytes);

        store.RecordSuccess("op-3", "fp-3", ResponseBytes);

        _ = await Assert.That(store.RecordCount).IsEqualTo(cap);
        _ = await Assert.That(store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out _)).IsFalse();
        _ = await Assert.That(store.TryReplay("op-3", "fp-3", TryAddAsyncResponse.Parser, out var replayed)).IsTrue();
        _ = await Assert.That(replayed).IsNotNull();
        _ = await Assert.That(replayed.Added).IsTrue();
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
        store.RecordSuccess("op-1", "fp-1", ResponseBytes);
        store.RecordSuccess("op-2", "fp-2", ResponseBytes);

        store.RecordSuccess("op-1", "fp-1", ResponseBytes);

        _ = await Assert.That(store.RecordCount).IsEqualTo(cap);
        _ = await Assert.That(store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out _)).IsTrue();
        _ = await Assert.That(store.TryReplay("op-2", "fp-2", TryAddAsyncResponse.Parser, out _)).IsTrue();
    }

    /// <summary>Background sweep removes expired records without a new access.</summary>
    [Test]
    public async Task SweepExpiredRemovesWithoutReadAccess()
    {
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions { Retention = TimeSpan.FromMilliseconds(50) }, "local", new IdempotencyMetrics(_testMeter));
        store.RecordSuccess("op-1", "fp-1", ResponseBytes);

        store.SweepExpired(DateTime.UtcNow.AddMinutes(1));

        _ = await Assert.That(store.RecordCount).IsEqualTo(0);
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
            store.RecordSuccess($"op-{NodeInvariantIndexStrings.FormatD4(i)}", $"fp-{NodeInvariantIndexStrings.FormatD4(i)}", ResponseBytes);

        _ = await Assert.That(store.RecordCount).IsEqualTo(cap);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();
}
