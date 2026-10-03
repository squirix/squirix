using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Which write-ahead intents of the idempotency store a snapshot exports as Started records.</summary>
[Immutable]
public sealed class RpcMutationIdempotencySnapshotTests : DisposableServerUnitTestBase
{
    private const string Fingerprint = "set-entry-async|default|k|abc123";
    private const string OperationId = "0123456789abcdef0123456789abcdef";

    private readonly Meter _testMeter = new("test");

    /// <summary>A live reservation that stamped no mutation frame (a replicated write) is not exported; once a frame is stamped it is.</summary>
    [Test]
    public async Task LiveReservationExportsOnlyWhenStamped()
    {
        var store = CreateStore();
        var execution = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = store.ReserveIntent(OperationId, Fingerprint, execution, out _);

        var unstamped = Export(store);
        store.MarkStamped(OperationId, execution);
        var stamped = Export(store);

        _ = await Assert.That(unstamped).IsEmpty();
        var record = await Assert.That(stamped).HasSingleItem();
        _ = await Assert.That(record.OperationId).IsEqualTo(OperationId);
        _ = await Assert.That(record.Fingerprint).IsEqualTo(Fingerprint);
        _ = await Assert.That(record.State).IsEqualTo(IdempotencyRecordState.Started);
    }

    /// <summary>A restored Started record is always exported, and a release never removes it.</summary>
    [Test]
    public async Task RestoredStartedIsNeverReleased()
    {
        var store = CreateStore();
        store.RestoreStarted(OperationId, Fingerprint, DateTime.UtcNow);

        store.ReleaseIntent(OperationId, Fingerprint, null);

        var record = await Assert.That(Export(store)).HasSingleItem();
        _ = await Assert.That(record.OperationId).IsEqualTo(OperationId);
        _ = await Assert.That(record.State).IsEqualTo(IdempotencyRecordState.Started);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static List<PersistedIdempotencyRecord> Export(RpcMutationIdempotencyStore store)
    {
        var destination = new List<PersistedIdempotencyRecord>();
        IIdempotencySnapshotExporter exporter = store;
        exporter.ExportSnapshot(destination, DateTime.UtcNow);
        return destination;
    }

    private RpcMutationIdempotencyStore CreateStore() => new(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));
}
