using System;
using Squirix.Server.Node.Services;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Capacity and eviction tests for <see cref="RpcMutationIdempotencyStore" />.</summary>
public sealed class RpcMutationIdempotencyStoreCapTests : UnitTestBase
{
    private static readonly byte[] ResponseBytes = RpcMutationIdempotencyStore.SerializeResponseBytes(new TryAddAsyncResponse { Added = true });

    /// <summary>Flooding unique operation ids keeps the in-memory record count within the configured cap.</summary>
    [Fact]
    public void FloodUniqueOperationIdsKeepsRecordCountWithinCap()
    {
        const int cap = 8;
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions { MaxInFlightRecords = cap, Retention = TimeSpan.FromHours(1) }, "test-node");

        for (var i = 0; i < cap * 3; i++)
            store.RecordSuccess($"op-{i:D4}", $"fp-{i:D4}", ResponseBytes);

        Assert.Equal(cap, store.RecordCount);
    }

    /// <summary>Evicting the oldest record allows a new operation id to be stored at capacity.</summary>
    [Fact]
    public void NewOperationEvictsOldestRecordWhenAtCapacity()
    {
        const int cap = 2;
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions { MaxInFlightRecords = cap, Retention = TimeSpan.FromHours(1) }, "test-node");
        store.RecordSuccess("op-1", "fp-1", ResponseBytes);
        store.RecordSuccess("op-2", "fp-2", ResponseBytes);

        store.RecordSuccess("op-3", "fp-3", ResponseBytes);

        Assert.Equal(cap, store.RecordCount);
        Assert.False(store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out _));
        Assert.True(store.TryReplay("op-3", "fp-3", TryAddAsyncResponse.Parser, out var replayed));
        Assert.NotNull(replayed);
        Assert.True(replayed.Added);
    }

    /// <summary>Replacing an existing operation id does not grow the record count.</summary>
    [Fact]
    public void RecordSuccessReplaceDoesNotGrowRecordCount()
    {
        const int cap = 2;
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions { MaxInFlightRecords = cap, Retention = TimeSpan.FromHours(1) }, "test-node");
        store.RecordSuccess("op-1", "fp-1", ResponseBytes);
        store.RecordSuccess("op-2", "fp-2", ResponseBytes);

        store.RecordSuccess("op-1", "fp-1", ResponseBytes);

        Assert.Equal(cap, store.RecordCount);
        Assert.True(store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out _));
        Assert.True(store.TryReplay("op-2", "fp-2", TryAddAsyncResponse.Parser, out _));
    }

    /// <summary>Expired records are removed before capacity enforcement on new inserts.</summary>
    [Fact]
    public void ExpiredRecordsAreRemovedBeforeCapacityEnforcement()
    {
        const int cap = 2;
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions { MaxInFlightRecords = cap, Retention = TimeSpan.FromMilliseconds(50) }, "test-node");
        store.RecordSuccess("op-1", "fp-1", ResponseBytes);
        store.RecordSuccess("op-2", "fp-2", ResponseBytes);
        store.RestoreRecord("op-stale", "fp-stale", ResponseBytes, DateTime.UtcNow.AddMinutes(-1));

        store.RecordSuccess("op-3", "fp-3", ResponseBytes);

        Assert.Equal(cap, store.RecordCount);
        Assert.False(store.TryReplay("op-stale", "fp-stale", TryAddAsyncResponse.Parser, out _));
        Assert.True(store.TryReplay("op-3", "fp-3", TryAddAsyncResponse.Parser, out _));
    }

    /// <summary>Background sweep removes expired records without a new access.</summary>
    [Fact]
    public void SweepExpiredRemovesExpiredRecordsWithoutAccess()
    {
        var store = new RpcMutationIdempotencyStore(TimeSpan.FromMilliseconds(50));
        store.RecordSuccess("op-1", "fp-1", ResponseBytes);

        store.SweepExpired(DateTime.UtcNow.AddMinutes(1));

        Assert.Equal(0, store.RecordCount);
    }
}
