using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Unit tests for mutating RPC idempotency store behavior.</summary>
[Immutable]
public sealed class RpcMutationIdempotencyCoordinatorTests : ServerUnitTestBase
{
    private const string ValidOperationId = "0123456789abcdef0123456789abcdef";

    /// <summary>
    /// After an unclean restart the idempotency store is empty until background recovery restores it and opens the
    /// startup gate. The coordinator must block on that gate and only then check replay, so a retry arriving before
    /// recovery finishes replays the restored record instead of re-executing the mutation. This deterministically
    /// simulates the race: the store stays empty and the gate stays closed while ExecuteAsync is in flight, then
    /// recovery restores the record and the gate opens. Regression guard for issue #320.
    /// </summary>
    [Fact]
    public async Task CoordinatorAwaitsStartupGateBeforeReplay()
    {
        var store = new RpcMutationIdempotencyStore();
        await using var journal = new RecordingGateJournal();
        var coordinator = new RpcMutationIdempotencyCoordinator(store, journal);
        var original = new TryAddAsyncResponse { Added = true };

        var flag = new ExecFlag();

        // Start the operation without awaiting: with the gate closed and the store empty it must block on the
        // startup gate rather than replay or execute.
        var operation = coordinator.ExecuteAsync(
            ValidOperationId,
            "fp-1",
            flag,
            static (state, _) =>
            {
                state.Value = true;
                return Task.FromResult(new TryAddAsyncResponse { Added = false });
            },
            DefaultCancellationToken);

        Assert.False(operation.IsCompleted);

        // Recovery restores the idempotency record and opens the startup gate.
        store.RestoreRecord(ValidOperationId, "fp-1", RpcMutationIdempotencyStore.SerializeResponseBytes(original), DateTime.UtcNow);
        journal.ReleaseStartupGate();

        var response = await operation;

        Assert.True(response.Added);
        Assert.False(flag.Value);
    }

    /// <summary>Ensures the coordinator replays cached responses without re-executing the handler.</summary>
    [Fact]
    public async Task CoordinatorReplaysWithoutReExecutingHandler()
    {
        var store = new RpcMutationIdempotencyStore();
        var coordinator = new RpcMutationIdempotencyCoordinator(store);
        var ctx = new ExecutionCounter();

        var first = await coordinator.ExecuteAsync(
            ValidOperationId,
            "fingerprint",
            ctx,
            static (state, _) =>
            {
                state.Value++;
                return Task.FromResult(new TryAddAsyncResponse { Added = true });
            },
            DefaultCancellationToken);

        var second = await coordinator.ExecuteAsync(
            ValidOperationId,
            "fingerprint",
            ctx,
            static (state, _) =>
            {
                state.Value++;
                return Task.FromResult(new TryAddAsyncResponse { Added = false });
            },
            DefaultCancellationToken);

        Assert.True(first.Added);
        Assert.True(second.Added);
        Assert.Equal(1, ctx.Value);
    }

    /// <summary>Ensures expired idempotency records are swept and no longer replay.</summary>
    [Fact]
    public async Task ExpiredRecordsAreNotReplayed()
    {
        var store = new RpcMutationIdempotencyStore(TimeSpan.FromMilliseconds(50));
        store.RecordSuccess("op-1", "fp-1", RpcMutationIdempotencyStore.SerializeResponseBytes(new TryAddAsyncResponse { Added = true }));

        await Task.Delay(100, DefaultCancellationToken);

        var replayed = store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out var response);

        Assert.False(replayed);
        Assert.Null(response);
    }

    /// <summary>Ensures a recorded success can be replayed from the in-memory cache.</summary>
    [Fact]
    public void RecordSuccessThenTryReplayReturnsCachedResponse()
    {
        var store = new RpcMutationIdempotencyStore();
        var original = new TryAddAsyncResponse { Added = true };
        store.RecordSuccess("op-1", "fp-1", RpcMutationIdempotencyStore.SerializeResponseBytes(original));

        var replayed = store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out var response);

        Assert.True(replayed);
        Assert.NotNull(response);
        Assert.True(response.Added);
    }

    /// <summary>Ensures unknown operation ids do not produce a replayed response.</summary>
    [Fact]
    public void ReplayReturnsFalseWhenOperationIdIsUnknown()
    {
        var store = new RpcMutationIdempotencyStore();
        var replayed = store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out var response);

        Assert.False(replayed);
        Assert.Null(response);
    }

    /// <summary>Ensures conforming operation ids pass validation.</summary>
    [Fact]
    public void RequireOperationIdAcceptsValidValue()
    {
        var normalized = RpcMutationContracts.RequireOperationId(ValidOperationId);
        Assert.Equal(ValidOperationId, normalized);
    }

    /// <summary>Ensures empty operation ids are rejected with the stable invalid-argument contract.</summary>
    [Fact]
    public void RequireOperationIdRejectsEmptyValue()
    {
        var ex = NodeExceptionAssert.For<RpcException>().Throws(string.Empty, static value => _ = RpcMutationContracts.RequireOperationId(value));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal(RpcMutationContracts.OperationIdRequiredDetail, ex.Status.Detail);
    }

    /// <summary>Ensures malformed operation ids are rejected with the stable format contract.</summary>
    [Fact]
    public void RequireOperationIdRejectsInvalidFormat()
    {
        var ex = NodeExceptionAssert.For<RpcException>().Throws("not-a-valid-operation-id", static value => _ = RpcMutationContracts.RequireOperationId(value));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal(RpcMutationContracts.OperationIdInvalidFormatDetail, ex.Status.Detail);
    }

    /// <summary>Ensures over-length operation ids are rejected before format validation.</summary>
    [Fact]
    public void RequireOperationIdRejectsTooLongValue()
    {
        var tooLong = new string('a', RpcMutationContracts.OperationIdLength + 1);
        var ex = NodeExceptionAssert.For<RpcException>().Throws(tooLong, static value => _ = RpcMutationContracts.RequireOperationId(value));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal(RpcMutationContracts.OperationIdTooLongDetail, ex.Status.Detail);
    }

    /// <summary>Ensures uppercase hex operation ids are rejected.</summary>
    [Fact]
    public void RequireOperationIdRejectsUppercaseHex()
    {
        var uppercase = ValidOperationId.ToUpperInvariant();
        var ex = NodeExceptionAssert.For<RpcException>().Throws(uppercase, static value => _ = RpcMutationContracts.RequireOperationId(value));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal(RpcMutationContracts.OperationIdInvalidFormatDetail, ex.Status.Detail);
    }

    /// <summary>Ensures RestoreRecord honors CreatedUtc for retention sweeps after recovery replay.</summary>
    [Fact]
    public void RestoredExpiredRecordIsNotReplayed()
    {
        var store = new RpcMutationIdempotencyStore(TimeSpan.FromMinutes(15));
        var responseBytes = RpcMutationIdempotencyStore.SerializeResponseBytes(new TryAddAsyncResponse { Added = true });
        store.RestoreRecord("op-1", "fp-1", responseBytes, DateTime.UtcNow.AddMinutes(-20));

        var replayed = store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out var response);

        Assert.False(replayed);
        Assert.Null(response);
    }

    /// <summary>Ensures reusing an operation id with a different fingerprint throws a typed exception.</summary>
    [Fact]
    public void ReuseWithDifferentFingerprintThrowsTypedException()
    {
        var store = new RpcMutationIdempotencyStore();
        store.RecordSuccess("op-1", "fp-1", RpcMutationIdempotencyStore.SerializeResponseBytes(new TryAddAsyncResponse { Added = true }));

        var ex = NodeExceptionAssert.For<ServerOpIdMismatchException>().Throws(
            store,
            static value =>
            {
                var replayed = value.TryReplay("op-1", "fp-2", TryAddAsyncResponse.Parser, out var replay);
                Assert.Fail($"Expected reuse mismatch, got replayed={replayed}, replay={replay}");
            });

        Assert.Equal(ServerOpIdMismatchException.StableDetail, ex.Message);
    }

    private sealed class ExecFlag
    {
        internal bool Value { get; set; }
    }

    private sealed class ExecutionCounter
    {
        internal int Value { get; set; }
    }

    private sealed class RecordingGateJournal : IJournalCoordinator
    {
        private readonly JournalStartupGate _gate = new(false);
        private EventHandler? _onAppended;

        public event EventHandler? OnAppended
        {
            add => _onAppended += value;
            remove => _onAppended -= value;
        }

        public long AppendedBytes => 0;

        public long AppendedOps => 0;

        public int CurrentSegmentIndex => 0;

        public bool HasFlushLoopFailure => false;

        public long HighWaterBytes => 0;

        public QuiescenceGate InFlightApplyGate => new();

        public bool IsJournalGroupCommitEnabled => false;

        public long MaxBytes => 0;

        public ulong NextSequence => 0;

        public double RecentAppendLatencyMs => 0;

        public long UsedBytes => 0;

        public ValueTask AppendIdempotencyOutcomeAsync(string operationId, string fingerprint, byte[] responseBytes, CancellationToken cancellationToken) => default;

        public ValueTask AppendPutAndAwaitDurabilityAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken) => default;

        public ValueTask AppendPutAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken) => default;

        public ValueTask AppendRemoveAsync(CacheKey key, CancellationToken cancellationToken) => default;

        public ValueTask AppendRemoveExpirationAsync(CacheKey key, CancellationToken cancellationToken) => default;

        public ValueTask AppendTouchExpirationAsync(CacheKey key, DateTime expiresUtc, CancellationToken cancellationToken) => default;

        public ValueTask AwaitDurabilityCommitAsync(CancellationToken cancellationToken) => default;

        public ValueTask DisposeAsync() => default;

        public ValueTask ExecuteMaintenanceExclusiveAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken) => default;

        public ValueTask<TResult> ExecuteSnapshotCutAsync<TState, TBarrier, TResult>(
            TState state,
            Func<TState, ulong, CancellationToken, ValueTask<TBarrier>> captureUnderBarrier,
            Func<TState, ulong, TBarrier, CancellationToken, ValueTask<TResult>> buildOutsideBarrier,
            CancellationToken cancellationToken) => default;

        public ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TResult>(Func<CancellationToken, ValueTask<TResult>> action, CancellationToken cancellationToken) => default;

        public ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TState, TResult>(
            TState state,
            Func<TState, CancellationToken, ValueTask<TResult>> action,
            CancellationToken cancellationToken) => default;

        public ValueTask WaitForStartupAsync(CancellationToken cancellationToken) => _gate.WaitAsync(cancellationToken);

        internal void ReleaseStartupGate() => _gate.Open();
    }
}
