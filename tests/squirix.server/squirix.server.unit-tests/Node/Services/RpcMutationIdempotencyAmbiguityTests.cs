using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Tests for the write-ahead idempotency intent that prevents re-executing a mutation with a lost outcome.</summary>
[Immutable]
public sealed class RpcMutationIdempotencyAmbiguityTests : DisposableServerUnitTestBase
{
    private const string ValidOperationId = "0123456789abcdef0123456789abcdef";

    private readonly Meter _testMeter = new("test");

    /// <summary>Reserving an intent acquires execution ownership; a second reservation sees the started record.</summary>
    [Fact]
    public void ReserveIntentAcquiresThenReportsStarted()
    {
        var store = CreateStore();

        Assert.Equal(IdempotencyReserveResult.Acquired, store.ReserveIntent(ValidOperationId, "fp-1"));
        Assert.Equal(IdempotencyReserveResult.AlreadyStarted, store.ReserveIntent(ValidOperationId, "fp-1"));
        Assert.False(store.TryReplay(ValidOperationId, "fp-1", TryAddAsyncResponse.Parser, out _));
    }

    /// <summary>Recording a success replaces the write-ahead intent with a replayable completed outcome.</summary>
    [Fact]
    public void SuccessAfterIntentReplayable()
    {
        var store = CreateStore();
        _ = store.ReserveIntent(ValidOperationId, "fp-1");
        store.RecordSuccess(ValidOperationId, "fp-1", IdempotencyResponseCodec.SerializeResponseBytes(new TryAddAsyncResponse { Added = true }));

        Assert.Equal(IdempotencyReserveResult.AlreadyCompleted, store.ReserveIntent(ValidOperationId, "fp-1"));
        var replayed = store.TryReplay(ValidOperationId, "fp-1", TryAddAsyncResponse.Parser, out var response);
        Assert.True(replayed);
        Assert.NotNull(response);
        Assert.True(response.Added);
    }

    /// <summary>Reusing a started operation id with a different fingerprint is rejected.</summary>
    [Fact]
    public void ReserveIntentRejectsFingerprintMismatch()
    {
        var store = CreateStore();
        _ = store.ReserveIntent(ValidOperationId, "fp-1");

        var ex = NodeExceptionAssert.For<ServerOpIdMismatchException>().Throws(
            store,
            static value => _ = value.ReserveIntent(ValidOperationId, "fp-2"));

        Assert.Equal(ServerOpIdMismatchException.StableDetail, ex.Message);
    }

    /// <summary>A started record restored from recovery replay blocks replay but stays in the store as unknown.</summary>
    [Fact]
    public void RestoredStartedRecordBlocksReplay()
    {
        var store = CreateStore();
        store.RestoreStarted(ValidOperationId, DateTime.UtcNow);

        Assert.False(store.TryReplay(ValidOperationId, "fp-1", TryAddAsyncResponse.Parser, out _));
        Assert.Equal(IdempotencyReserveResult.AlreadyStarted, store.ReserveIntent(ValidOperationId, "fp-1"));
    }

    /// <summary>A completed outcome restored during replay supersedes a previously restored started record.</summary>
    [Fact]
    public void RestoredOutcomeSupersedesRestoredStarted()
    {
        var store = CreateStore();
        store.RestoreStarted(ValidOperationId, DateTime.UtcNow);
        store.RestoreRecord(ValidOperationId, "fp-1", IdempotencyResponseCodec.SerializeResponseBytes(new TryAddAsyncResponse { Added = true }), DateTime.UtcNow);

        var replayed = store.TryReplay(ValidOperationId, "fp-1", TryAddAsyncResponse.Parser, out var response);
        Assert.True(replayed);
        Assert.NotNull(response);
        Assert.True(response.Added);
    }

    /// <summary>A retry for a recovered started record surfaces COMMIT_OUTCOME_UNKNOWN and does not execute the handler.</summary>
    [Fact]
    public async Task UnknownOutcomeSkipsHandler()
    {
        var store = CreateStore();
        store.RestoreStarted(ValidOperationId, DateTime.UtcNow);
        var coordinator = new RpcMutationIdempotencyCoordinator(store);
        var flag = new ExecFlag();

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            coordinator.ExecuteAsync(
                ValidOperationId,
                "fp-1",
                flag,
                static (state, _) =>
                {
                    state.Value = true;
                    return Task.FromResult(new TryAddAsyncResponse { Added = false });
                },
                DefaultCancellationToken));

        Assert.Equal(StatusCode.Unavailable, ex.StatusCode);
        Assert.True(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(ex.Status.Detail));
        Assert.False(flag.Value);
    }

    /// <summary>A duplicate reservation (concurrent execution window) surfaces the unknown outcome instead of re-executing.</summary>
    [Fact]
    public async Task ConcurrentReservationUnknownOutcome()
    {
        var store = CreateStore();
        _ = store.ReserveIntent(ValidOperationId, "fp-1");
        var coordinator = new RpcMutationIdempotencyCoordinator(store);
        var flag = new ExecFlag();

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            coordinator.ExecuteAsync(
                ValidOperationId,
                "fp-1",
                flag,
                static (state, _) =>
                {
                    state.Value = true;
                    return Task.FromResult(new TryAddAsyncResponse { Added = false });
                },
                DefaultCancellationToken));

        Assert.Equal(StatusCode.Unavailable, ex.StatusCode);
        Assert.True(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(ex.Status.Detail));
        Assert.False(flag.Value);
    }

    /// <summary>Executing an acquired operation records the outcome so a retry replays it.</summary>
    [Fact]
    public async Task CoordinatorExecutesOnceAndRecordsOutcome()
    {
        var store = CreateStore();
        var coordinator = new RpcMutationIdempotencyCoordinator(store);
        var flag = new ExecFlag();

        var response = await coordinator.ExecuteAsync(
            ValidOperationId,
            "fp-1",
            flag,
            static (state, _) =>
            {
                state.Value = true;
                return Task.FromResult(new TryAddAsyncResponse { Added = true });
            },
            DefaultCancellationToken);

        Assert.True(response.Added);
        Assert.True(flag.Value);

        var replayed = await coordinator.ExecuteAsync(
            ValidOperationId,
            "fp-1",
            new ExecFlag(),
            static (state, _) =>
            {
                state.Value = true;
                return Task.FromResult(new TryAddAsyncResponse { Added = false });
            },
            DefaultCancellationToken);

        Assert.True(replayed.Added);
    }

    /// <summary>Releasing a started reservation lets the operation be reserved again.</summary>
    [Fact]
    public void ReleaseIntentReacquiresReservation()
    {
        var store = CreateStore();

        Assert.Equal(IdempotencyReserveResult.Acquired, store.ReserveIntent(ValidOperationId, "fp-1"));
        store.ReleaseIntent(ValidOperationId, "fp-1");

        Assert.Equal(IdempotencyReserveResult.Acquired, store.ReserveIntent(ValidOperationId, "fp-1"));
    }

    /// <summary>Releasing an unknown, completed, or mismatched reservation is a no-op.</summary>
    [Fact]
    public void ReleaseIntentIgnoresForeignRecords()
    {
        var store = CreateStore();
        store.ReleaseIntent(ValidOperationId, "fp-1");

        _ = store.ReserveIntent(ValidOperationId, "fp-1");
        store.ReleaseIntent(ValidOperationId, "fp-2");
        Assert.Equal(IdempotencyReserveResult.AlreadyStarted, store.ReserveIntent(ValidOperationId, "fp-1"));

        store.RecordSuccess(ValidOperationId, "fp-1", IdempotencyResponseCodec.SerializeResponseBytes(new TryAddAsyncResponse { Added = true }));
        store.ReleaseIntent(ValidOperationId, "fp-1");
        Assert.Equal(IdempotencyReserveResult.AlreadyCompleted, store.ReserveIntent(ValidOperationId, "fp-1"));
    }

    /// <summary>Releasing a started record restored without a fingerprint is a no-op.</summary>
    [Fact]
    public void ReleaseIntentKeepsUnfingerprinted()
    {
        var store = CreateStore();
        store.RestoreStarted(ValidOperationId, DateTime.UtcNow);
        store.ReleaseIntent(ValidOperationId, "fp-1");

        Assert.Equal(IdempotencyReserveResult.AlreadyStarted, store.ReserveIntent(ValidOperationId, "fp-1"));
    }

    /// <summary>A completed reservation replays without executing the handler.</summary>
    [Fact]
    public async Task CompletedReservationReplaysOutcome()
    {
        var store = CreateStore();
        store.RecordSuccess(ValidOperationId, "fp-1", IdempotencyResponseCodec.SerializeResponseBytes(new TryAddAsyncResponse { Added = true }));
        var coordinator = new RpcMutationIdempotencyCoordinator(store);
        var flag = new ExecFlag();

        var response = await coordinator.ExecuteAsync(
            ValidOperationId,
            "fp-1",
            flag,
            static (state, _) =>
            {
                state.Value = true;
                return Task.FromResult(new TryAddAsyncResponse { Added = false });
            },
            DefaultCancellationToken);

        Assert.True(response.Added);
        Assert.False(flag.Value);
    }

    /// <summary>A memory-only failure releases the intent so a retry re-executes.</summary>
    [Fact]
    public async Task MemoryThrowReleasesForRetry()
    {
        var store = CreateStore();
        var coordinator = new RpcMutationIdempotencyCoordinator(store);
        var flag = new ExecFlag();

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(
            coordinator.ExecuteAsync(
                ValidOperationId,
                "fp-1",
                flag,
                static (state, _) =>
                {
                    state.Value = true;
                    return Task.FromException<TryAddAsyncResponse>(new InvalidOperationException("boom"));
                },
                DefaultCancellationToken));

        Assert.True(flag.Value);

        var response = await coordinator.ExecuteAsync(
            ValidOperationId,
            "fp-1",
            flag,
            static (_, _) => Task.FromResult(new TryAddAsyncResponse { Added = true }),
            DefaultCancellationToken);

        Assert.True(response.Added);
    }

    /// <summary>A second restore for the same operation keeps the first record.</summary>
    [Fact]
    public void RestoreStartedKeepsFirstRecord()
    {
        var store = CreateStore();
        store.RestoreStarted(ValidOperationId, "fp-1", DateTime.UtcNow);
        store.RestoreStarted(ValidOperationId, "fp-2", DateTime.UtcNow);

        Assert.Equal(IdempotencyReserveResult.AlreadyStarted, store.ReserveIntent(ValidOperationId, "fp-1"));
    }

    /// <summary>An outcome-append failure leaves no completed record: retry surfaces unknown.</summary>
    [Fact]
    public async Task OutcomeAppendFailureStaysUnknown()
    {
        var store = CreateStore();
        await using var journal = new OutcomeFailingJournal();
        var coordinator = new RpcMutationIdempotencyCoordinator(store, journal);
        var flag = new ExecFlag();

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(
            coordinator.ExecuteAsync<(IJournalCoordinator Journal, ExecFlag Flag), TryAddAsyncResponse>(
                ValidOperationId,
                "fp-1",
                (Journal: journal, Flag: flag),
                static async (state, cancellationToken) =>
                {
                    await state.Journal.AppendPutAsync(CacheKey.Default("k"), ReadOnlyMemory<byte>.Empty, cancellationToken).ConfigureAwait(false);
                    state.Flag.Value = true;
                    return new TryAddAsyncResponse { Added = true };
                },
                DefaultCancellationToken));

        Assert.True(flag.Value);
        Assert.False(store.TryReplay(ValidOperationId, "fp-1", TryAddAsyncResponse.Parser, out _));

        var error = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            coordinator.ExecuteAsync(
                ValidOperationId,
                "fp-1",
                flag,
                static (state, _) =>
                {
                    state.Value = true;
                    return Task.FromResult(new TryAddAsyncResponse { Added = false });
                },
                DefaultCancellationToken));

        Assert.Equal(StatusCode.Unavailable, error.StatusCode);
        Assert.True(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(error.Status.Detail));
    }

    private RpcMutationIdempotencyStore CreateStore() => new(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));

    private sealed class ExecFlag
    {
        internal bool Value { get; set; }
    }

    /// <summary>A journal that stamps mutation appends but fails outcome appends.</summary>
    private sealed class OutcomeFailingJournal : IJournalCoordinator
    {
        public event EventHandler? OnAppended;

        public long AppendedBytes => 0;

        public long AppendedOps => 0;

        public double RecentAppendLatencyMs => 0;

        public long HighWaterBytes => 0;

        public long MaxBytes => 0;

        public long UsedBytes => 0;

        public QuiescenceGate InFlightApplyGate => throw new NotSupportedException();

        public int CurrentSegmentIndex => 0;

        public bool HasFlushLoopFailure => false;

        public bool IsJournalGroupCommitEnabled => false;

        public ulong NextSequence => 0;

        public ValueTask AppendIdempotencyOutcomeAsync(string operationId, string fingerprint, byte[] responseBytes, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("boom");

        public ValueTask AppendPutAndAwaitDurabilityAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask AppendPutAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken)
        {
            RpcMutationIdempotencyExecutionAmbient.NotifyMutationStamped();
            OnAppended?.Invoke(this, EventArgs.Empty);
            return ValueTask.CompletedTask;
        }

        public ValueTask AppendRemoveAsync(CacheKey key, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask AppendRemoveExpirationAsync(CacheKey key, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask AppendTouchExpirationAsync(CacheKey key, DateTime expiresUtc, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask AwaitDurabilityCommitAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public ValueTask ExecuteMaintenanceExclusiveAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask<TResult> ExecuteSnapshotCutAsync<TState, TBarrier, TResult>(
            TState state,
            Func<TState, ulong, CancellationToken, ValueTask<TBarrier>> captureUnderBarrier,
            Func<TState, ulong, TBarrier, CancellationToken, ValueTask<TResult>> buildOutsideBarrier,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TResult>(Func<CancellationToken, ValueTask<TResult>> action, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TState, TResult>(
            TState state,
            Func<TState, CancellationToken, ValueTask<TResult>> action,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask ExecuteUnderSnapshotBarrierAsync<TState>(
            TState state,
            Func<TState, CancellationToken, ValueTask> action,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask WaitForStartupAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }
}
