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
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Tests for the write-ahead idempotency intent that prevents re-executing a mutation with a lost outcome.</summary>
[Immutable]
public sealed class RpcMutationIdempotencyAmbiguityTests : DisposableServerUnitTestBase
{
    private const string ValidOperationId = "0123456789abcdef0123456789abcdef";

    private static readonly TimeSpan JoinTimeout = TimeSpan.FromSeconds(10);

    private readonly Meter _testMeter = new("test");

    /// <summary>A completed reservation replays without executing the handler.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompletedReservationReplaysOutcome(CancellationToken cancellationToken)
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
            cancellationToken);

        _ = await Assert.That(response.Added).IsTrue();
        _ = await Assert.That(flag.Value).IsFalse();
    }

    /// <summary>A duplicate reservation (concurrent execution window) surfaces the unknown outcome instead of re-executing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentReservationUnknownOutcome(CancellationToken cancellationToken)
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
                cancellationToken));

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(ex.Status.Detail)).IsTrue();
        _ = await Assert.That(flag.Value).IsFalse();
    }

    /// <summary>Executing an acquired operation records the outcome so a retry replays it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CoordinatorExecutesOnceAndRecordsOutcome(CancellationToken cancellationToken)
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
            cancellationToken);

        _ = await Assert.That(response.Added).IsTrue();
        _ = await Assert.That(flag.Value).IsTrue();

        var replayed = await coordinator.ExecuteAsync(
            ValidOperationId,
            "fp-1",
            new ExecFlag(),
            static (state, _) =>
            {
                state.Value = true;
                return Task.FromResult(new TryAddAsyncResponse { Added = false });
            },
            cancellationToken);

        _ = await Assert.That(replayed.Added).IsTrue();
    }

    /// <summary>A memory-only failure releases the intent so a retry re-executes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MemoryThrowReleasesForRetry(CancellationToken cancellationToken)
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
                cancellationToken));

        _ = await Assert.That(flag.Value).IsTrue();

        var response = await coordinator.ExecuteAsync(
            ValidOperationId,
            "fp-1",
            flag,
            static (_, _) => Task.FromResult(new TryAddAsyncResponse { Added = true }),
            cancellationToken);

        _ = await Assert.That(response.Added).IsTrue();
    }

    /// <summary>A retry joined to an execution that fails before stamping re-acquires the released intent and executes itself.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task JoinerExecutesAfterReleasedIntent(CancellationToken cancellationToken)
    {
        var store = CreateStore();
        var coordinator = new RpcMutationIdempotencyCoordinator(store);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var flag = new ExecFlag();

        var original = coordinator.ExecuteAsync<TaskCompletionSource, TryAddAsyncResponse>(
            ValidOperationId,
            "fp-1",
            gate,
            static async (g, _) =>
            {
                await g.Task.ConfigureAwait(false);
                throw new InvalidOperationException("boom");
            },
            cancellationToken);
        var retry = coordinator.ExecuteAsync(
            ValidOperationId,
            "fp-1",
            flag,
            static (state, _) =>
            {
                state.Value = true;
                return Task.FromResult(new TryAddAsyncResponse { Added = true });
            },
            cancellationToken);
        var joinedWhileInFlight = !retry.IsCompleted;
        gate.SetResult();
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(original);
        var response = await retry.WaitAsync(JoinTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(joinedWhileInFlight).IsTrue();
        _ = await Assert.That(response.Added).IsTrue();
        _ = await Assert.That(flag.Value).IsTrue();
        _ = await Assert.That(store.ExecutionCount).IsEqualTo(0);
    }

    /// <summary>A retry joined to an execution that fails after stamping surfaces the unknown outcome and never executes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task JoinerSeesUnknownAfterStampedFailure(CancellationToken cancellationToken)
    {
        var store = CreateStore();
        await using var journal = new OutcomeFailingJournal();
        var coordinator = new RpcMutationIdempotencyCoordinator(store, journal);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var flag = new ExecFlag();

        var original = coordinator.ExecuteAsync<(IJournalCoordinator Journal, TaskCompletionSource Gate), TryAddAsyncResponse>(
            ValidOperationId,
            "fp-1",
            (Journal: journal, Gate: gate),
            static async (state, cancellationToken) =>
            {
                await state.Journal.AppendPutAsync(CacheKey.Default("k"), ReadOnlyMemory<byte>.Empty, cancellationToken).ConfigureAwait(false);
                await state.Gate.Task.WaitAsync(CancellationToken.None).ConfigureAwait(false);
                return new TryAddAsyncResponse { Added = true };
            },
            cancellationToken);
        var retry = coordinator.ExecuteAsync(
            ValidOperationId,
            "fp-1",
            flag,
            static (state, _) =>
            {
                state.Value = true;
                return Task.FromResult(new TryAddAsyncResponse { Added = false });
            },
            cancellationToken);
        var joinedWhileInFlight = !retry.IsCompleted;
        gate.SetResult();
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(original);
        var error = await NodeAsyncAssert.ThrowsAsync<RpcException>(retry.WaitAsync(JoinTimeout, TimeProvider.System, cancellationToken));

        _ = await Assert.That(joinedWhileInFlight).IsTrue();
        _ = await Assert.That(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(error.Status.Detail)).IsTrue();
        _ = await Assert.That(flag.Value).IsFalse();
        _ = await Assert.That(store.ExecutionCount).IsEqualTo(0);
    }

    /// <summary>An outcome-append failure leaves no completed record: retry surfaces unknown.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OutcomeAppendFailureStaysUnknown(CancellationToken cancellationToken)
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
                cancellationToken));

        _ = await Assert.That(flag.Value).IsTrue();
        _ = await Assert.That(store.TryReplay(ValidOperationId, "fp-1", TryAddAsyncResponse.Parser, out _)).IsFalse();

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
                cancellationToken));

        _ = await Assert.That(error.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(error.Status.Detail)).IsTrue();
    }

    /// <summary>Releasing an unknown, completed, or mismatched reservation is a no-op.</summary>
    [Test]
    public async Task ReleaseIntentIgnoresForeignRecords()
    {
        var store = CreateStore();
        store.ReleaseIntent(ValidOperationId, "fp-1");

        _ = store.ReserveIntent(ValidOperationId, "fp-1");
        store.ReleaseIntent(ValidOperationId, "fp-2");
        _ = await Assert.That(store.ReserveIntent(ValidOperationId, "fp-1")).IsEqualTo(IdempotencyReserveResult.AlreadyStarted);

        store.RecordSuccess(ValidOperationId, "fp-1", IdempotencyResponseCodec.SerializeResponseBytes(new TryAddAsyncResponse { Added = true }));
        store.ReleaseIntent(ValidOperationId, "fp-1");
        _ = await Assert.That(store.ReserveIntent(ValidOperationId, "fp-1")).IsEqualTo(IdempotencyReserveResult.AlreadyCompleted);
    }

    /// <summary>Releasing a started record restored without a fingerprint is a no-op.</summary>
    [Test]
    public async Task ReleaseIntentKeepsUnfingerprinted()
    {
        var store = CreateStore();
        store.RestoreStarted(ValidOperationId, DateTime.UtcNow);
        store.ReleaseIntent(ValidOperationId, "fp-1");

        _ = await Assert.That(store.ReserveIntent(ValidOperationId, "fp-1")).IsEqualTo(IdempotencyReserveResult.AlreadyStarted);
    }

    /// <summary>Releasing a started reservation lets the operation be reserved again.</summary>
    [Test]
    public async Task ReleaseIntentReacquiresReservation()
    {
        var store = CreateStore();

        _ = await Assert.That(store.ReserveIntent(ValidOperationId, "fp-1")).IsEqualTo(IdempotencyReserveResult.Acquired);
        store.ReleaseIntent(ValidOperationId, "fp-1");

        _ = await Assert.That(store.ReserveIntent(ValidOperationId, "fp-1")).IsEqualTo(IdempotencyReserveResult.Acquired);
    }

    /// <summary>Reserving an intent acquires execution ownership; a second reservation sees the started record.</summary>
    [Test]
    public async Task ReserveIntentAcquiresThenReportsStarted()
    {
        var store = CreateStore();

        _ = await Assert.That(store.ReserveIntent(ValidOperationId, "fp-1")).IsEqualTo(IdempotencyReserveResult.Acquired);
        _ = await Assert.That(store.ReserveIntent(ValidOperationId, "fp-1")).IsEqualTo(IdempotencyReserveResult.AlreadyStarted);
        _ = await Assert.That(store.TryReplay(ValidOperationId, "fp-1", TryAddAsyncResponse.Parser, out _)).IsFalse();
    }

    /// <summary>Reusing a started operation id with a different fingerprint is rejected.</summary>
    [Test]
    public async Task ReserveIntentRejectsFingerprintMismatch()
    {
        var store = CreateStore();
        _ = store.ReserveIntent(ValidOperationId, "fp-1");

        var ex = NodeExceptionAssert.For<ServerOpIdMismatchException>().Throws(store, static value => _ = value.ReserveIntent(ValidOperationId, "fp-2"));

        _ = await Assert.That(ex.Message).IsEqualTo(ServerOpIdMismatchException.StableDetail);
    }

    /// <summary>A second restore for the same operation keeps the first record.</summary>
    [Test]
    public async Task RestoreStartedKeepsFirstRecord()
    {
        var store = CreateStore();
        store.RestoreStarted(ValidOperationId, "fp-1", DateTime.UtcNow);
        store.RestoreStarted(ValidOperationId, "fp-2", DateTime.UtcNow);

        _ = await Assert.That(store.ReserveIntent(ValidOperationId, "fp-1")).IsEqualTo(IdempotencyReserveResult.AlreadyStarted);
    }

    /// <summary>A completed outcome restored during replay supersedes a previously restored started record.</summary>
    [Test]
    public async Task RestoredOutcomeSupersedesRestoredStarted()
    {
        var store = CreateStore();
        store.RestoreStarted(ValidOperationId, DateTime.UtcNow);
        store.RestoreRecord(ValidOperationId, "fp-1", IdempotencyResponseCodec.SerializeResponseBytes(new TryAddAsyncResponse { Added = true }), DateTime.UtcNow);

        var replayed = store.TryReplay(ValidOperationId, "fp-1", TryAddAsyncResponse.Parser, out var response);
        _ = await Assert.That(replayed).IsTrue();
        _ = await Assert.That(response).IsNotNull();
        _ = await Assert.That(response.Added).IsTrue();
    }

    /// <summary>A started record restored from recovery replay blocks replay but stays in the store as unknown.</summary>
    [Test]
    public async Task RestoredStartedRecordBlocksReplay()
    {
        var store = CreateStore();
        store.RestoreStarted(ValidOperationId, DateTime.UtcNow);

        _ = await Assert.That(store.TryReplay(ValidOperationId, "fp-1", TryAddAsyncResponse.Parser, out _)).IsFalse();
        _ = await Assert.That(store.ReserveIntent(ValidOperationId, "fp-1")).IsEqualTo(IdempotencyReserveResult.AlreadyStarted);
    }

    /// <summary>Recording a success replaces the write-ahead intent with a replayable completed outcome.</summary>
    [Test]
    public async Task SuccessAfterIntentReplayable()
    {
        var store = CreateStore();
        _ = store.ReserveIntent(ValidOperationId, "fp-1");
        store.RecordSuccess(ValidOperationId, "fp-1", IdempotencyResponseCodec.SerializeResponseBytes(new TryAddAsyncResponse { Added = true }));

        _ = await Assert.That(store.ReserveIntent(ValidOperationId, "fp-1")).IsEqualTo(IdempotencyReserveResult.AlreadyCompleted);
        var replayed = store.TryReplay(ValidOperationId, "fp-1", TryAddAsyncResponse.Parser, out var response);
        _ = await Assert.That(replayed).IsTrue();
        _ = await Assert.That(response).IsNotNull();
        _ = await Assert.That(response.Added).IsTrue();
    }

    /// <summary>A retry for a recovered started record surfaces COMMIT_OUTCOME_UNKNOWN and does not execute the handler.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnknownOutcomeSkipsHandler(CancellationToken cancellationToken)
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
                cancellationToken));

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(ex.Status.Detail)).IsTrue();
        _ = await Assert.That(flag.Value).IsFalse();
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

        public int CurrentSegmentIndex => 0;

        public bool HasFlushLoopFailure => false;

        public long HighWaterBytes => 0;

        public QuiescenceGate InFlightApplyGate => throw new NotSupportedException();

        public bool IsJournalGroupCommitEnabled => false;

        public long MaxBytes => 0;

        public ulong NextSequence => 0;

        public double RecentAppendLatencyMs => 0;

        public long UsedBytes => 0;

        public ValueTask AppendIdempotencyOutcomeAsync(string operationId, string fingerprint, byte[] responseBytes, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("boom");

        public ValueTask AppendPutAndAwaitDurabilityAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask AppendPutAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken)
        {
            RpcMutationIdempotencyExecutionAmbient.NotifyMutationStamped();
            OnAppended?.Invoke(this, EventArgs.Empty);
            return ValueTask.CompletedTask;
        }

        public ValueTask AppendRemoveAsync(CacheKey key, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask AppendRemoveExpirationAsync(CacheKey key, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask AppendTouchExpirationAsync(CacheKey key, DateTime expiresUtc, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask AwaitDurabilityCommitAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public ValueTask ExecuteMaintenanceExclusiveAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<TResult> ExecuteSnapshotCutAsync<TState, TBarrier, TResult>(
            TState state,
            Func<TState, ulong, CancellationToken, ValueTask<TBarrier>> captureUnderBarrier,
            Func<TState, ulong, TBarrier, CancellationToken, ValueTask<TResult>> buildOutsideBarrier,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TResult>(Func<CancellationToken, ValueTask<TResult>> action, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TState, TResult>(
            TState state,
            Func<TState, CancellationToken, ValueTask<TResult>> action,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask ExecuteUnderSnapshotBarrierAsync<TState>(TState state, Func<TState, CancellationToken, ValueTask> action, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public void FailJournalPipeline(Exception reason) => throw new NotSupportedException();

        public ValueTask WaitForStartupAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }
}
