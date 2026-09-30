using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Rocks;
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
        var coordinator = new RpcMutationIdempotencyCoordinator(store, NullLogger<RpcMutationIdempotencyCoordinator>.Instance);
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
        _ = store.ReserveIntent(ValidOperationId, "fp-1", null, out _);
        var coordinator = new RpcMutationIdempotencyCoordinator(store, NullLogger<RpcMutationIdempotencyCoordinator>.Instance);
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
        var coordinator = new RpcMutationIdempotencyCoordinator(store, NullLogger<RpcMutationIdempotencyCoordinator>.Instance);
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
        var coordinator = new RpcMutationIdempotencyCoordinator(store, NullLogger<RpcMutationIdempotencyCoordinator>.Instance);
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
        var coordinator = new RpcMutationIdempotencyCoordinator(store, NullLogger<RpcMutationIdempotencyCoordinator>.Instance);
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

    /// <summary>An execution that fails after stamping and a retry joined to it both surface the unknown outcome; the retry never executes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task JoinerSeesUnknownAfterStampedFailure(CancellationToken cancellationToken)
    {
        var store = CreateStore();
        await using var journal = CreateOutcomeFailingJournal();
        var coordinator = new RpcMutationIdempotencyCoordinator(store, journal, NullLogger<RpcMutationIdempotencyCoordinator>.Instance);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var flag = new ExecFlag();

        var original = coordinator.ExecuteAsync<(IJournalCoordinator Journal, TaskCompletionSource Gate), TryAddAsyncResponse>(
            ValidOperationId,
            "fp-1",
            (Journal: journal, Gate: gate),
            static async (state, cancellationToken) =>
            {
                await state.Journal.AppendPutAsync(default, CacheKey.Default("k"), ReadOnlyMemory<byte>.Empty, cancellationToken).ConfigureAwait(false);
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
        var originalError = await NodeAsyncAssert.ThrowsAsync<RpcException>(original);
        var error = await NodeAsyncAssert.ThrowsAsync<RpcException>(retry.WaitAsync(JoinTimeout, TimeProvider.System, cancellationToken));

        _ = await Assert.That(joinedWhileInFlight).IsTrue();
        _ = await Assert.That(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(originalError.Status.Detail)).IsTrue();
        _ = await Assert.That(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(error.Status.Detail)).IsTrue();
        _ = await Assert.That(flag.Value).IsFalse();
        _ = await Assert.That(store.ExecutionCount).IsEqualTo(0);
    }

    /// <summary>An outcome-append failure after stamping leaves no completed record: the first caller and a retry both surface unknown.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OutcomeAppendFailureStaysUnknown(CancellationToken cancellationToken)
    {
        var store = CreateStore();
        await using var journal = CreateOutcomeFailingJournal();
        var coordinator = new RpcMutationIdempotencyCoordinator(store, journal, NullLogger<RpcMutationIdempotencyCoordinator>.Instance);
        var flag = new ExecFlag();

        var firstError = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            coordinator.ExecuteAsync<(IJournalCoordinator Journal, ExecFlag Flag), TryAddAsyncResponse>(
                ValidOperationId,
                "fp-1",
                (Journal: journal, Flag: flag),
                static async (state, cancellationToken) =>
                {
                    await state.Journal.AppendPutAsync(default, CacheKey.Default("k"), ReadOnlyMemory<byte>.Empty, cancellationToken).ConfigureAwait(false);
                    state.Flag.Value = true;
                    return new TryAddAsyncResponse { Added = true };
                },
                cancellationToken));

        _ = await Assert.That(flag.Value).IsTrue();
        _ = await Assert.That(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(firstError.Status.Detail)).IsTrue();
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

        _ = store.ReserveIntent(ValidOperationId, "fp-1", null, out _);
        store.ReleaseIntent(ValidOperationId, "fp-2");
        _ = await Assert.That(store.ReserveIntent(ValidOperationId, "fp-1", null, out _)).IsEqualTo(IdempotencyReserveResult.AlreadyStarted);

        store.RecordSuccess(ValidOperationId, "fp-1", IdempotencyResponseCodec.SerializeResponseBytes(new TryAddAsyncResponse { Added = true }));
        store.ReleaseIntent(ValidOperationId, "fp-1");
        _ = await Assert.That(store.ReserveIntent(ValidOperationId, "fp-1", null, out _)).IsEqualTo(IdempotencyReserveResult.AlreadyCompleted);
    }

    /// <summary>Releasing a started record restored without a fingerprint is a no-op.</summary>
    [Test]
    public async Task ReleaseIntentKeepsUnfingerprinted()
    {
        var store = CreateStore();
        store.RestoreStarted(ValidOperationId, DateTime.UtcNow);
        store.ReleaseIntent(ValidOperationId, "fp-1");

        _ = await Assert.That(store.ReserveIntent(ValidOperationId, "fp-1", null, out _)).IsEqualTo(IdempotencyReserveResult.AlreadyStarted);
    }

    /// <summary>Releasing a started reservation lets the operation be reserved again.</summary>
    [Test]
    public async Task ReleaseIntentReacquiresReservation()
    {
        var store = CreateStore();

        _ = await Assert.That(store.ReserveIntent(ValidOperationId, "fp-1", null, out _)).IsEqualTo(IdempotencyReserveResult.Acquired);
        store.ReleaseIntent(ValidOperationId, "fp-1");

        _ = await Assert.That(store.ReserveIntent(ValidOperationId, "fp-1", null, out _)).IsEqualTo(IdempotencyReserveResult.Acquired);
    }

    /// <summary>Reserving an intent acquires execution ownership; a second reservation sees the started record.</summary>
    [Test]
    public async Task ReserveIntentAcquiresThenReportsStarted()
    {
        var store = CreateStore();

        _ = await Assert.That(store.ReserveIntent(ValidOperationId, "fp-1", null, out _)).IsEqualTo(IdempotencyReserveResult.Acquired);
        _ = await Assert.That(store.ReserveIntent(ValidOperationId, "fp-1", null, out _)).IsEqualTo(IdempotencyReserveResult.AlreadyStarted);
        _ = await Assert.That(store.TryReplay(ValidOperationId, "fp-1", TryAddAsyncResponse.Parser, out _)).IsFalse();
    }

    /// <summary>Reusing a started operation id with a different fingerprint is rejected.</summary>
    [Test]
    public async Task ReserveIntentRejectsFingerprintMismatch()
    {
        var store = CreateStore();
        _ = store.ReserveIntent(ValidOperationId, "fp-1", null, out _);

        var ex = NodeExceptionAssert.For<ServerOpIdMismatchException>().Throws(store, static value => _ = value.ReserveIntent(ValidOperationId, "fp-2", null, out _));

        _ = await Assert.That(ex.Message).IsEqualTo(ServerOpIdMismatchException.StableDetail);
    }

    /// <summary>A second restore for the same operation keeps the first record.</summary>
    [Test]
    public async Task RestoreStartedKeepsFirstRecord()
    {
        var store = CreateStore();
        store.RestoreStarted(ValidOperationId, "fp-1", DateTime.UtcNow);
        store.RestoreStarted(ValidOperationId, "fp-2", DateTime.UtcNow);

        _ = await Assert.That(store.ReserveIntent(ValidOperationId, "fp-1", null, out _)).IsEqualTo(IdempotencyReserveResult.AlreadyStarted);
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
        _ = await Assert.That(store.ReserveIntent(ValidOperationId, "fp-1", null, out _)).IsEqualTo(IdempotencyReserveResult.AlreadyStarted);
    }

    /// <summary>Recording a success replaces the write-ahead intent with a replayable completed outcome.</summary>
    [Test]
    public async Task SuccessAfterIntentReplayable()
    {
        var store = CreateStore();
        _ = store.ReserveIntent(ValidOperationId, "fp-1", null, out _);
        store.RecordSuccess(ValidOperationId, "fp-1", IdempotencyResponseCodec.SerializeResponseBytes(new TryAddAsyncResponse { Added = true }));

        _ = await Assert.That(store.ReserveIntent(ValidOperationId, "fp-1", null, out _)).IsEqualTo(IdempotencyReserveResult.AlreadyCompleted);
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
        var coordinator = new RpcMutationIdempotencyCoordinator(store, NullLogger<RpcMutationIdempotencyCoordinator>.Instance);
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

    /// <summary>Mocks a journal that stamps mutation appends but fails outcome appends.</summary>
    /// <returns>The mocked journal.</returns>
    private static IJournalCoordinator CreateOutcomeFailingJournal()
    {
        var expectations = new IJournalCoordinatorCreateExpectations();
        _ = expectations.Setups.WaitForStartupAsync(Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        _ = expectations.Setups.AppendPutAsync(Arg.Any<AsyncLockOwnership>(), Arg.Any<CacheKey>(), Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<CancellationToken>())
                        .Callback(static (_, _, _, _) =>
                         {
                             RpcMutationIdempotencyExecutionAmbient.NotifyMutationStamped();
                             return ValueTask.CompletedTask;
                         });
        _ = expectations.Setups.AppendIdempotencyOutcomeAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
                        .Throws(new InvalidOperationException("boom"));
        _ = expectations.Setups.DisposeAsync().ReturnValue(ValueTask.CompletedTask);
        return expectations.Instance();
    }

    private RpcMutationIdempotencyStore CreateStore() => new(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));

    private sealed class ExecFlag
    {
        internal bool Value { get; set; }
    }
}
