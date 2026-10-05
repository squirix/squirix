using System;
using System.Diagnostics.Metrics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.App;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>A hosted mutation RPC becomes visible to readers only after its own journal frame is covered by a completed flush.</summary>
[Immutable]
public sealed class DurableVisibilityTests : IsolatedStorageTestBase
{
    private const string Fingerprint = "fp-1";

    private const string OperationId = "0123456789abcdef0123456789abcdef";

    private const string OtherOperationId = "fedcba9876543210fedcba9876543210";

    private static readonly TimeSpan ShouldCompleteTimeout = TimeSpan.FromSeconds(5);

    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    private static readonly TimeSpan UnreachedBatchDeadline = TimeSpan.FromHours(1);

    private static readonly string KeyA = CacheKey.Default("a").ToString();

    private static readonly string KeyW = CacheKey.Default("w").ToString();

    private readonly Meter _testMeter = new("test");

    /// <summary>While the flush that covers the frame is stalled, the frame is in the file but memory does not hold the value yet.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ReaderCannotObserveWriteBeforeFlush(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await CreateWarmJournalAsync(groupCommit, cancellationToken);
        var target = new Target(journal, CreateStore());
        journal.Writer.Flush.Arm();
        string visibleDuringStall;
        string framesDuringStall;
        TryAddAsyncResponse response;
        try
        {
            var put = target.PutAsync(OperationId, "a", cancellationToken);
            await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            visibleDuringStall = target.Memory.Snapshot;
            framesDuringStall = journal.ReadStampedPuts(cancellationToken);
            journal.Writer.Flush.Release();
            response = await put.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        }
        finally
        {
            journal.Writer.ReleaseAll();
        }

        await journal.ShutdownAsync();

        _ = await Assert.That(framesDuringStall).IsEqualTo(StallableJournal.Describe([$"{KeyA}#{OperationId}", KeyW]));
        _ = await Assert.That(visibleDuringStall).IsEmpty();
        _ = await Assert.That(response.Added).IsTrue();
        _ = await Assert.That(target.Memory.Snapshot).IsEqualTo(KeyA);
    }

    /// <summary>A flush that fails never exposes the write: the writer gets the unknown outcome, memory stays empty, the pipeline is latched and a retry stays unknown.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task FailedFlushNeverExposesWrite(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await CreateWarmJournalAsync(groupCommit, cancellationToken);
        var target = new Target(journal, CreateStore());
        journal.Writer.Flush.Arm();
        SquirixException originalError;
        RpcException retryError;
        try
        {
            var put = target.PutAsync(OperationId, "a", cancellationToken);
            await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            journal.Writer.Flush.ReleaseWithFailure(new IOException("device lost"));
            originalError = await NodeAsyncAssert.ThrowsAsync<SquirixException>(put.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            retryError = await NodeAsyncAssert.ThrowsAsync<RpcException>(target.PutAsync(OperationId, "a", cancellationToken).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        }
        finally
        {
            journal.Writer.ReleaseAll();
        }

        _ = await Assert.That(originalError.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        _ = await Assert.That(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(retryError.Status.Detail)).IsTrue();
        _ = await Assert.That(target.Memory.Snapshot).IsEmpty();
        _ = await Assert.That(journal.Journal.GetJournalThreadFailure()).IsNotNull();
        _ = await Assert.That(journal.Journal.InFlightApplyGate.HasPending).IsFalse();
    }

    /// <summary>A write of another key is admitted while a write is stalled in its flush, so distinct keys share flushes instead of queueing behind the mutation gate.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DistinctKeyAdmittedDuringStalledFlush(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await CreateWarmJournalAsync(groupCommit, cancellationToken);
        var target = new Target(journal, CreateStore());
        var preconditionRan = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        journal.Writer.Flush.Arm();
        TryAddAsyncResponse response;
        int other;
        try
        {
            var put = target.PutAsync(OperationId, "a", cancellationToken);
            await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            var otherPut = target.PutObservingPreconditionAsync("b", preconditionRan, cancellationToken);
            await preconditionRan.Task.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            journal.Writer.Flush.Release();
            response = await put.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            other = await otherPut.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        }
        finally
        {
            journal.Writer.ReleaseAll();
        }

        _ = await Assert.That(response.Added).IsTrue();
        _ = await Assert.That(other).IsEqualTo(1);
        _ = await Assert.That(target.Memory.Snapshot).IsEqualTo(KeyA);
    }

    /// <summary>A same-key successor waits for the durable apply of the earlier write: its precondition sees the earlier value and appends nothing.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SameKeySuccessorWaitsForDurableApply(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await CreateWarmJournalAsync(groupCommit, cancellationToken);
        var target = new Target(journal, CreateStore());
        journal.Writer.Flush.Arm();
        var preconditionRan = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        TryAddAsyncResponse first;
        TryAddAsyncResponse second;
        bool pendingDuringStall;
        bool preconditionRanDuringStall;
        string framesDuringStall;
        try
        {
            var put = target.PutAsync(OperationId, "a", cancellationToken);
            await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            var add = target.AddAsync(OtherOperationId, "a", preconditionRan, cancellationToken);
            pendingDuringStall = await PendingProbe.StaysPendingAsync(add);
            preconditionRanDuringStall = preconditionRan.Task.IsCompleted;
            framesDuringStall = journal.ReadStampedPuts(cancellationToken);
            journal.Writer.Flush.Release();
            first = await put.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            second = await add.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        }
        finally
        {
            journal.Writer.ReleaseAll();
        }

        await journal.ShutdownAsync();

        _ = await Assert.That(pendingDuringStall).IsTrue();
        _ = await Assert.That(preconditionRanDuringStall).IsFalse();
        _ = await Assert.That(framesDuringStall).IsEqualTo(StallableJournal.Describe([$"{KeyA}#{OperationId}", KeyW]));
        _ = await Assert.That(first.Added).IsTrue();
        _ = await Assert.That(second.Added).IsFalse();
        _ = await Assert.That(journal.ReadStampedPuts(cancellationToken)).IsEqualTo(StallableJournal.Describe([$"{KeyA}#{OperationId}", KeyW]));
    }

    /// <summary>A replicated apply, whose durable source is the group log, does not wait for the cache journal flush.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ReplicatedApplyDoesNotWaitForFlush(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await CreateWarmJournalAsync(groupCommit, cancellationToken);
        var target = new Target(journal, CreateStore());
        journal.Writer.Flush.Arm();
        int applied;
        bool flushEntered;
        try
        {
            applied = await ReplicatedPutAsync(target, "a", cancellationToken).WaitAsync(ShouldCompleteTimeout, TimeProvider.System, cancellationToken);
            flushEntered = journal.Writer.Flush.Entered.IsCompleted;
        }
        finally
        {
            journal.Writer.ReleaseAll();
        }

        _ = await Assert.That(applied).IsEqualTo(1);
        _ = await Assert.That(flushEntered).IsFalse();
        _ = await Assert.That(target.Memory.Snapshot).IsEqualTo(KeyA);
    }

    /// <summary>A snapshot cut waits for a write stalled in its flush and then captures it, with a watermark that covers its frame.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task CutDuringStalledApplyCoversWrite(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await CreateWarmJournalAsync(groupCommit, cancellationToken);
        var target = new Target(journal, CreateStore());
        journal.Writer.Flush.Arm();
        var captureRan = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        (ulong Watermark, string Memory) captured;
        TryAddAsyncResponse response;
        bool capturedDuringStall;
        try
        {
            var put = target.PutAsync(OperationId, "a", cancellationToken);
            await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            var cut = journal.Journal.ExecuteSnapshotCutAsync(
                (target.Memory, captureRan),
                static (s, watermark, _) => CaptureAsync(s.Memory, s.captureRan, watermark),
                static (_, _, barrier, _) => new ValueTask<(ulong Watermark, string Memory)>(barrier),
                cancellationToken).AsTask();
            capturedDuringStall = !await PendingProbe.StaysPendingAsync(captureRan.Task);
            journal.Writer.Flush.Release();
            response = await put.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            captured = await cut.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        }
        finally
        {
            journal.Writer.ReleaseAll();
        }

        await journal.ShutdownAsync();
        var sequences = journal.ReadPutSequences(cancellationToken);

        _ = await Assert.That(capturedDuringStall).IsFalse();
        _ = await Assert.That(response.Added).IsTrue();
        _ = await Assert.That(captured.Memory).IsEqualTo(KeyA);
        _ = await Assert.That(captured.Watermark).IsGreaterThanOrEqualTo(sequences[KeyA]);
    }

    /// <summary>
    /// A write parked in the durability wait that precedes its apply when the journal stops gracefully is made durable by the final seal and
    /// applied, but its outcome append is refused: the caller gets the unknown outcome and the started intent is retained.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GracefulStopDuringPreApplyWait(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, UnreachedBatchDeadline, 64, cancellationToken);
        var target = new Target(journal, CreateStore());
        var put = target.PutAsync(OperationId, "a", cancellationToken);

        // The idle journal thread waits without a timeout until a waiter registers in the batch, so this pins the parked path.
        await journal.Journal.WaitUntilAsync(static j => j.GroupCommit!.GetJournalThreadWaitTimeoutMs() != Timeout.Infinite, StallTimeout, cancellationToken);
        await journal.ShutdownAsync();

        var originalError = await NodeAsyncAssert.ThrowsAsync<RpcException>(put.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        var retryError = await NodeAsyncAssert.ThrowsAsync<RpcException>(target.PutAsync(OperationId, "a", cancellationToken).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

        _ = await Assert.That(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(originalError.Status.Detail)).IsTrue();
        _ = await Assert.That(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(retryError.Status.Detail)).IsTrue();
        _ = await Assert.That(target.Memory.Snapshot).IsEqualTo(KeyA);
        _ = await Assert.That(journal.ReadStampedPuts(cancellationToken)).IsEqualTo($"{KeyA}#{OperationId}");
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        base.DisposeManaged();
        _testMeter.Dispose();
    }

    private static ValueTask<(ulong Watermark, string Memory)> CaptureAsync(AppliedKeys memory, TaskCompletionSource captureRan, ulong watermark)
    {
        _ = captureRan.TrySetResult();
        return new ValueTask<(ulong Watermark, string Memory)>((watermark, memory.Snapshot));
    }

    private static async Task<int> ReplicatedPutAsync(Target target, string key, CancellationToken cancellationToken)
    {
        var scope = new object();
        RpcMutationIdempotencyExecutionAmbient.Activate(scope, OperationId, Fingerprint);
        try
        {
            using var suspended = RpcMutationIdempotencyExecutionAmbient.SuspendStamping();
            return await target.Memory.PutAsync(target.Executor, target.Journal.Journal, key, cancellationToken);
        }
        finally
        {
            RpcMutationIdempotencyExecutionAmbient.Deactivate(scope);
        }
    }

    private RpcMutationIdempotencyStore CreateStore() => new(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));

    /// <summary>Creates a journal whose segment header and a first frame are already durable, so later stalls catch only the frames under test.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The started journal.</returns>
    private async Task<StallableJournal> CreateWarmJournalAsync(bool groupCommit, CancellationToken cancellationToken)
    {
        var journal = await StallableJournal.CreateAsync(Dir, groupCommit, cancellationToken);
        try
        {
            await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("w"), JournalEntryPayloadKit.EncodePut("w"), cancellationToken);
            await journal.Journal.AwaitDurabilityCommitAsync(cancellationToken);
            return journal;
        }
        catch
        {
            await journal.DisposeAsync();
            throw;
        }
    }

    /// <summary>Idempotent puts through the coordinator, the durable executor and a real (stallable) journal, over an in-memory key model.</summary>
    [ThreadSafe]
    private sealed class Target
    {
        private readonly RpcMutationIdempotencyCoordinator _coordinator;

        internal Target(StallableJournal journal, RpcMutationIdempotencyStore store)
        {
            var log = new EventRecordingLogger();
            Journal = journal;
            Executor = new DurableMutationExecutor(journal.Journal, log);
            _coordinator = new RpcMutationIdempotencyCoordinator(store, journal.Journal, log);
        }

        /// <summary>Gets the executor the mutations run through.</summary>
        internal DurableMutationExecutor Executor { get; }

        /// <summary>Gets the journal the mutations write to.</summary>
        internal StallableJournal Journal { get; }

        /// <summary>Gets the keys the mutations applied to memory.</summary>
        internal AppliedKeys Memory { get; } = new();

        /// <summary>Runs one idempotent add that skips when memory already holds any key.</summary>
        /// <param name="operationId">Idempotency operation id.</param>
        /// <param name="key">Default-namespace key to add.</param>
        /// <param name="preconditionRan">Completed when the precondition runs.</param>
        /// <param name="cancellationToken">Caller cancellation token.</param>
        /// <returns>The RPC response; <see cref="TryAddAsyncResponse.Added" /> is <see langword="false" /> when the key was already applied.</returns>
        internal Task<TryAddAsyncResponse> AddAsync(string operationId, string key, TaskCompletionSource preconditionRan, CancellationToken cancellationToken) =>
            _coordinator.ExecuteAsync(
                operationId,
                Fingerprint,
                (Target: this, Key: key, Ran: preconditionRan),
                static async (s, ct) =>
                {
                    var applied = await s.Target.AddIfMemoryEmptyAsync(s.Key, s.Ran, ct).ConfigureAwait(false);
                    return new TryAddAsyncResponse { Added = applied == 1 };
                },
                cancellationToken);

        /// <summary>Runs one idempotent put of <paramref name="key" /> through the coordinator.</summary>
        /// <param name="operationId">Idempotency operation id.</param>
        /// <param name="key">Default-namespace key to put.</param>
        /// <param name="cancellationToken">Caller cancellation token.</param>
        /// <returns>The RPC response.</returns>
        internal Task<TryAddAsyncResponse> PutAsync(string operationId, string key, CancellationToken cancellationToken) =>
            _coordinator.ExecuteAsync(
                operationId,
                Fingerprint,
                (Target: this, Key: key),
                static async (s, ct) =>
                {
                    var applied = await s.Target.Memory.PutAsync(s.Target.Executor, s.Target.Journal.Journal, s.Key, ct).ConfigureAwait(false);
                    return new TryAddAsyncResponse { Added = applied == 1 };
                },
                cancellationToken);

        /// <summary>Runs one unstamped put of <paramref name="key" /> whose precondition signals <paramref name="preconditionRan" />; it applies nothing to <see cref="Memory" />.</summary>
        /// <param name="key">Default-namespace key to put.</param>
        /// <param name="preconditionRan">Completed when the precondition runs.</param>
        /// <param name="cancellationToken">Caller cancellation token.</param>
        /// <returns>The mutation task.</returns>
        internal Task<int> PutObservingPreconditionAsync(string key, TaskCompletionSource preconditionRan, CancellationToken cancellationToken)
        {
            var cacheKey = CacheKey.Default(key);
            return Executor.ExecuteAsync(
                cacheKey,
                static (s, _) => SignalPreconditionRanAsync(s.Ran),
                new DurableMutationPipeline<(IJournalCoordinator Journal, CacheKey Key, byte[] Payload, TaskCompletionSource Ran), int>(
                    (Journal.Journal, cacheKey, JournalEntryPayloadKit.EncodePut(key), preconditionRan),
                    static (s, ownership, ct) => s.Journal.AppendPutAsync(ownership, s.Key, s.Payload, ct),
                    static (_, _) => ValueTask.FromResult(1)),
                cancellationToken).AsTask();
        }

        private static ValueTask<DurableMutationCondition<int>> SignalPreconditionRanAsync(TaskCompletionSource preconditionRan)
        {
            _ = preconditionRan.TrySetResult();
            return new ValueTask<DurableMutationCondition<int>>(DurableMutationCondition<int>.Apply());
        }

        private static ValueTask<DurableMutationCondition<int>> SkipWhenMemoryHoldsKeyAsync(TaskCompletionSource preconditionRan, AppliedKeys memory)
        {
            _ = preconditionRan.TrySetResult();
            return new ValueTask<DurableMutationCondition<int>>(memory.Snapshot.Length != 0 ? DurableMutationCondition<int>.Skip(0) : DurableMutationCondition<int>.Apply());
        }

        private Task<int> AddIfMemoryEmptyAsync(string key, TaskCompletionSource preconditionRan, CancellationToken cancellationToken)
        {
            var cacheKey = CacheKey.Default(key);
            return Executor.ExecuteAsync(
                cacheKey,
                static (s, _) => SkipWhenMemoryHoldsKeyAsync(s.Ran, s.Memory),
                new DurableMutationPipeline<(IJournalCoordinator Journal, CacheKey Key, byte[] Payload, TaskCompletionSource Ran, AppliedKeys Memory), int>(
                    (Journal.Journal, cacheKey, JournalEntryPayloadKit.EncodePut(key), preconditionRan, Memory),
                    static (s, ownership, ct) => s.Journal.AppendPutAsync(ownership, s.Key, s.Payload, ct),
                    static (_, _) => ValueTask.FromResult(1)),
                cancellationToken).AsTask();
        }
    }
}
