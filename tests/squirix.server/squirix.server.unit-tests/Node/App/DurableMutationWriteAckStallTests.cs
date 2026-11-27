using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Logging;
using Squirix.Server.Node.App;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Logging;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Binary;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.App;

/// <summary>
/// Journal disposal over a segment write that never returns, in group commit mode: the append waits for the journal thread's write ack
/// after its frame entered the ring, and shutdown faults that wait after the join budget.
/// </summary>
[Immutable]
public sealed class DurableMutationWriteAckStallTests : IsolatedStorageTestBase
{
    private const int CommitUnknownEventId = 1015;

    private static readonly TimeSpan ShutdownBudget = TimeSpan.FromMilliseconds(250);

    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    private static readonly string KeyA = CacheKey.Default("a").ToString();

    private static readonly string KeyW = CacheKey.Default("w").ToString();

    /// <summary>Independent frames queued behind a stalled writer are combined into fewer file writes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QueuedFramesShareSegmentWrites(CancellationToken cancellationToken)
    {
        const int count = 32;
        await using var journal = await CreateWarmJournalAsync(cancellationToken);
        var executor = new DurableMutationExecutor(journal.Journal, NullLogger<DurableMutationExecutor>.Instance);
        var memory = new AppliedKeys();
        var requests = new Task<int>[count];
        var keys = new string[count];
        var expectedSequence = journal.Journal.NextSequence + count;
        var writesBefore = journal.Writer.WriteCount;
        journal.Writer.Write.Arm();
        try
        {
            for (var i = 0; i < count; i++)
            {
                var key = $"queued-{i}";
                keys[i] = CacheKey.Default(key).ToString();
                requests[i] = memory.PutAsync(executor, journal.Journal, key, cancellationToken);
                if (i == 0)
                    await journal.Writer.Write.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            }

            await journal.Journal.WaitUntilAsync(j => j.NextSequence == expectedSequence, StallTimeout, TimeProvider.System, cancellationToken);
            using var bound = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            bound.CancelAfter(StallTimeout);
            using var gate = await journal.Journal.MutationGate.LockAsync(bound.Token);
            _ = await Assert.That(memory.Snapshot).IsEmpty();
        }
        finally
        {
            journal.Writer.Write.Release();
        }

        _ = await Task.WhenAll(requests).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var writeCount = journal.Writer.WriteCount - writesBefore;
        _ = await Assert.That(writeCount).IsGreaterThan(0);
        _ = await Assert.That(writeCount).IsLessThan(count / 2);
        _ = await Assert.That(memory.Snapshot).IsEqualTo(StallableJournal.Describe(keys));
        await journal.ShutdownAsync();
        _ = await Assert.That(journal.Recover(string.Empty, 0, cancellationToken)).IsEqualTo(StallableJournal.Describe([.. keys, KeyW]));
    }

    /// <summary>Another key enters the journal during a stalled write without either mutation applying before durability.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OtherKeyQueuesDuringStalledWrite(CancellationToken cancellationToken)
    {
        await using var journal = await CreateWarmJournalAsync(cancellationToken);
        var executor = new DurableMutationExecutor(journal.Journal, NullLogger<DurableMutationExecutor>.Instance);
        var memory = new AppliedKeys();
        journal.Writer.Write.Arm();
        var first = memory.PutAsync(executor, journal.Journal, "a", cancellationToken);
        Task<int> second;
        try
        {
            await journal.Writer.Write.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            var before = journal.Journal.NextSequence;
            second = memory.PutAsync(executor, journal.Journal, "b", cancellationToken);
            await journal.Journal.WaitUntilAsync(j => j.NextSequence > before, StallTimeout, TimeProvider.System, cancellationToken);

            // Once the second sequence is allocated, taking the gate proves that its frame was published.
            using var bound = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            bound.CancelAfter(StallTimeout);
            using var gate = await journal.Journal.MutationGate.LockAsync(bound.Token);
            _ = await Assert.That(first.IsCompleted || second.IsCompleted).IsFalse();
            _ = await Assert.That(memory.Snapshot).IsEmpty();
        }
        finally
        {
            journal.Writer.Write.Release();
        }

        _ = await Task.WhenAll(first, second).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        await journal.ShutdownAsync();
        var keyB = CacheKey.Default("b").ToString();
        _ = await Assert.That(memory.Snapshot).IsEqualTo(StallableJournal.Describe([KeyA, keyB]));
        _ = await Assert.That(journal.Recover(string.Empty, 0, cancellationToken)).IsEqualTo(StallableJournal.Describe([KeyA, keyB, KeyW]));
    }

    /// <summary>
    /// A frame the journal thread dequeued but could not write yet has its caller faulted commit-unknown by the stop, but the frame stays
    /// accepted: once the write is released it is written and replayed, so an acknowledged frame is never silently lost.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StopKeepsFrameStalledBeforeWrite(CancellationToken cancellationToken)
    {
        await using var journal = await CreateWarmJournalAsync(cancellationToken);
        var memory = new AppliedKeys();
        journal.Writer.Write.Arm();
        var put = memory.PutAsync(new DurableMutationExecutor(journal.Journal, NullLogger<DurableMutationExecutor>.Instance), journal.Journal, "a", cancellationToken);
        await journal.Writer.Write.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        await DisposeOverStuckWriteAsync(journal, put, cancellationToken);
        var replayed = journal.Recover(string.Empty, 0, cancellationToken);

        _ = await Assert.That(memory.Snapshot).IsEmpty();
        _ = await Assert.That(replayed).IsEqualTo(StallableJournal.Describe([KeyA, KeyW]));
    }

    /// <summary>
    /// A frame whose write reached the file before the disk hung is on disk when disposal faults its caller, so a crash at that point
    /// replays it, and the released write keeps it.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WrittenFrameOutlivesWriteAckFault(CancellationToken cancellationToken)
    {
        await using var journal = await CreateWarmJournalAsync(cancellationToken);
        var memory = new AppliedKeys();
        journal.Writer.AfterWrite.Arm();
        var put = memory.PutAsync(new DurableMutationExecutor(journal.Journal, NullLogger<DurableMutationExecutor>.Instance), journal.Journal, "a", cancellationToken);
        await journal.Writer.AfterWrite.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        string crashImage;
        try
        {
            _ = await NodeAsyncAssert.ThrowsAsync<TimeoutException>(journal.StopStalledAsync(), StallTimeout, TimeProvider.System, cancellationToken);
            var error = await NodeAsyncAssert.ThrowsAsync<SquirixException>(put, StallTimeout, TimeProvider.System, cancellationToken);
            _ = await Assert.That(error.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
            crashImage = journal.ReadStampedPuts(cancellationToken);
        }
        finally
        {
            await journal.ReclaimLeakedAsync(StallTimeout);
        }

        var replayed = journal.Recover(string.Empty, 0, cancellationToken);

        _ = await Assert.That(crashImage).IsEqualTo(StallableJournal.Describe([KeyA, KeyW]));
        _ = await Assert.That(replayed).IsEqualTo(StallableJournal.Describe([KeyA, KeyW]));
        _ = await Assert.That(memory.Snapshot).IsEmpty();
    }

    /// <summary>
    /// Disposal that faults the write ack of a frame already on the ring gives the caller the stable commit-unknown contract (gRPC
    /// Unavailable with COMMIT_OUTCOME_UNKNOWN) with the shutdown fault logged as the cause, and leaves no in-flight apply behind.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WriteAckFaultMapsToUnknown(CancellationToken cancellationToken)
    {
        var log = new EventRecordingLogger();
        await using var journal = await CreateWarmJournalAsync(cancellationToken);
        var memory = new AppliedKeys();
        journal.Writer.Write.Arm();
        var put = memory.PutAsync(new DurableMutationExecutor(journal.Journal, log), journal.Journal, "a", cancellationToken);
        await journal.Writer.Write.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        SquirixException error;
        try
        {
            _ = await NodeAsyncAssert.ThrowsAsync<TimeoutException>(journal.StopStalledAsync(), StallTimeout, TimeProvider.System, cancellationToken);
            error = await NodeAsyncAssert.ThrowsAsync<SquirixException>(put, StallTimeout, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await journal.ReclaimLeakedAsync(StallTimeout);
        }

        var transport = error.ToServerCallException();
        var logged = log.Find(CommitUnknownEventId);

        _ = await Assert.That(error.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        _ = await Assert.That(transport.StatusCode).IsEqualTo(FrameStatus.Unavailable);
        _ = await Assert.That(transport.Message).IsEqualTo(ServerOpContract.CommitOutcomeUnknownDetail);
        _ = await Assert.That(logged?.Level).IsEqualTo(ServerLogLevel.Warning);
        _ = await Assert.That(logged?.Cause).IsTypeOf<ObjectDisposedException>();
        _ = await Assert.That(journal.Journal.InFlightApplyGate.HasPending).IsFalse();
        _ = await Assert.That(memory.Snapshot).IsEmpty();
    }

    /// <summary>
    /// A write ack fault after the enqueue releases the key lock and the in-flight apply slot exactly once, so the next mutation of the
    /// same key runs and is applied instead of waiting forever.
    /// </summary>
    /// <param name="groupCommit">Whether the journal runs in group commit mode instead of flushing per write.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task PostEnqueueFaultReleasesKey(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = new PostEnqueueFaultJournal(groupCommit);
        var memory = new AppliedKeys();
        var executor = new DurableMutationExecutor(journal, NullLogger<DurableMutationExecutor>.Instance);

        var error = await NodeAsyncAssert.ThrowsAsync<SquirixException>(memory.PutAsync(executor, journal, "a", cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        var applied = await memory.PutAsync(executor, journal, "a", cancellationToken);

        _ = await Assert.That(error.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        _ = await Assert.That(applied).IsEqualTo(1);
        _ = await Assert.That(memory.Snapshot).IsEqualTo(KeyA);
        _ = await Assert.That(journal.InFlightApplyGate.HasPending).IsFalse();
    }

    /// <summary>
    /// A failure latched before the frame is enqueued leaves nothing that may become durable: the caller keeps the definite failure, it is
    /// not reported as commit-unknown, and no in-flight apply is left behind.
    /// </summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task PreEnqueueFaultStaysDefinite(bool groupCommit, CancellationToken cancellationToken)
    {
        var log = new EventRecordingLogger();
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, cancellationToken);
        var reason = new IOException("journal device lost");

        // The latch lands under the mutation gate, after admission and before the append reaches the ring.
        var put = new DurableMutationExecutor(journal.Journal, log).ExecuteAsync(
            CacheKey.Default("a"),
            static (s, _) =>
            {
                s.Journal.FailJournalPipeline(s.Reason);
                return new ValueTask<DurableMutationCondition<int>>(DurableMutationCondition<int>.Apply());
            },
            new DurableMutationPipeline<(IJournalCoordinator Journal, IOException Reason), int>(
                (journal.Journal, reason),
                static (s, ownership, ct) => s.Journal.AppendPutAsync(ownership, CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), ct),
                static (_, _) => ValueTask.FromResult(1)),
            cancellationToken).AsTask();
        var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(put, StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(error).IsTypeOf<InvalidOperationException>().Because(error.ToString());
        _ = await Assert.That(error.InnerException).IsSameReferenceAs(reason);
        _ = await Assert.That(log.Find(CommitUnknownEventId)).IsNull();
        _ = await Assert.That(journal.Journal.InFlightApplyGate.HasPending).IsFalse();
    }

    /// <summary>Disposes over the stuck write, then releases it and joins the leaked journal thread so the data directory can be replayed.</summary>
    /// <param name="journal">Journal whose segment write is stuck.</param>
    /// <param name="put">Mutation stuck in its write ack wait.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    private static async Task DisposeOverStuckWriteAsync(StallableJournal journal, Task<int> put, CancellationToken cancellationToken)
    {
        try
        {
            _ = await NodeAsyncAssert.ThrowsAsync<TimeoutException>(journal.StopStalledAsync(), StallTimeout, TimeProvider.System, cancellationToken);
            _ = await NodeAsyncAssert.ThrowsAsync<SquirixException>(put, StallTimeout, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await journal.ReclaimLeakedAsync(StallTimeout);
        }
    }

    /// <summary>Creates a group commit journal with a short shutdown budget whose segment header and a first frame are already written.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The started journal.</returns>
    private async Task<StallableJournal> CreateWarmJournalAsync(CancellationToken cancellationToken)
    {
        var journal = await StallableJournal.CreateAsync(Dir, true, ShutdownBudget, NullLogger.Instance, cancellationToken);
        try
        {
            // The first write also writes the segment header: warm up so an armed write stall catches the frame under test.
            await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("w"), JournalEntryPayloadKit.EncodePut("w"), cancellationToken);
            return journal;
        }
        catch
        {
            await journal.DisposeAsync();
            throw;
        }
    }

    /// <summary>Journal double whose first put faults as a write ack fault after the enqueue; later puts succeed.</summary>
    [ThreadSafe]
    private sealed class PostEnqueueFaultJournal : IJournalCoordinator
    {
        private int _faultNextPut = 1;

        internal PostEnqueueFaultJournal(bool groupCommit)
        {
            IsJournalGroupCommitEnabled = groupCommit;
        }

        public event EventHandler? OnAppended
        {
            add => _ = value;
            remove => _ = value;
        }

        public long AppendedBytes => 0;

        public long AppendedOps => 0;

        public bool AtHighWater => false;

        public int CurrentSegmentIndex => 0;

        public long HighWaterBytes => 0;

        public QuiescenceGate InFlightApplyGate { get; } = new();

        public bool IsJournalGroupCommitEnabled { get; }

        public long MaxBytes => 0;

        public ulong NextSequence => 0;

        public double RecentAppendLatencyMs => 0;

        public long UsedBytes => 0;

        public ValueTask AppendPutAsync(JournalAppendContext ownership, CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken) =>
            Interlocked.Exchange(ref _faultNextPut, 0) == 1 ? WriteAckFaultAsync() : ValueTask.CompletedTask;

        public ValueTask AppendRemoveAsync(JournalAppendContext ownership, CacheKey key, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask AwaitDurabilityCommitAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public ValueTask<TResult> ExecuteSnapshotCutAsync<TState, TBarrier, TResult>(
            TState state,
            Func<TState, ulong, CancellationToken, ValueTask<TBarrier>> captureUnderBarrier,
            Func<TState, ulong, TBarrier, CancellationToken, ValueTask<TResult>> buildOutsideBarrier,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TResult>(
            Func<AsyncLockOwnership, CancellationToken, ValueTask<TResult>> action,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TState, TResult>(
            TState state,
            Func<TState, AsyncLockOwnership, CancellationToken, ValueTask<TResult>> action,
            CancellationToken cancellationToken) => action(state, default, cancellationToken);

        public ValueTask ExecuteUnderSnapshotBarrierAsync<TState>(
            TState state,
            Func<TState, AsyncLockOwnership, CancellationToken, ValueTask> action,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        public void FailJournalPipeline(Exception reason) => throw new NotSupportedException();

        public Exception? GetJournalThreadFailure() => null;

        public ValueTask WaitForStartupAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        private static ValueTask WriteAckFaultAsync() => ValueTask.FromException(
            new JournalPostEnqueueFaultException(
                JournalPostEnqueueFaultException.WriteAckFaultedMessage,
                new ObjectDisposedException(nameof(PostEnqueueFaultJournal))));
    }
}
