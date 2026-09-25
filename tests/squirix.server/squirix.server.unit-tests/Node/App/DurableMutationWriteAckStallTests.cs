using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.App;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
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

    /// <summary>
    /// A frame the journal thread dequeued but could not write yet is faulted by disposal, and the released write is dropped instead of
    /// replayed: a graceful shutdown never leaves it durable.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeDropsFrameStalledBeforeWrite(CancellationToken cancellationToken)
    {
        await using var journal = await CreateWarmJournalAsync(cancellationToken);
        var memory = new AppliedKeys();
        journal.Writer.Write.Arm();
        var put = memory.PutAsync(new DurableMutationExecutor(journal.Journal), journal.Journal, "a", cancellationToken);
        await journal.Writer.Write.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        await DisposeOverStuckWriteAsync(journal, put, cancellationToken);
        var replayed = journal.Recover(string.Empty, 0, cancellationToken);

        _ = await Assert.That(memory.Snapshot).IsEmpty();
        _ = await Assert.That(replayed).IsEqualTo(KeyW);
    }

    /// <summary>
    /// A frame whose write reached the file before the disk hung is on disk when disposal faults its caller, so a crash at that point
    /// replays it; only a released write is truncated again.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WrittenFrameOutlivesWriteAckFault(CancellationToken cancellationToken)
    {
        await using var journal = await CreateWarmJournalAsync(cancellationToken);
        var memory = new AppliedKeys();
        journal.Writer.AfterWrite.Arm();
        var put = memory.PutAsync(new DurableMutationExecutor(journal.Journal), journal.Journal, "a", cancellationToken);
        await journal.Writer.AfterWrite.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        string crashImage;
        try
        {
            _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(journal.DisposeStalledAsync().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(put.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            crashImage = journal.ReadStampedPuts(cancellationToken);
        }
        finally
        {
            await journal.ReclaimLeakedAsync(StallTimeout);
        }

        var replayed = journal.Recover(string.Empty, 0, cancellationToken);

        _ = await Assert.That(crashImage).IsEqualTo(StallableJournal.Describe([KeyA, KeyW]));
        _ = await Assert.That(replayed).IsEqualTo(KeyW);
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
        var put = memory.PutAsync(new DurableMutationExecutor(journal.Journal) { Log = log }, journal.Journal, "a", cancellationToken);
        await journal.Writer.Write.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        SquirixException error;
        try
        {
            _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(journal.DisposeStalledAsync().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            error = await NodeAsyncAssert.ThrowsAsync<SquirixException>(put.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        }
        finally
        {
            await journal.ReclaimLeakedAsync(StallTimeout);
        }

        var transport = error.ToRpcException();
        var logged = log.Find(CommitUnknownEventId);

        _ = await Assert.That(error.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        _ = await Assert.That(transport.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(transport.Status.Detail).IsEqualTo(ServerOpContract.CommitOutcomeUnknownDetail);
        _ = await Assert.That(logged?.Level).IsEqualTo(LogLevel.Warning);
        _ = await Assert.That(logged?.Cause).IsTypeOf<ObjectDisposedException>();
        _ = await Assert.That(journal.Journal.InFlightApplyGate.HasPending).IsFalse();
        _ = await Assert.That(memory.Snapshot).IsEmpty();
    }

    /// <summary>
    /// A write ack fault after the enqueue releases the conflict key and the in-flight apply slot exactly once, so the next mutation of the
    /// same key is admitted and applied instead of failing as a key already in flight.
    /// </summary>
    /// <param name="groupCommit">Whether the journal runs in group commit mode (keyed admission) instead of the monolithic path.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task PostEnqueueFaultReleasesKey(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = new PostEnqueueFaultJournal(groupCommit);
        var memory = new AppliedKeys();
        var executor = new DurableMutationExecutor(journal) { Log = NullLogger.Instance };

        var error = await NodeAsyncAssert.ThrowsAsync<SquirixException>(memory.PutAsync(executor, journal, "a", cancellationToken));
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
        var put = new DurableMutationExecutor(journal.Journal) { Log = log }.ExecuteAsync(
            CacheKey.Default("a"),
            static (s, _) =>
            {
                s.Journal.FailJournalPipeline(s.Reason);
                return new ValueTask<DurableMutationCondition<int>>(DurableMutationCondition<int>.Apply());
            },
            new DurableMutationPipeline<(IJournalCoordinator Journal, IOException Reason), int>(
                (journal.Journal, reason),
                static (s, ct) => s.Journal.AppendPutAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), ct),
                static (_, _) => ValueTask.FromResult(1)),
            cancellationToken).AsTask();
        var error = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(put.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

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
            _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(journal.DisposeStalledAsync().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(put.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
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
            await journal.Journal.AppendPutAsync(CacheKey.Default("w"), JournalEntryPayloadKit.EncodePut("w"), cancellationToken);
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

        public int CurrentSegmentIndex => 0;

        public bool HasFlushLoopFailure => false;

        public long HighWaterBytes => 0;

        public QuiescenceGate InFlightApplyGate { get; } = new();

        public bool IsJournalGroupCommitEnabled { get; }

        public long MaxBytes => 0;

        public ulong NextSequence => 0;

        public double RecentAppendLatencyMs => 0;

        public long UsedBytes => 0;

        public ValueTask AppendIdempotencyOutcomeAsync(string operationId, string fingerprint, byte[] responseBytes, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask AppendPutAndAwaitDurabilityAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask AppendPutAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken) =>
            Interlocked.Exchange(ref _faultNextPut, 0) == 1
                ? ValueTask.FromException(
                    new JournalPostEnqueueFaultException(JournalPostEnqueueFaultException.WriteAckFaultedMessage, new ObjectDisposedException(nameof(PostEnqueueFaultJournal))))
                : ValueTask.CompletedTask;

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
            CancellationToken cancellationToken) => action(state, cancellationToken);

        public ValueTask ExecuteUnderSnapshotBarrierAsync<TState>(TState state, Func<TState, CancellationToken, ValueTask> action, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public void FailJournalPipeline(Exception reason) => throw new NotSupportedException();

        public Exception? GetJournalThreadFailure() => null;

        public ValueTask WaitForStartupAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }
}
