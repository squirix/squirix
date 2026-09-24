using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.App;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.App;

/// <summary>Durable mutations under a stalled journal fsync: memory must match the WAL.</summary>
[Immutable]
public sealed class DurableMutationStallTests : IsolatedStorageTestBase
{
    private static readonly TimeSpan CancelObservationWindow = TimeSpan.FromMilliseconds(500);

    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    private static readonly TimeSpan UnreachedBatchDeadline = TimeSpan.FromMinutes(10);

    private static readonly string KeyA = CacheKey.Default("a").ToString();

    private static readonly string KeysAb = $"{KeyA},{CacheKey.Default("b")}";

    /// <summary>A memory apply that fails after its frame entered the ring latches the journal pipeline and surfaces the original error.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ApplyErrorAfterRingEntryLatchesPipeline(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, cancellationToken);
        var executor = new DurableMutationExecutor(journal.Journal);
        var failure = new InvalidOperationException("memory apply failed");
        var memory = new AppliedKeys();

        var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(
            executor.ExecuteAsync(
                CacheKey.Default("a"),
                static (_, _) => new ValueTask<DurableMutationCondition<int>>(DurableMutationCondition<int>.Apply()),
                new DurableMutationPipeline<(IJournalCoordinator Journal, byte[] Payload, Exception Failure), int>(
                    (journal.Journal, JournalEntryPayloadKit.EncodePut("a"), failure),
                    static (s, ct) => s.Journal.AppendPutAsync(CacheKey.Default("a"), s.Payload, ct),
                    static (s, _) => ValueTask.FromException<int>(s.Failure)),
                cancellationToken).AsTask());
        var latched = journal.Journal.GetJournalThreadFailure();
        var next = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(memory.PutAsync(executor, journal.Journal, "b", cancellationToken));

        _ = await Assert.That(error).IsSameReferenceAs(failure);
        _ = await Assert.That(latched?.InnerException).IsSameReferenceAs(failure);
        _ = await Assert.That(next.InnerException).IsSameReferenceAs(latched);
        _ = await Assert.That(memory.Snapshot).IsEmpty();
    }

    /// <summary>A caller that cancels its durability wait after the append still leaves the mutation applied, exactly as the WAL replays it.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task CanceledDurabilityWaitStillAppliesMemory(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, cancellationToken);
        var memory = new AppliedKeys();

        _ = await CancelDuringStalledFlushAsync(journal, memory, cancellationToken);
        await journal.ShutdownAsync();
        var replayed = journal.Recover(string.Empty, 0, cancellationToken);

        _ = await Assert.That(replayed).IsEqualTo(KeyA);
        _ = await Assert.That(memory.Snapshot).IsEqualTo(replayed);
    }

    /// <summary>Journal disposal faults a durability wait the caller already canceled instead of leaving it parked.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeFaultsUncancellableWaitInBudget(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, UnreachedBatchDeadline, 64, cancellationToken);
        var memory = new AppliedKeys();
        using var caller = new CancellationTokenSource();

        var put = memory.PutAsync(new DurableMutationExecutor(journal.Journal), journal.Journal, "a", caller.Token);
        await CancelAfterAppendAsync(journal, caller, cancellationToken);

        // The batch deadline is never reached, so only a wait that honored the canceled caller would complete here.
        var completedAfterCancel = await Task.WhenAny(put, Task.Delay(CancelObservationWindow, TimeProvider.System, cancellationToken)) == put;
        await journal.ShutdownAsync();

        _ = await Assert.That(completedAfterCancel).IsFalse();
        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(put.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
    }

    /// <summary>A journal pipeline failure faults a durability wait the caller already canceled with the latched error.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PipelineFailureFaultsUncancellableWait(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, UnreachedBatchDeadline, 64, cancellationToken);
        var memory = new AppliedKeys();
        var reason = new IOException("journal device lost");
        using var caller = new CancellationTokenSource();

        var put = memory.PutAsync(new DurableMutationExecutor(journal.Journal), journal.Journal, "a", caller.Token);
        await CancelAfterAppendAsync(journal, caller, cancellationToken);

        // The batch deadline is never reached, so only a wait that honored the canceled caller would complete here.
        var completedAfterCancel = await Task.WhenAny(put, Task.Delay(CancelObservationWindow, TimeProvider.System, cancellationToken)) == put;
        journal.Journal.FailJournalPipeline(reason);
        var error = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(put.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

        _ = await Assert.That(completedAfterCancel).IsFalse();
        _ = await Assert.That(ReferenceEquals(error, reason) || ReferenceEquals(error.InnerException, reason)).IsTrue().Because(error.ToString());
        _ = await Assert.That(memory.Snapshot).IsEmpty();
    }

    /// <summary>After the frame entered the ring, a canceled caller keeps waiting for durability, then the apply runs on an uncanceled token.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task PostRingCancellationStillAppliesAndWaits(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, cancellationToken);
        var executor = new DurableMutationExecutor(journal.Journal);
        var applyObservedCancellation = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        journal.Writer.Flush.Arm();
        using var caller = new CancellationTokenSource();

        var put = executor.ExecuteAsync(
            CacheKey.Default("a"),
            static (_, _) => new ValueTask<DurableMutationCondition<int>>(DurableMutationCondition<int>.Apply()),
            new DurableMutationPipeline<(IJournalCoordinator Journal, byte[] Payload, TaskCompletionSource<bool> Applied), int>(
                (journal.Journal, JournalEntryPayloadKit.EncodePut("a"), applyObservedCancellation),
                static (s, ct) => s.Journal.AppendPutAsync(CacheKey.Default("a"), s.Payload, ct),
                static (s, ct) => ValueTask.FromResult(s.Applied.TrySetResult(ct.IsCancellationRequested) ? 1 : 0)),
            caller.Token).AsTask();
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        await caller.CancelAsync();
        var completedWhileStalled = await Task.WhenAny(put, Task.Delay(CancelObservationWindow, TimeProvider.System, cancellationToken)) == put;
        journal.Writer.Flush.Release();
        var result = await put.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(completedWhileStalled).IsFalse();
        _ = await Assert.That(result).IsEqualTo(1);
        _ = await Assert.That(await applyObservedCancellation.Task).IsFalse();
    }

    /// <summary>A snapshot cut covering a frame whose durability wait was canceled recovers the same state as a full WAL replay and as memory.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SnapshotAfterCanceledWaitMatchesReplay(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var memory = new AppliedKeys();

        var executor = await CancelDuringStalledFlushAsync(journal, memory, cancellationToken);

        // A cut stops one sequence short of the newest frame, so only a later frame makes it cover the canceled one.
        _ = await memory.PutAsync(executor, journal.Journal, "b", cancellationToken);
        var (sequence, captured) = await journal.Journal.ExecuteSnapshotCutAsync(
            memory,
            static (m, _, _) => ValueTask.FromResult(m.Snapshot),
            static (_, cutSequence, snapshot, _) => ValueTask.FromResult((cutSequence, snapshot)),
            cancellationToken);
        await journal.ShutdownAsync();
        var replayed = journal.Recover(string.Empty, 0, cancellationToken);
        var restarted = journal.Recover(captured, sequence, cancellationToken);

        _ = await Assert.That(replayed).IsEqualTo(KeysAb);
        _ = await Assert.That(restarted).IsEqualTo(replayed);
        _ = await Assert.That(memory.Snapshot).IsEqualTo(replayed);
    }

    private static async Task<DurableMutationExecutor> CancelDuringStalledFlushAsync(StallableJournal journal, AppliedKeys memory, CancellationToken cancellationToken)
    {
        var executor = new DurableMutationExecutor(journal.Journal);
        journal.Writer.Flush.Arm();
        using var caller = new CancellationTokenSource();

        var put = memory.PutAsync(executor, journal.Journal, "a", caller.Token);
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        await caller.CancelAsync();

        // The flush is still blocked: a wait that honored the canceled caller would complete here.
        var completedWhileStalled = await Task.WhenAny(put, Task.Delay(CancelObservationWindow, TimeProvider.System, cancellationToken)) == put;
        _ = await Assert.That(completedWhileStalled).IsFalse();

        journal.Writer.Flush.Release();
        _ = await put.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        await journal.Journal.AwaitDurabilityCommitAsync(cancellationToken);
        return executor;
    }

    private static async Task CancelAfterAppendAsync(StallableJournal journal, CancellationTokenSource caller, CancellationToken cancellationToken)
    {
        await journal.Journal.WaitUntilAsync(static j => j.AppendedOps > 0, StallTimeout, cancellationToken);
        await caller.CancelAsync();
    }
}
