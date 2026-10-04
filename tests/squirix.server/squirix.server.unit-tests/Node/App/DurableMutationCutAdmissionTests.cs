using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.App;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.App;

/// <summary>A snapshot cut stops admitting grouped writers while it drains the admitted ones, so sustained writes cannot starve it.</summary>
[Immutable]
public sealed class DurableMutationCutAdmissionTests : IsolatedStorageTestBase
{
    private static readonly string KeyA = CacheKey.Default("a").ToString();

    private static readonly string KeyB = CacheKey.Default("b").ToString();

    private static readonly TimeSpan ObservationWindow = TimeSpan.FromMilliseconds(100);

    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// A writer that arrives after the cut requested admission is refused until the cut captured: at capture the last allocated frame is
    /// the writer admitted before the cut, it is applied, and the late writer's frame lies above it.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CutRefusesLateWritersUntilCapture(CancellationToken cancellationToken)
    {
        await using var journal = await CreateWarmJournalAsync(cancellationToken);
        var memory = new AppliedKeys();
        var executor = new DurableMutationExecutor(journal.Journal, NullLogger<DurableMutationExecutor>.Instance);
        journal.Writer.Flush.Arm();
        bool lateWriterPending;
        (ulong Watermark, ulong LastAllocated, string AppliedAtCapture, bool Pending) cut;
        try
        {
            var first = memory.PutAsync(executor, journal.Journal, "a", cancellationToken);
            await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            var cutTask = journal.Journal.ExecuteSnapshotCutAsync(
                (Memory: memory, journal.Journal),
                static (s, sequence, _) => new ValueTask<(ulong, ulong, string, bool)>((sequence, s.Journal.NextSequence, s.Memory.Snapshot, s.Journal.InFlightApplyGate.HasPending)),
                static (_, _, barrier, _) => new ValueTask<(ulong Watermark, ulong LastAllocated, string AppliedAtCapture, bool Pending)>(barrier),
                cancellationToken).AsTask();
            var second = memory.PutAsync(executor, journal.Journal, "b", cancellationToken);
            lateWriterPending = await IsStillPendingAsync(second, cancellationToken);
            journal.Writer.Flush.Release();
            _ = await first.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            cut = await cutTask.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await second.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        }
        finally
        {
            journal.Writer.ReleaseAll();
        }

        await journal.ShutdownAsync();
        var sequences = journal.ReadPutSequences(cancellationToken);

        _ = await Assert.That(lateWriterPending).IsTrue();
        _ = await Assert.That(cut.Watermark).IsLessThanOrEqualTo(cut.LastAllocated);
        _ = await Assert.That(cut.LastAllocated).IsEqualTo(sequences[KeyA]);
        _ = await Assert.That(sequences[KeyB]).IsGreaterThan(cut.LastAllocated);
        _ = await Assert.That(cut.AppliedAtCapture).IsEqualTo(KeyA);
        _ = await Assert.That(cut.Pending).IsFalse();
        _ = await Assert.That(memory.Snapshot).IsEqualTo(StallableJournal.Describe([KeyA, KeyB]));
    }

    /// <summary>A cut cancelled while it drains reopens admission, so writers are admitted again.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancelledCutReopensAdmission(CancellationToken cancellationToken)
    {
        await using var journal = await CreateWarmJournalAsync(cancellationToken);
        var memory = new AppliedKeys();
        var executor = new DurableMutationExecutor(journal.Journal, NullLogger<DurableMutationExecutor>.Instance);
        using var cutCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        journal.Writer.Flush.Arm();
        bool admittedAfterCancel;
        try
        {
            var first = memory.PutAsync(executor, journal.Journal, "a", cancellationToken);
            await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            var cutTask = journal.Journal.ExecuteSnapshotCutAsync(
                0,
                static (_, sequence, _) => new ValueTask<ulong>(sequence),
                static (_, _, barrier, _) => new ValueTask<ulong>(barrier),
                cutCancellation.Token).AsTask();
            await cutCancellation.CancelAsync();
            _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(cutTask.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            admittedAfterCancel = journal.Journal.InFlightApplyGate.TryEnter();
            if (admittedAfterCancel)
                journal.Journal.InFlightApplyGate.Exit();

            journal.Writer.Flush.Release();
            _ = await first.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        }
        finally
        {
            journal.Writer.ReleaseAll();
        }

        _ = await Assert.That(admittedAfterCancel).IsTrue();
        _ = await Assert.That(journal.Journal.InFlightApplyGate.HasPending).IsFalse();
    }

    /// <summary>An append that fails with an unexpected exception before its frame is enqueued leaves no in-flight apply, so a later cut completes and writers are admitted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnexpectedAppendFailureReleasesAdmission(CancellationToken cancellationToken)
    {
        await using var journal = await CreateWarmJournalAsync(cancellationToken);
        var executor = new DurableMutationExecutor(journal.Journal, NullLogger<DurableMutationExecutor>.Instance);

        var failing = executor.ExecuteAsync(
            CacheKey.Default("a"),
            static (_, _) => new ValueTask<DurableMutationCondition<int>>(DurableMutationCondition<int>.Apply()),
            new DurableMutationPipeline<int, int>(
                0,
                static (_, _, _) => ValueTask.FromException(new NotSupportedException("encode failed")),
                static (_, _) => ValueTask.FromResult(1)),
            cancellationToken).AsTask();
        _ = await NodeAsyncAssert.ThrowsAsync<NotSupportedException>(failing.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

        var watermark = await journal.Journal.ExecuteSnapshotCutAsync(
            0,
            static (_, sequence, _) => new ValueTask<ulong>(sequence),
            static (_, _, barrier, _) => new ValueTask<ulong>(barrier),
            cancellationToken).AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var admitted = journal.Journal.InFlightApplyGate.TryEnter();
        if (admitted)
            journal.Journal.InFlightApplyGate.Exit();

        _ = await Assert.That(watermark).IsGreaterThan(0UL);
        _ = await Assert.That(admitted).IsTrue();
        _ = await Assert.That(journal.Journal.InFlightApplyGate.HasPending).IsFalse();
    }

    private static async Task<bool> IsStillPendingAsync(Task operation, CancellationToken cancellationToken)
    {
        try
        {
            await operation.WaitAsync(ObservationWindow, TimeProvider.System, cancellationToken);
            return false;
        }
        catch (TimeoutException)
        {
            return true;
        }
    }

    /// <summary>Creates a group commit journal whose segment header and a first frame are already durable.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The started journal.</returns>
    private async Task<StallableJournal> CreateWarmJournalAsync(CancellationToken cancellationToken)
    {
        var journal = await StallableJournal.CreateAsync(Dir, true, cancellationToken);
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
}
