using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>
/// A snapshot cut captures its watermark under the mutation gate, so a frame whose sequence it covers must be on the ring before the cut's
/// checkpoint: the journal refuses an append whose caller does not hold the gate before it allocates a sequence or enqueues anything.
/// </summary>
[Immutable]
public sealed class JournalAppendGateTests : IsolatedStorageTestBase
{
    private static readonly CacheKey Key = CacheKey.Default("a");

    private static readonly TimeSpan HolderEnterTimeout = TimeSpan.FromSeconds(10);

    /// <summary>Every cache mutation appender refuses a caller outside the gate, and no sequence or frame is left behind.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task UngatedAppendIsRefused(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, cancellationToken);
        var sequence = journal.Journal.NextSequence;

        await AssertEveryAppendRefusedAsync(journal.Journal, default, cancellationToken);
        await journal.ShutdownAsync();

        _ = await Assert.That(journal.Journal.NextSequence).IsEqualTo(sequence);
        _ = await Assert.That(journal.Recover(string.Empty, 0, cancellationToken)).IsEmpty();
    }

    /// <summary>
    /// While one flow holds the gate, an append from another flow is refused, whether it presents the ownership of its own barrier that
    /// already ended or none at all: nothing reaches the ring and no sequence is allocated.
    /// </summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task AppendOutsideHolderFlowIsRefused(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, cancellationToken);
        var stale = await journal.Journal.ExecuteUnderSnapshotBarrierAsync(static (ownership, _) => ValueTask.FromResult(ownership), cancellationToken);
        var sequence = journal.Journal.NextSequence;
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var holder = journal.Journal.ExecuteUnderSnapshotBarrierAsync(
            (Entered: entered, Release: release),
            static async (s, _, ct) =>
            {
                s.Entered.SetResult();
                await s.Release.Task.WaitAsync(ct);
            },
            cancellationToken).AsTask();
        await entered.Task.WaitAsync(HolderEnterTimeout, TimeProvider.System, cancellationToken);

        await AssertEveryAppendRefusedAsync(journal.Journal, stale, cancellationToken);
        await AssertEveryAppendRefusedAsync(journal.Journal, default, cancellationToken);
        _ = await Assert.That(journal.Journal.NextSequence).IsEqualTo(sequence);

        _ = release.TrySetResult();
        await holder;
        await journal.ShutdownAsync();

        _ = await Assert.That(journal.Journal.NextSequence).IsEqualTo(sequence);
        _ = await Assert.That(journal.Recover(string.Empty, 0, cancellationToken)).IsEmpty();
    }

    /// <summary>An append made while the caller holds the gate is accepted and replayed.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task GatedAppendIsAccepted(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, cancellationToken);

        await journal.Journal.ExecuteUnderSnapshotBarrierAsync(
            journal.Journal,
            static (appender, ownership, ct) => appender.AppendPutAsync(ownership, Key, JournalEntryPayloadKit.EncodePut("a"), ct),
            cancellationToken);
        await journal.ShutdownAsync();

        _ = await Assert.That(journal.Recover(string.Empty, 0, cancellationToken)).IsEqualTo(Key.ToString());
    }

    /// <summary>Asserts that every cache mutation appender refuses <paramref name="ownership" />.</summary>
    /// <param name="journal">Journal to append to.</param>
    /// <param name="ownership">The gate ownership presented with each append.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The assertions.</returns>
    private static async Task AssertEveryAppendRefusedAsync(JournalCoordinator journal, AsyncLockOwnership ownership, CancellationToken cancellationToken)
    {
        var call = (Journal: journal, Ownership: ownership, Payload: JournalEntryPayloadKit.EncodePut("a"), Token: cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(RunAsync(call, static s => s.Journal.AppendPutAsync(s.Ownership, Key, s.Payload, s.Token)));
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(
            RunAsync(call, static s => s.Journal.AppendPutAndAwaitDurabilityAsync(s.Ownership, Key, s.Payload, s.Token)));
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(RunAsync(call, static s => s.Journal.AppendRemoveAsync(s.Ownership, Key, s.Token)));
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(RunAsync(call, static s => s.Journal.AppendRemoveExpirationAsync(s.Ownership, Key, s.Token)));
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(
            RunAsync(call, static s => s.Journal.AppendTouchExpirationAsync(s.Ownership, Key, DateTime.UtcNow, s.Token)));
    }

    /// <summary>Runs an append as a task that faults with the refusal, whether it is thrown synchronously or through the returned operation.</summary>
    /// <typeparam name="TState">Type of the append arguments.</typeparam>
    /// <param name="state">Arguments passed to <paramref name="append" />.</param>
    /// <param name="append">Append to run.</param>
    /// <returns>The append as a task.</returns>
    private static Task RunAsync<TState>(TState state, Func<TState, ValueTask> append)
    {
        ValueTask pending;
        try
        {
            pending = append(state);
        }
        catch (InvalidOperationException ex)
        {
            return Task.FromException(ex);
        }

        return pending.AsTask();
    }
}
