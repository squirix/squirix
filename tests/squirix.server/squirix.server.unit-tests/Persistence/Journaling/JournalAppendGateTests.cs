using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.TestKit;
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
        var call = (journal.Journal, Payload: JournalEntryPayloadKit.EncodePut("a"), Token: cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(RunAsync(call, static s => s.Journal.AppendPutAsync(Key, s.Payload, s.Token)));
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(RunAsync(call, static s => s.Journal.AppendPutAndAwaitDurabilityAsync(Key, s.Payload, s.Token)));
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(RunAsync(call, static s => s.Journal.AppendRemoveAsync(Key, s.Token)));
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(RunAsync(call, static s => s.Journal.AppendRemoveExpirationAsync(Key, s.Token)));
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(RunAsync(call, static s => s.Journal.AppendTouchExpirationAsync(Key, DateTime.UtcNow, s.Token)));
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
            static (appender, ct) => appender.AppendPutAsync(Key, JournalEntryPayloadKit.EncodePut("a"), ct),
            cancellationToken);
        await journal.ShutdownAsync();

        _ = await Assert.That(journal.Recover(string.Empty, 0, cancellationToken)).IsEqualTo(Key.ToString());
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
