using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.App;
using Squirix.Server.Runtime;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.App;

/// <summary>Same-key durable mutations run one after another with identical results whether journal group commit is on or off.</summary>
[Immutable]
public sealed class DurableMutationSameKeyOrderTests : IsolatedStorageTestBase
{
    private static readonly string KeyK = CacheKey.Default("k").ToString();

    private static readonly string KeyW = CacheKey.Default("w").ToString();

    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    /// <summary>A same-key mutation waits for the earlier one, including its durability wait, and its precondition then sees the earlier mutation applied.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task AddAfterPutWaitsAndIsSkipped(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await CreateWarmJournalAsync(groupCommit, cancellationToken);
        var memory = new AppliedKeys();
        var executor = new DurableMutationExecutor(journal.Journal, NullLogger<DurableMutationExecutor>.Instance);
        journal.Writer.Flush.Arm();
        int put;
        int added;
        bool pendingDuringStall;
        try
        {
            var first = memory.PutAsync(executor, journal.Journal, "k", cancellationToken);
            await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            var second = memory.AddIfAbsentAsync(executor, journal.Journal, "k", cancellationToken);
            pendingDuringStall = await PendingProbe.StaysPendingAsync(second);
            journal.Writer.Flush.Release();
            put = await first.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            added = await second.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        }
        finally
        {
            journal.Writer.ReleaseAll();
        }

        await journal.ShutdownAsync();

        _ = await Assert.That(pendingDuringStall).IsTrue();
        _ = await Assert.That(put).IsEqualTo(1);
        _ = await Assert.That(added).IsEqualTo(0);
        _ = await Assert.That(memory.Snapshot).IsEqualTo(KeyK);
        _ = await Assert.That(journal.ReadStampedPuts(cancellationToken)).IsEqualTo(StallableJournal.Describe([KeyK, KeyW]));
    }

    /// <summary>An add after a completed put is skipped in both modes.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task AddAfterCompletedPutIsSkipped(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await CreateWarmJournalAsync(groupCommit, cancellationToken);
        var memory = new AppliedKeys();
        var executor = new DurableMutationExecutor(journal.Journal, NullLogger<DurableMutationExecutor>.Instance);

        _ = await memory.PutAsync(executor, journal.Journal, "k", cancellationToken);
        var added = await memory.AddIfAbsentAsync(executor, journal.Journal, "k", cancellationToken);

        _ = await Assert.That(added).IsEqualTo(0);
    }

    /// <summary>A remove queued behind a put of the same key is journaled and applied after it, so the key ends absent and replays absent.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task RemoveAfterPutEndsAbsent(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await CreateWarmJournalAsync(groupCommit, cancellationToken);
        var memory = new AppliedKeys();
        var executor = new DurableMutationExecutor(journal.Journal, NullLogger<DurableMutationExecutor>.Instance);
        journal.Writer.Flush.Arm();
        try
        {
            var first = memory.PutAsync(executor, journal.Journal, "k", cancellationToken);
            await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            var second = memory.RemoveAsync(executor, journal.Journal, "k", cancellationToken);
            journal.Writer.Flush.Release();
            _ = await first.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await second.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        }
        finally
        {
            journal.Writer.ReleaseAll();
        }

        await journal.ShutdownAsync();
        var replayed = journal.Recover(string.Empty, 0, cancellationToken);

        _ = await Assert.That(memory.Snapshot).IsEmpty();
        _ = await Assert.That(replayed).IsEqualTo(KeyW);
        _ = await Assert.That(journal.ReadStampedPuts(cancellationToken)).IsEqualTo(StallableJournal.Describe([KeyK, KeyW]));
    }

    /// <summary>A same-key mutation cancelled while it waits for the earlier one appends nothing and leaves no in-flight apply.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task CancelledWaiterAppendsNothing(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await CreateWarmJournalAsync(groupCommit, cancellationToken);
        var memory = new AppliedKeys();
        var executor = new DurableMutationExecutor(journal.Journal, NullLogger<DurableMutationExecutor>.Instance);
        using var waiterCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        journal.Writer.Flush.Arm();
        try
        {
            var first = memory.PutAsync(executor, journal.Journal, "k", cancellationToken);
            await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            var second = memory.PutAsync(executor, journal.Journal, "k", "second", waiterCancellation.Token);
            await waiterCancellation.CancelAsync();
            _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(second.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            journal.Writer.Flush.Release();
            _ = await first.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        }
        finally
        {
            journal.Writer.ReleaseAll();
        }

        await journal.ShutdownAsync();

        _ = await Assert.That(journal.Journal.InFlightApplyGate.HasPending).IsFalse();
        _ = await Assert.That(journal.ReadStampedPuts(cancellationToken)).IsEqualTo(StallableJournal.Describe([KeyK, KeyW]));
        _ = await Assert.That(memory.Snapshot).IsEqualTo(KeyK);
    }

    /// <summary>
    /// Two idempotent calls with different operation ids on one key run in order: the second waits for the
    /// first one's frame and apply, and both frames carry their own operation id.
    /// </summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task IdempotentSameKeyCallsRunInOrder(bool groupCommit, CancellationToken cancellationToken)
    {
        // The stalled write keeps the first call in flight in both modes: grouped waits for the write ack, ungrouped for the flush behind it.
        await using var journal = await CreateWarmJournalAsync(groupCommit, cancellationToken);
        var memory = new AppliedKeys();
        var executor = new DurableMutationExecutor(journal.Journal, NullLogger<DurableMutationExecutor>.Instance);
        journal.Writer.Write.Arm();
        bool pendingDuringStall;
        try
        {
            var first = StartPutInScopeAsync(memory, executor, journal, "op-1", cancellationToken);
            await journal.Writer.Write.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            var second = StartPutInScopeAsync(memory, executor, journal, "op-2", cancellationToken);
            pendingDuringStall = await PendingProbe.StaysPendingAsync(second);
            journal.Writer.Write.Release();
            _ = await first.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await second.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        }
        finally
        {
            journal.Writer.ReleaseAll();
        }

        await journal.ShutdownAsync();

        _ = await Assert.That(pendingDuringStall).IsTrue();
        _ = await Assert.That(journal.ReadStampedPuts(cancellationToken)).IsEqualTo(StallableJournal.Describe([$"{KeyK}#op-1", $"{KeyK}#op-2", KeyW]));
        _ = await Assert.That(memory.Snapshot).IsEqualTo(KeyK);
        _ = await Assert.That(journal.Journal.InFlightApplyGate.HasPending).IsFalse();
    }

    private static Task<int> StartPutInScopeAsync(AppliedKeys memory, DurableMutationExecutor executor, StallableJournal journal, string operationId, CancellationToken cancellationToken)
    {
        // The executor is an async method, so it never throws synchronously and the scope is always deactivated.
        var scope = new object();
        RpcMutationIdempotencyExecutionAmbient.Activate(scope, operationId, "fingerprint");
        var put = memory.PutAsync(executor, journal.Journal, "k", cancellationToken);
        RpcMutationIdempotencyExecutionAmbient.Deactivate(scope);
        return put;
    }

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
}
