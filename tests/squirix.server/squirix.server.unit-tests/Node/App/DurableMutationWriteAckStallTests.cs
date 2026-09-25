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

/// <summary>
/// Journal disposal over a segment write that never returns, in group commit mode: the append waits for the journal thread's write ack
/// after its frame entered the ring, and shutdown faults that wait after the join budget.
/// </summary>
[Immutable]
public sealed class DurableMutationWriteAckStallTests : IsolatedStorageTestBase
{
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
}
