using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.App;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.App;

/// <summary>
/// A frame that would exceed the on-disk journal capacity is never written, so its caller must see the definite capacity failure and
/// memory must not apply it: a following durability flush succeeds and would otherwise report the dropped frame as durable.
/// </summary>
/// <remarks>
/// Append admission refuses such a frame before it enters the ring, in plain and group commit mode alike: a plain append returns
/// once its frame is on the ring and has no write ack that could carry a later rejection by the journal thread.
/// </remarks>
[Immutable]
public sealed class DurableMutationCapacityTests : IsolatedStorageTestBase
{
    private const int CapacityMb = 1;

    private static readonly string KeyA = CacheKey.Default("a").ToString();

    /// <summary>
    /// A put larger than the remaining journal capacity fails with the capacity error, leaves memory and the journal without it and no
    /// in-flight apply behind, and the next put that fits is applied and replayed.
    /// </summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task CapacityDropFailsCaller(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, CapacityMb, cancellationToken);
        var memory = new AppliedKeys();
        var executor = new DurableMutationExecutor(journal.Journal);

        _ = await NodeAsyncAssert.ThrowsAsync<JournalCapacityExceededException>(memory.PutAsync(executor, journal.Journal, "big", OversizedValue(), cancellationToken));
        var droppedMemory = memory.Snapshot;
        var pendingApply = journal.Journal.InFlightApplyGate.HasPending;
        _ = await memory.PutAsync(executor, journal.Journal, "a", cancellationToken);
        await journal.ShutdownAsync();
        var replayed = journal.Recover(string.Empty, 0, cancellationToken);

        _ = await Assert.That(droppedMemory).IsEmpty();
        _ = await Assert.That(pendingApply).IsFalse();
        _ = await Assert.That(memory.Snapshot).IsEqualTo(KeyA);
        _ = await Assert.That(replayed).IsEqualTo(KeyA);
    }

    /// <summary>
    /// A capacity rejection is definite: it releases the conflict key and the in-flight apply slot, so the next mutation of the same key is
    /// admitted and applied instead of failing as a key already in flight, and the caller sees the capacity error, not an unknown outcome.
    /// </summary>
    /// <param name="groupCommit">Whether the journal runs in group commit mode (keyed admission) instead of the monolithic path.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task CapacityRejectionReleasesKey(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, CapacityMb, cancellationToken);
        var memory = new AppliedKeys();
        var executor = new DurableMutationExecutor(journal.Journal);

        _ = await NodeAsyncAssert.ThrowsAsync<JournalCapacityExceededException>(memory.PutAsync(executor, journal.Journal, "a", OversizedValue(), cancellationToken));
        var droppedMemory = memory.Snapshot;
        var applied = await memory.PutAsync(executor, journal.Journal, "a", cancellationToken);
        var pendingApply = journal.Journal.InFlightApplyGate.HasPending;
        await journal.ShutdownAsync();

        _ = await Assert.That(droppedMemory).IsEmpty();
        _ = await Assert.That(applied).IsEqualTo(1);
        _ = await Assert.That(pendingApply).IsFalse();
        _ = await Assert.That(memory.Snapshot).IsEqualTo(KeyA);
        _ = await Assert.That(journal.Recover(string.Empty, 0, cancellationToken)).IsEqualTo(KeyA);
    }

    /// <summary>Gets a value whose put frame exceeds the whole journal capacity.</summary>
    /// <returns>The oversized value.</returns>
    private static string OversizedValue() => new('x', (CapacityMb * 1024 * 1024) + 1024);
}
