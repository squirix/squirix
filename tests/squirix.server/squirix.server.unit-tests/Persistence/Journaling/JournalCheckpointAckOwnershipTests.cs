using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>
/// A durability checkpoint completes only the completion source carried by its own work item. A source
/// that is already registered but whose checkpoint is still waiting to be enqueued must stay pending
/// when a later caller's checkpoint is processed; otherwise mutations would observe durability before
/// their frames reach the segment file.
/// </summary>
[Immutable]
public sealed class JournalCheckpointAckOwnershipTests : IsolatedStorageTestBase
{
    /// <summary>A foreign checkpoint flush completes only its own wait and leaves earlier registered waits pending.</summary>
    [Fact]
    public async Task ForeignFlushLeavesAckPending()
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 4,
            FlushIntervalMs = 600_000,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new Ledger(options);
        var state = await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken);
        await using var journal = JournalCoordinatorFactory.Create(options, state, manifestStore, new AsyncManualResetEvent(true));
        await journal.WaitForStartupAsync(DefaultCancellationToken);
        var coordinator = Assert.IsType<JournalCoordinator>(journal);

        var registered = DurabilityAckRegistry.NewWait();
        coordinator.DurabilityAcks.Add(registered);

        // A later caller registers and enqueues its own checkpoint; processing it must not touch
        // the wait registered above.
        await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);

        Assert.False(registered.Task.IsCompleted);

        // The foreign wait stays pending forever by design; detach it so dispose does not fail it.
        _ = coordinator.DurabilityAcks.Remove(registered);
    }
}
