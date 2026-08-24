using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>
/// Regression coverage for the rearchitected <see cref="PooledAck" /> lifetime ownership model
/// (issue #400): once the journal ring accepts a work item, the journal owns its ack and is the single
/// pool point. The caller returns the ack only when the item was not accepted. These tests pin the
/// acceptance boundary, the no-leak / single-pool-point invariant, and the caller-vs-journal ownership
/// transfer using the coordinator's observable registry state (deterministic, no shared-pool identity).
/// </summary>
[Immutable]
public sealed class DurabilityAckOwnershipTests : IsolatedStorageTestBase
{
    /// <summary>
    /// <see cref="BoundedJournalRing.EnqueueAsync" /> must not admit an item when it throws
    /// <see cref="OperationCanceledException" />: a pre-canceled token leaves the ring with exactly the
    /// items already enqueued and no leaked or duplicate slot. The acceptance boundary is what lets callers
    /// keep ownership of an un-admitted item's ack.
    /// </summary>
    [Fact]
    public async Task RingCanceledEnqueueLeavesNoLeak()
    {
        const int capacity = 4;
        using var ring = new BoundedJournalRing(capacity);
        for (var i = 0; i < capacity; i++)
            await ring.EnqueueAsync(JournalWorkItem.Shutdown(), CancellationToken.None);

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(ring.EnqueueAsync(JournalWorkItem.Shutdown(), new CancellationToken(true)));

        var drained = 0;
        while (ring.TryDequeue(out _))
            drained++;

        Assert.Equal(capacity, drained);
    }

    /// <summary>
    /// A successful flush transfers ack ownership to the journal, which removes the ack from the registry and
    /// returns it to the pool after setting the result. After completion the registry holds no durable acks.
    /// </summary>
    [Fact]
    public async Task FlushSuccessLeavesRegistryEmpty()
    {
        var options = BuildOptions();
        using var manifestStore = new Ledger(options);
        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);
        await journal.WaitForStartupAsync(DefaultCancellationToken);
        var coordinator = Assert.IsType<JournalCoordinator>(journal);

        await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);

        Assert.Empty(coordinator.DurabilityAcks.TakeAll());
    }

    /// <summary>
    /// A flush whose enqueue is canceled before the ring accepts the item keeps caller ownership of the ack:
    /// the caller removes it from the registry and returns it to the pool. The coordinator stays usable for a
    /// subsequent, accepted flush (no orphaned ack poisoning the pipeline).
    /// </summary>
    [Fact]
    public async Task FlushCancelKeepsRegistryEmpty()
    {
        var options = BuildOptions();
        using var manifestStore = new Ledger(options);
        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);
        await journal.WaitForStartupAsync(DefaultCancellationToken);
        var coordinator = Assert.IsType<JournalCoordinator>(journal);

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(journal.AwaitDurabilityCommitAsync(cts.Token));

        Assert.Empty(coordinator.DurabilityAcks.TakeAll());

        await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);
        Assert.Empty(coordinator.DurabilityAcks.TakeAll());
    }

    /// <summary>
    /// Maintenance begin/end acks are journal-owned after acceptance; both are removed from the registry by the
    /// journal completion path. After a successful maintenance the registry is empty.
    /// </summary>
    [Fact]
    public async Task MaintenanceSuccessLeavesRegistryEmpty()
    {
        var options = BuildOptions();
        using var manifestStore = new Ledger(options);
        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);
        await journal.WaitForStartupAsync(DefaultCancellationToken);
        var coordinator = Assert.IsType<JournalCoordinator>(journal);

        await journal.ExecuteMaintenanceExclusiveAsync(static (_) => ValueTask.CompletedTask, DefaultCancellationToken);

        Assert.Empty(coordinator.DurabilityAcks.TakeAll());
    }

    /// <summary>
    /// An append-with-durability ack is journal-owned after acceptance; the journal removes it from the registry
    /// and returns it to the pool after the fsync. After a successful append the registry is empty.
    /// </summary>
    [Fact]
    public async Task AppendDurabilityLeavesRegistryEmpty()
    {
        var options = BuildOptions();
        using var manifestStore = new Ledger(options);
        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);
        await journal.WaitForStartupAsync(DefaultCancellationToken);
        var coordinator = Assert.IsType<JournalCoordinator>(journal);

        var payload = JournalEntryPayloadKit.EncodePut("v");
        await journal.AppendPutAndAwaitDurabilityAsync(CacheKey.Default("k"), payload, DefaultCancellationToken);

        Assert.Empty(coordinator.DurabilityAcks.TakeAll());
    }

    /// <summary>
    /// A terminal failure drains registered-but-not-yet-enqueued acks and faults each: the registered ack
    /// observed here completes with the failure reason (it was not leaked), proving the failure path is the
    /// single completion point for those acks.
    /// </summary>
    [Fact]
    public async Task TerminalFailureDrainsRegisteredAck()
    {
        var options = BuildOptions();
        using var manifestStore = new Ledger(options);
        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);
        await journal.WaitForStartupAsync(DefaultCancellationToken);
        var coordinator = Assert.IsType<JournalCoordinator>(journal);

        // A registered ack that was never enqueued (item still pending in the caller).
        var ack = PooledAck.Rent();
        coordinator.DurabilityAcks.Add(ack);

        // Start awaiting before disposal faults the ack; WaitAsync resets the core, so awaiting after the
        // fault would wipe the result and hang.
        var awaitTask = ack.WaitAsync(DefaultCancellationToken).AsTask();

        // ReSharper disable once DisposeOnUsingVariable
        await journal.DisposeAsync();

        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(awaitTask);
    }

    /// <summary>
    /// The journal completes and pools an ack before the caller's awaiter necessarily attaches. The core must
    /// not advance its version at the pool point, or a late <see cref="ValueTask" /> attach throws
    /// <see cref="InvalidOperationException" /> on the stale token. This pins the reviewer-found race: the
    /// caller still observes the completed result after the journal already returned the ack.
    /// </summary>
    [Fact]
    public async Task LateAwaitAfterJournalReturn()
    {
        var ack = PooledAck.Rent();
        var pending = ack.WaitAsync(CancellationToken.None);

        Assert.True(ack.TrySetResult());
        ack.Return();

        Assert.True(pending.IsCompletedSuccessfully);
        await pending;
    }

    private PersistenceOptions BuildOptions()
    {
        return new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 4,
            FlushIntervalMs = 600_000,
            ManifestRetentionCount = 1,
            JournalGroupCommitMaxWait = TimeSpan.Zero,
            JournalGroupCommitMaxBatch = 8,
        };
    }
}
