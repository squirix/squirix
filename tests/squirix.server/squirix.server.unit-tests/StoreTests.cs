using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Integration tests for the manifest store.</summary>
public sealed class StoreTests : IsolatedStorageTestBase
{
    /// <inheritdoc />
    protected override string TempDirectoryName => "manifest";

    /// <summary>Verifies sequential roll publishes advance the current manifest while a persistent handle stays open.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EnqueueRollAdvancesManifestSequentially(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions { DataDir = Dir };
        await RollAsync();
        using var reloaded = new Ledger(options, NullLogger<Ledger>.Instance);
        _ = await Assert.That((await reloaded.ReadCurrentOrDefaultAsync(cancellationToken)).CurrentJournal).IsEqualTo(2);
        return;

        static async ValueTask<bool> ConditionAsync(Ledger s, CancellationToken ct)
        {
            return (await s.ReadCurrentOrDefaultAsync(ct).ConfigureAwait(false)).CurrentJournal == 2;
        }

        async Task RollAsync()
        {
            using var store = new Ledger(options, NullLogger<Ledger>.Instance);
            Exception? rollError = null;
            store.EnqueueRoll(1, 1, static () => { }, ex => rollError = ex);
            store.EnqueueRoll(2, 2, static () => { }, ex => rollError = ex);

            await store.WaitUntilValueAsync(ConditionAsync, cancellationToken);
            rollError.ThrowIfFaulted();
            _ = await Assert.That((await store.ReadCurrentOrDefaultAsync(cancellationToken)).CurrentJournal).IsEqualTo(2);
        }
    }

    /// <summary>Verifies the first write creates the two-slot manifest file holding the written manifest.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WriteCreatesManifestFile(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions { DataDir = Dir };
        using var store = new Ledger(options, NullLogger<Ledger>.Instance);

        await store.WriteAsync(new State { CurrentJournal = 1, NextSequence = 1 }, cancellationToken);

        var currentPath = NodePathKit.Combine(Dir, "man-current");
        _ = await Assert.That(new FileInfo(currentPath).Length).IsEqualTo(SlotFormat.FileSize);
        var manifest = SlotFile.Read(currentPath).State;
        _ = await Assert.That(manifest.CurrentJournal).IsEqualTo(1);
        _ = await Assert.That(manifest.NextSequence).IsEqualTo(1UL);
    }

    /// <summary>Verifies CURRENT is written in place: a reader without delete sharing keeps it open across publishes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishSucceedsWhileReaderHoldsFile(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions { DataDir = Dir };
        using var store = new Ledger(options, NullLogger<Ledger>.Instance);
        await store.WriteAsync(new State { CurrentJournal = 1, NextSequence = 1 }, cancellationToken);

        var currentPath = NodePathKit.Combine(Dir, "man-current");
        using var reader = File.OpenHandle(currentPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        await store.WriteAsync(new State { CurrentJournal = 2, NextSequence = 2 }, cancellationToken);
        await store.WriteAsync(new State { CurrentJournal = 3, NextSequence = 3 }, cancellationToken);

        _ = await Assert.That(SlotFile.Read(currentPath).State.CurrentJournal).IsEqualTo(3);
    }

    /// <summary>Verifies that after a publish fails, every later publish is refused with the first failure and nothing reaches the manifest file.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedPublishRefusesLaterPublishes(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions { DataDir = Dir };
        using var store = new Ledger(options, NullLogger<Ledger>.Instance);
        var currentPath = NodePathKit.Combine(Dir, "man-current");
        Directory.CreateDirectory(currentPath);
        var first = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(store.WriteAsync(new State { CurrentJournal = 2, NextSequence = 2 }, cancellationToken));
        Directory.Delete(currentPath);

        var write = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(store.WriteAsync(new State { CurrentJournal = 3, NextSequence = 3 }, cancellationToken));
        var snapshot = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(
            store.WriteSnapshotAsync(new State { CurrentJournal = 3, NextSequence = 3, LastSnapshot = new SnapshotRef { Index = 1, CreatedUtc = DateTime.UtcNow } }, cancellationToken));
        var rollFailure = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        var rolled = false;
        store.EnqueueRoll(3, 3, () => rolled = true, ex => rollFailure.TrySetResult(ex));
        var roll = await rollFailure.Task.WaitAsync(cancellationToken);

        _ = await Assert.That(write.InnerException).IsSameReferenceAs(first);
        _ = await Assert.That(snapshot.InnerException).IsSameReferenceAs(first);
        _ = await Assert.That(roll).IsTypeOf<InvalidOperationException>();
        _ = await Assert.That(roll.InnerException).IsSameReferenceAs(first);
        _ = await Assert.That(rolled).IsFalse();
        _ = await Assert.That(File.Exists(currentPath)).IsFalse();
        _ = await Assert.That((await store.ReadCurrentOrDefaultAsync(cancellationToken)).CurrentJournal).IsEqualTo(1);
    }

    /// <summary>Verifies that a manifest the encoder rejects fails before any write and leaves the ledger usable.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RejectedManifestKeepsLedgerUsable(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions { DataDir = Dir };
        using var store = new Ledger(options, NullLogger<Ledger>.Instance);

        _ = await NodeAsyncAssert.ThrowsAnyAsync<ArgumentException>(store.WriteAsync(new State { CurrentJournal = 0, NextSequence = 1 }, cancellationToken));
        await store.WriteAsync(new State { CurrentJournal = 2, NextSequence = 2 }, cancellationToken);

        _ = await Assert.That(SlotFile.Read(NodePathKit.Combine(Dir, "man-current")).State.CurrentJournal).IsEqualTo(2);
    }
}
