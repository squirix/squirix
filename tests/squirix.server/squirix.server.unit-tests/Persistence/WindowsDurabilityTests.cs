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

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Durability behavior tests for manifest persistence in the two-slot current file.</summary>
public sealed class WindowsDurabilityTests : IsolatedStorageTestBase
{
    /// <summary>Verifies that <see cref="Ledger" /> creates the current file holding the first manifest.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FirstWriteCreatesCurrentFile(CancellationToken cancellationToken)
    {
        var options = StoreTestSupport.CreateOptions(Dir);
        using var store = new Ledger(options, NullLogger<Ledger>.Instance);

        await store.WriteAsync(new State { CurrentJournal = 1, NextSequence = 1 }, cancellationToken);
        var currentPath = NodePathKit.Combine(Dir, "man-current");
        _ = await Assert.That(File.Exists(currentPath)).IsTrue();
        _ = await Assert.That(StoreTestSupport.ReadCurrentSelection(Dir).State.CurrentJournal).IsEqualTo(1);
    }

    /// <summary>Verifies that first boot without a current file returns a default manifest.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MissingFileReadsAsDefault(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions { DataDir = Dir };
        using var store = new Ledger(options, NullLogger<Ledger>.Instance);

        var manifest = await store.ReadCurrentOrDefaultAsync(cancellationToken);

        _ = await Assert.That(manifest.CurrentJournal).IsEqualTo(1);
        _ = await Assert.That(manifest.NextSequence).IsEqualTo(1UL);
    }

    /// <summary>Verifies that subsequent manifest writes replace the manifest in the current file.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RewriteReplacesManifest(CancellationToken cancellationToken)
    {
        var options = StoreTestSupport.CreateOptions(Dir);
        using var store = new Ledger(options, NullLogger<Ledger>.Instance);

        await store.WriteAsync(new State { CurrentJournal = 1, NextSequence = 1 }, cancellationToken);
        await store.WriteAsync(new State { CurrentJournal = 2, NextSequence = 10 }, cancellationToken);
        var selection = StoreTestSupport.ReadCurrentSelection(Dir);
        _ = await Assert.That(selection.State.CurrentJournal).IsEqualTo(2);
        _ = await Assert.That(selection.State.NextSequence).IsEqualTo(10UL);
    }

    /// <summary>Verifies that an empty current file, as left by a crash during the first publish, reads as never published.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EmptyFileReadsAsDefault(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions { DataDir = Dir };
        using var store = new Ledger(options, NullLogger<Ledger>.Instance);
        await File.WriteAllBytesAsync(NodePathKit.Combine(Dir, "man-current"), ReadOnlyMemory<byte>.Empty, cancellationToken);

        var manifest = await store.ReadCurrentOrDefaultAsync(cancellationToken);

        _ = await Assert.That(manifest.CurrentJournal).IsEqualTo(1);
    }

    /// <summary>Verifies that a current file of an earlier build is refused and left unchanged.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EarlierFileIsRefusedUnchanged(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions { DataDir = Dir };
        using var store = new Ledger(options, NullLogger<Ledger>.Instance);
        var path = NodePathKit.Combine(Dir, "man-current");
        byte[] earlier = [0x53, 0x51, 0x4D, 0x43, 0x2A, 0x00, 0x00, 0x00, 0x5D, 0xB7, 0x56, 0x95];
        await File.WriteAllBytesAsync(path, earlier, cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(store.ReadCurrentOrDefaultAsync(cancellationToken));

        await SequenceAssert.EqualMemoryAsync<byte>(earlier, await File.ReadAllBytesAsync(path, cancellationToken));
    }

    /// <summary>Verifies that a manifest without a valid slot fails loudly and is left unchanged.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ManifestWithoutValidSlotFailsLoudly(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions { DataDir = Dir };
        using var store = new Ledger(options, NullLogger<Ledger>.Instance);
        var path = NodePathKit.Combine(Dir, "man-current");
        var image = GC.AllocateArray<byte>(SlotFormat.FileSize);
        SlotFormat.Encode(image, new State { CurrentJournal = 5 }, 1UL);
        SlotFormat.Encode(image.AsSpan(SlotFormat.SlotSize), new State { CurrentJournal = 6 }, 2UL);
        image[SlotFormat.RecordSize - 1] ^= 0xFF;
        image[SlotFormat.SlotSize + SlotFormat.RecordSize - 1] ^= 0xFF;
        await File.WriteAllBytesAsync(path, image, cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(store.ReadCurrentOrDefaultAsync(cancellationToken));

        await SequenceAssert.EqualMemoryAsync<byte>(image, await File.ReadAllBytesAsync(path, cancellationToken));
    }

    /// <summary>Verifies that a torn newest slot falls back to the older slot, which holds the whole previous manifest with its snapshot.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TornNewestSlotReadsPreviousManifest(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions { DataDir = Dir };
        using var store = new Ledger(options, NullLogger<Ledger>.Instance);
        var snapshot = new SnapshotRef { Index = 2, LastAppliedSequence = 30UL, ReplayFromJournalSegment = 2, CreatedUtc = DateTime.UtcNow };
        var image = GC.AllocateArray<byte>(SlotFormat.FileSize);
        SlotFormat.Encode(image, new State { CurrentJournal = 3, NextSequence = 40UL, LastSnapshot = snapshot }, 1UL);
        SlotFormat.Encode(image.AsSpan(SlotFormat.SlotSize), new State { CurrentJournal = 4, NextSequence = 50UL, LastSnapshot = snapshot }, 2UL);
        image[SlotFormat.SlotSize + SlotFormat.RecordSize - 1] ^= 0xFF;
        await File.WriteAllBytesAsync(NodePathKit.Combine(Dir, "man-current"), image, cancellationToken);

        var manifest = await store.ReadCurrentOrDefaultAsync(cancellationToken);

        _ = await Assert.That(manifest.CurrentJournal).IsEqualTo(3);
        _ = await Assert.That(manifest.NextSequence).IsEqualTo(40UL);
        _ = await Assert.That(manifest.LastSnapshot).IsEqualTo(snapshot);
    }
}
