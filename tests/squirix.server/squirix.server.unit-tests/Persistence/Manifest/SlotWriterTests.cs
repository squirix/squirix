using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>Coverage for the in-place two-slot <c language="csharp">man-current</c> writer.</summary>
[Immutable]
public sealed class SlotWriterTests : IsolatedStorageTestBase
{
    /// <summary>A publish without its mirror already moves the manifest, and a later writer keeps that slot intact.</summary>
    [Test]
    public async Task UnmirroredPublishIsInForce()
    {
        var path = Path.Join(Dir, "man-current");
        using (var writer = new SlotWriter(path))
            _ = writer.Publish(new State { CurrentJournal = 2 });

        var published = SlotFile.Read(path);
        _ = await Assert.That(published.State.CurrentJournal).IsEqualTo(2);

        var slot = ReadSlot(path, published.Slot);
        using (var next = new SlotWriter(path))
            _ = next.Publish(new State { CurrentJournal = 3 });

        _ = await Assert.That(SlotFile.Read(path).State.CurrentJournal).IsEqualTo(3);
        await SequenceAssert.EqualMemoryAsync<byte>(slot, ReadSlot(path, published.Slot));
    }

    /// <summary>A publish followed by its mirror leaves both slots holding the same manifest and a full-size file.</summary>
    [Test]
    public async Task MirrorWritesBothSlots()
    {
        var path = Path.Join(Dir, "man-current");
        var snapshot = new SnapshotRef { Index = 2, LastAppliedSequence = 8UL, ReplayFromJournalSegment = 3, CreatedUtc = DateTime.UtcNow };
        using (var writer = new SlotWriter(path))
        {
            _ = writer.Publish(new State { CurrentJournal = 4, NextSequence = 9UL, LastSnapshot = snapshot });
            writer.Mirror();
        }

        _ = await Assert.That(new FileInfo(path).Length).IsEqualTo(SlotFormat.FileSize);
        var first = SlotFormat.Select(ReadSlot(path, 0), [], SlotFormat.FileSize, path);
        var second = SlotFormat.Select(ReadSlot(path, 1), [], SlotFormat.FileSize, path);
        _ = await Assert.That(first.State.CurrentJournal).IsEqualTo(4);
        _ = await Assert.That(second.State.CurrentJournal).IsEqualTo(4);
        _ = await Assert.That(first.State.NextSequence).IsEqualTo(9UL);
        _ = await Assert.That(second.State.NextSequence).IsEqualTo(9UL);
        _ = await Assert.That(first.State.LastSnapshot).IsEqualTo(snapshot);
        _ = await Assert.That(second.State.LastSnapshot).IsEqualTo(snapshot);
        _ = await Assert.That(first.Sequence).IsNotEqualTo(second.Sequence);
    }

    /// <summary>The creation time of the snapshot survives a write and a read exactly, so a decoded manifest equals the one written.</summary>
    [Test]
    public async Task CreatedUtcRoundTripsExactly()
    {
        var path = Path.Join(Dir, "man-current");
        var snapshot = new SnapshotRef { Index = 1, LastAppliedSequence = 5UL, ReplayFromJournalSegment = 1, CreatedUtc = DateTime.UtcNow.AddTicks(1) };
        using (var writer = new SlotWriter(path))
            _ = writer.Publish(new State { CurrentJournal = 1, NextSequence = 6UL, LastSnapshot = snapshot });

        var read = SlotFile.Read(path).State.LastSnapshot;

        _ = await Assert.That(read).IsEqualTo(snapshot);
        _ = await Assert.That(read?.CreatedUtc.Ticks).IsEqualTo(snapshot.CreatedUtc.Ticks);
    }

    /// <summary>After a reopen the next publish goes to the slot that does not hold the newest record, leaving that one intact.</summary>
    [Test]
    public async Task ReopenPublishesIntoOtherSlotThanNewest()
    {
        var path = Path.Join(Dir, "man-current");
        using (var writer = new SlotWriter(path))
        {
            _ = writer.Publish(new State { CurrentJournal = 2 });
            writer.Mirror();
            _ = writer.Publish(new State { CurrentJournal = 3 });
            writer.Mirror();
        }

        _ = await Assert.That(SlotFile.Read(path).Slot).IsEqualTo(1);

        using (var reopened = new SlotWriter(path))
            _ = reopened.Publish(new State { CurrentJournal = 5 });

        var selection = SlotFile.Read(path);
        _ = await Assert.That(selection.State.CurrentJournal).IsEqualTo(5);
        _ = await Assert.That(selection.Slot).IsEqualTo(0);
        _ = await Assert.That(SlotFormat.Select(ReadSlot(path, 1), [], SlotFormat.FileSize, path).State.CurrentJournal).IsEqualTo(3);
    }

    /// <summary>A torn publish target leaves the other slot holding the whole previous manifest, snapshot included.</summary>
    [Test]
    public async Task TornTargetFallsBackToPreviousManifest()
    {
        var path = Path.Join(Dir, "man-current");
        var snapshot = new SnapshotRef { Index = 3, LastAppliedSequence = 40UL, ReplayFromJournalSegment = 2, CreatedUtc = DateTime.UtcNow };
        using (var writer = new SlotWriter(path))
        {
            _ = writer.Publish(new State { CurrentJournal = 4, NextSequence = 50UL, LastSnapshot = snapshot });
            writer.Mirror();
            _ = writer.Publish(new State { CurrentJournal = 5, NextSequence = 60UL, LastSnapshot = snapshot });
        }

        var target = SlotFile.Read(path).Slot;
        TearSlot(path, target);

        var selection = SlotFile.Read(path);
        _ = await Assert.That(selection.State.CurrentJournal).IsEqualTo(4);
        _ = await Assert.That(selection.State.NextSequence).IsEqualTo(50UL);
        _ = await Assert.That(selection.State.LastSnapshot).IsEqualTo(snapshot);
    }

    /// <summary>A torn mirror leaves the slot written by the publish as the newest valid one.</summary>
    [Test]
    public async Task TornMirrorSelectsPublishedSlot()
    {
        var path = Path.Join(Dir, "man-current");
        using (var writer = new SlotWriter(path))
        {
            _ = writer.Publish(new State { CurrentJournal = 2 });
            writer.Mirror();
            _ = writer.Publish(new State { CurrentJournal = 3 });
            writer.Mirror();
        }

        var published = SlotFile.Read(path);
        TearSlot(path, 1 - published.Slot);

        var selection = SlotFile.Read(path);
        _ = await Assert.That(selection.Slot).IsEqualTo(published.Slot);
        _ = await Assert.That(selection.State.CurrentJournal).IsEqualTo(3);
    }

    /// <summary>A manifest a reader would reject is refused before anything is written.</summary>
    [Test]
    public async Task RejectedManifestWritesNothing()
    {
        var path = Path.Join(Dir, "man-current");
        using var writer = new SlotWriter(path);

        _ = NodeExceptionAssert.For<ArgumentException>().Throws(writer, static state => state.Publish(new State { CurrentJournal = 0 }));

        _ = await Assert.That(SlotFile.Read(path).Sequence).IsEqualTo(0UL);
    }

    /// <summary>A file written by an earlier build is refused on the first write and left unchanged.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EarlierFileIsRefusedUnchanged(CancellationToken cancellationToken)
    {
        var path = Path.Join(Dir, "man-current");
        byte[] earlier = [0x53, 0x51, 0x4D, 0x50, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x00, 0x00, 0x00, 0xF8, 0x20, 0x38, 0x0D];
        await File.WriteAllBytesAsync(path, earlier, cancellationToken);
        using var writer = new SlotWriter(path);

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(writer, static state => state.Publish(new State { CurrentJournal = 2 }));

        await SequenceAssert.EqualMemoryAsync<byte>(earlier, await File.ReadAllBytesAsync(path, cancellationToken));
    }

    /// <summary>A mirror before any publish is refused instead of writing an invalid record.</summary>
    [Test]
    public void MirrorWithoutPublishIsRefused()
    {
        using var writer = new SlotWriter(Path.Join(Dir, "man-current"));

        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(writer, static state => state.Mirror());
    }

    /// <summary>A disposed writer refuses further writes.</summary>
    [Test]
    public void DisposedWriterRefusesWrites()
    {
        var writer = new SlotWriter(Path.Join(Dir, "man-current"));
        writer.Dispose();

        _ = NodeExceptionAssert.For<ObjectDisposedException>().Throws(writer, static state => state.Publish(new State()));
    }

    private static byte[] ReadSlot(string path, int slot)
    {
        using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, SlotFile.CompatibleShare);
        var record = GC.AllocateArray<byte>(SlotFormat.RecordSize);
        long offset = slot == 0 ? 0 : SlotFormat.SlotSize;
        _ = RandomAccess.Read(handle, record, offset);
        return record;
    }

    private static void TearSlot(string path, int slot)
    {
        using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.ReadWrite, SlotFile.CompatibleShare);
        long offset = (slot == 0 ? 0 : SlotFormat.SlotSize) + SlotFormat.RecordSize - 1;
        var last = GC.AllocateArray<byte>(1);
        _ = RandomAccess.Read(handle, last, offset);
        last[0] ^= 0xFF;
        RandomAccess.Write(handle, last, offset);
    }
}
