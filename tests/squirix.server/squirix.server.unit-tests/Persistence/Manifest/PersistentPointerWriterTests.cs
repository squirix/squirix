using System;
using System.IO;
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
public sealed class PersistentPointerWriterTests : IsolatedStorageTestBase
{
    /// <summary>A publish without its mirror already moves the pointer, and a later writer keeps that slot intact.</summary>
    [Test]
    public async Task UnmirroredPublishIsInForce()
    {
        var path = Path.Join(Dir, "man-current");
        using (var writer = new PersistentPointerWriter(path))
            writer.Publish(2);

        var published = PointerFile.Read(path);
        _ = await Assert.That(published.ManifestIndex).IsEqualTo(2);

        var slot = ReadSlot(path, published.Slot);
        using (var next = new PersistentPointerWriter(path))
            next.Publish(3);

        _ = await Assert.That(PointerFile.Read(path).ManifestIndex).IsEqualTo(3);
        await SequenceAssert.EqualMemoryAsync<byte>(slot, ReadSlot(path, published.Slot));
    }

    /// <summary>A publish followed by its mirror leaves both slots on the same index and a full-size file.</summary>
    [Test]
    public async Task MirrorWritesBothSlots()
    {
        var path = Path.Join(Dir, "man-current");
        using (var writer = new PersistentPointerWriter(path))
        {
            writer.Publish(4);
            writer.Mirror();
        }

        _ = await Assert.That(new FileInfo(path).Length).IsEqualTo(PointerSlots.FileSize);
        _ = await Assert.That(PointerSlots.Select(ReadSlot(path, 0), [], PointerSlots.FileSize, path).ManifestIndex).IsEqualTo(4);
        _ = await Assert.That(PointerSlots.Select(ReadSlot(path, 1), [], PointerSlots.FileSize, path).ManifestIndex).IsEqualTo(4);
    }

    /// <summary>After a reopen the next publish goes to the slot that does not hold the newest record, leaving that one intact.</summary>
    [Test]
    public async Task ReopenPublishesIntoOtherSlotThanNewest()
    {
        var path = Path.Join(Dir, "man-current");
        using (var writer = new PersistentPointerWriter(path))
        {
            writer.Publish(2);
            writer.Mirror();
            writer.Publish(3);
            writer.Mirror();
        }

        _ = await Assert.That(PointerFile.Read(path).Slot).IsEqualTo(1);

        using (var reopened = new PersistentPointerWriter(path))
            reopened.Publish(5);

        var selection = PointerFile.Read(path);
        _ = await Assert.That(selection.ManifestIndex).IsEqualTo(5);
        _ = await Assert.That(selection.Slot).IsEqualTo(0);
        _ = await Assert.That(PointerSlots.Select(ReadSlot(path, 1), [], PointerSlots.FileSize, path).ManifestIndex).IsEqualTo(3);
    }

    /// <summary>A mirror before any publish is refused instead of writing an invalid record.</summary>
    [Test]
    public void MirrorWithoutPublishIsRefused()
    {
        using var writer = new PersistentPointerWriter(Path.Join(Dir, "man-current"));

        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(writer, static state => state.Mirror());
    }

    /// <summary>A disposed writer refuses further writes.</summary>
    [Test]
    public void DisposedWriterRefusesWrites()
    {
        var writer = new PersistentPointerWriter(Path.Join(Dir, "man-current"));
        writer.Dispose();

        _ = NodeExceptionAssert.For<ObjectDisposedException>().Throws(writer, static state => state.Publish(1));
    }

    private static byte[] ReadSlot(string path, int slot)
    {
        using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, PointerFile.CompatibleShare);
        var record = GC.AllocateArray<byte>(PointerSlots.RecordSize);
        long offset = slot == 0 ? 0 : PointerSlots.SlotSize;
        _ = RandomAccess.Read(handle, record, offset);
        return record;
    }
}
