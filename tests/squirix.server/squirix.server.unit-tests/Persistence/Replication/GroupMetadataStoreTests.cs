using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>The two-slot metadata file: creation, alternating in-place publishes, and the slot rule applied on load.</summary>
[Immutable]
public sealed class GroupMetadataStoreTests : ServerUnitTestBase
{
    private const string GroupId = "meta-store";

    /// <summary>The first publish leaves a complete two-slot file holding one record, and no file exists before it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FirstPublishCreatesFullFile(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-store-create");
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, GroupId));
        using var store = NewStore(dir, new CountingFollowerLogFaultHooks());
        _ = await Assert.That(File.Exists(path)).IsFalse();

        store.Publish(Meta(1UL));

        var bytes = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        var read = GroupMetadataStore.Read(path);
        _ = await Assert.That(bytes.Length).IsEqualTo(GroupMetadataSlots.FileSize);
        _ = await Assert.That((read.Status, read.Slot, read.Sequence, read.Metadata.CurrentTerm)).IsEqualTo((GroupMetadataReadStatus.Valid, 0, 1UL, 1UL));
        _ = await Assert.That(File.Exists(GroupStoragePaths.GetMetadataTempPath(dir, GroupId))).IsFalse();
    }

    /// <summary>Each publish goes to the slot that does not hold the newest record, the sequence grows, and the file never changes size.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishAlternatesSlots(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-store-alternate");
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, GroupId));
        using var store = NewStore(dir, new CountingFollowerLogFaultHooks());

        for (var term = 1UL; term <= 5UL; term++)
        {
            store.Publish(Meta(term));

            var read = GroupMetadataStore.Read(path);
            var expectedSlot = term % 2UL == 1UL ? 0 : 1;
            _ = await Assert.That((read.Slot, read.Sequence, read.Metadata.CurrentTerm)).IsEqualTo((expectedSlot, term, term));
        }

        var bytes = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        _ = await Assert.That(bytes.Length).IsEqualTo(GroupMetadataSlots.FileSize);
    }

    /// <summary>A reopened store reads the newest record and continues with the other slot and the next sequence.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReopenContinuesFromNewestSlot(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-store-reopen");
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        PublishTerms(dir, 1UL, 2UL, 3UL);

        using var reopened = NewStore(dir, new CountingFollowerLogFaultHooks());
        var loaded = reopened.Open();
        reopened.Publish(Meta(4UL));

        _ = await Assert.That((loaded.Status, loaded.Slot, loaded.Sequence, loaded.Metadata.CurrentTerm)).IsEqualTo((GroupMetadataReadStatus.Valid, 0, 3UL, 3UL));
        var read = GroupMetadataStore.Read(path);
        _ = await Assert.That((read.Slot, read.Sequence, read.Metadata.CurrentTerm)).IsEqualTo((1, 4UL, 4UL));
        _ = await Assert.That((await GroupMetadataFile.ReadBytesAsync(path, cancellationToken)).Length).IsEqualTo(GroupMetadataSlots.FileSize);
    }

    /// <summary>A flipped byte in the newest slot makes the previous record the one that is read, and the next publish overwrites the corrupt slot.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CorruptNewestSlotFallsBack(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-store-corrupt");
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        PublishTerms(dir, 1UL, 2UL);
        await FollowerLogTestKit.CorruptByteAsync(path, GroupMetadataSlots.SlotSize + 20, cancellationToken);

        using var reopened = NewStore(dir, new CountingFollowerLogFaultHooks());
        var loaded = reopened.Open();
        reopened.Publish(Meta(3UL));

        _ = await Assert.That((loaded.Status, loaded.Slot, loaded.Sequence, loaded.Metadata.CurrentTerm)).IsEqualTo((GroupMetadataReadStatus.Valid, 0, 1UL, 1UL));
        var read = GroupMetadataStore.Read(path);
        _ = await Assert.That((read.Slot, read.Sequence, read.Metadata.CurrentTerm)).IsEqualTo((1, 2UL, 3UL));
    }

    /// <summary>A newest slot whose second half never reached the medium is skipped like a corrupt one.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TornNewestSlotFallsBack(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-store-torn");
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        PublishTerms(dir, 1UL, 2UL, 3UL);
        var bytes = await File.ReadAllBytesAsync(path, cancellationToken);
        Array.Clear(bytes, GroupMetadataSlots.SlotSize / 2, GroupMetadataSlots.SlotSize / 2);
        Array.Clear(bytes, 0, 64);
        await File.WriteAllBytesAsync(path, bytes, cancellationToken);

        using var reopened = NewStore(dir, new CountingFollowerLogFaultHooks());
        var loaded = reopened.Open();
        reopened.Publish(Meta(4UL));

        _ = await Assert.That((loaded.Status, loaded.Slot, loaded.Sequence, loaded.Metadata.CurrentTerm)).IsEqualTo((GroupMetadataReadStatus.Valid, 1, 2UL, 2UL));
        var read = GroupMetadataStore.Read(path);
        _ = await Assert.That((read.Slot, read.Sequence, read.Metadata.CurrentTerm)).IsEqualTo((0, 3UL, 4UL));
    }

    /// <summary>With both slots corrupt no record is trusted and the file is left untouched.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BothSlotsCorruptIsRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-store-both-corrupt");
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        PublishTerms(dir, 1UL, 2UL);
        await FollowerLogTestKit.CorruptByteAsync(path, 20, cancellationToken);
        await FollowerLogTestKit.CorruptByteAsync(path, GroupMetadataSlots.SlotSize + 20, cancellationToken);
        var before = await File.ReadAllBytesAsync(path, cancellationToken);

        using var reopened = NewStore(dir, new CountingFollowerLogFaultHooks());
        var loaded = reopened.Open();

        _ = await Assert.That(loaded.Status).IsEqualTo(GroupMetadataReadStatus.Corrupt);
        await AssertBytesAsync(path, before, cancellationToken);
    }

    /// <summary>Two valid slots with one sequence are accepted when they are byte-identical.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EqualSequencesWithIdenticalBytes(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-store-equal-same");
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, GroupId));
        await File.WriteAllBytesAsync(path, TwoSlotImage(5UL, Meta(7UL), 5UL, Meta(7UL)), cancellationToken);

        var read = GroupMetadataStore.Read(path);

        _ = await Assert.That((read.Status, read.Sequence, read.Metadata.CurrentTerm)).IsEqualTo((GroupMetadataReadStatus.Valid, 5UL, 7UL));
    }

    /// <summary>Two valid slots with one sequence but different bytes cannot be told apart, so the metadata is corrupt.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EqualSequencesWithDifferentBytes(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-store-equal-diff");
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, GroupId));
        await File.WriteAllBytesAsync(path, TwoSlotImage(5UL, Meta(7UL), 5UL, Meta(8UL)), cancellationToken);

        var read = GroupMetadataStore.Read(path);

        _ = await Assert.That(read.Status).IsEqualTo(GroupMetadataReadStatus.Corrupt);
    }

    /// <summary>A record that does not fit a slot is refused before any byte is written, on an existing file and on creation.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OversizedRecordIsRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-store-oversized");
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, GroupId));
        var hooks = new CountingFollowerLogFaultHooks();
        var oversized = Meta(9UL) with { VotedFor = new string('v', GroupMetadataSlots.PayloadCapacity) };
        using var store = NewStore(dir, hooks);

        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(store, oversized, static (target, record) => target.Publish(in record));
        _ = await Assert.That(File.Exists(path)).IsFalse();

        store.Publish(Meta(1UL));
        var before = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        var writesBefore = hooks.Snapshot().MetaWrites;
        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(store, oversized, static (target, record) => target.Publish(in record));

        await AssertBytesAsync(path, before, cancellationToken);
        _ = await Assert.That(hooks.Snapshot().MetaWrites).IsEqualTo(writesBefore);
    }

    /// <summary>A file in the single-record layout of an earlier build is refused and not modified.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EarlierLayoutIsRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-store-earlier");
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, GroupId));
        var meta = Meta(3UL);
        var earlier = new byte[GroupLogCodec.ComputeMetaEncodedLength(in meta)];
        GroupLogCodec.EncodeMeta(in meta, earlier);
        await File.WriteAllBytesAsync(path, earlier, cancellationToken);

        using var store = NewStore(dir, new CountingFollowerLogFaultHooks());
        var loaded = store.Open();

        _ = await Assert.That(loaded.Status).IsEqualTo(GroupMetadataReadStatus.Unsupported);
        _ = await Assert.That(GroupMetadataStore.DescribeFailure(loaded.Status, GroupId)).Contains("earlier build", StringComparison.Ordinal);
        await AssertBytesAsync(path, earlier, cancellationToken);
    }

    /// <summary>A file of any size other than two slots is corrupt, not from an earlier build, and is not modified, even when its first bytes look like a slot.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WrongSizeIsRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-store-size");
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, GroupId));
        var image = TwoSlotImage(1UL, Meta(1UL), 0UL, Meta(0UL));
        var longer = new byte[image.Length + 1];
        image.CopyTo(longer, 0);
        await File.WriteAllBytesAsync(path, longer, cancellationToken);

        using var store = NewStore(dir, new CountingFollowerLogFaultHooks());
        var loaded = store.Open();

        _ = await Assert.That(loaded.Status).IsEqualTo(GroupMetadataReadStatus.Corrupt);
        await AssertBytesAsync(path, longer, cancellationToken);
    }

    /// <summary>The static read opens a file the writer holds open and applies the slot rule to it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadWorksWhileWriterIsOpen(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-store-shared");
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, GroupId));
        using var store = NewStore(dir, new CountingFollowerLogFaultHooks());
        store.Publish(Meta(1UL));
        store.Publish(Meta(2UL));

        var read = GroupMetadataStore.Read(path);
        var fromKit = await GroupMetadataFile.ReadAsync(path, cancellationToken);

        _ = await Assert.That((read.Status, read.Slot, read.Sequence, read.Metadata.CurrentTerm)).IsEqualTo((GroupMetadataReadStatus.Valid, 1, 2UL, 2UL));
        _ = await Assert.That(fromKit.CurrentTerm).IsEqualTo(2UL);
    }

    /// <summary>A publish that fails before the write leaves the previous record current, and the next publish reuses the same slot.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedPublishKeepsPreviousRecord(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-store-failed");
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, GroupId));
        var armed = false;
        var faults = new IFollowerLogFaultHooksCreateExpectations();
        _ = faults.Setups.OnMetaPublishing().Callback(() =>
        {
            if (armed)
                throw new IOException("simulated failure at the metadata publish.");
        });
        using var store = new GroupMetadataStore(path, GroupStoragePaths.GetMetadataTempPath(dir, GroupId), GroupId, faults.Instance(), NullLogger.Instance);
        store.Publish(Meta(1UL));
        var before = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        armed = true;

        _ = NodeExceptionAssert.For<IOException>().Throws(store, Meta(2UL), static (target, record) => target.Publish(in record));

        await AssertBytesAsync(path, before, cancellationToken);
        armed = false;
        store.Publish(Meta(3UL));
        var read = GroupMetadataStore.Read(path);
        _ = await Assert.That((read.Slot, read.Sequence, read.Metadata.CurrentTerm)).IsEqualTo((1, 2UL, 3UL));
    }

    /// <summary>A failed creation leaves no metadata file, so the next publish creates it.</summary>
    [Test]
    public async Task FailedCreationLeavesNoFile()
    {
        using var dir = new TempDirectory("squirix-meta-store-failed-create");
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, GroupId));
        var armed = true;
        var faults = new IFollowerLogFaultHooksCreateExpectations();
        _ = faults.Setups.OnMetaPublishing().Callback(() =>
        {
            if (armed)
                throw new IOException("simulated failure at the metadata creation.");
        });
        using var store = new GroupMetadataStore(path, GroupStoragePaths.GetMetadataTempPath(dir, GroupId), GroupId, faults.Instance(), NullLogger.Instance);

        _ = NodeExceptionAssert.For<IOException>().Throws(store, Meta(1UL), static (target, record) => target.Publish(in record));
        _ = await Assert.That(File.Exists(path)).IsFalse();
        armed = false;
        store.Publish(Meta(2UL));

        var read = GroupMetadataStore.Read(path);
        _ = await Assert.That((read.Slot, read.Sequence, read.Metadata.CurrentTerm)).IsEqualTo((0, 1UL, 2UL));
    }

    /// <summary>A store that did not open an existing file never replaces it on publish.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishNeverReplacesUnopenedFile(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-store-unopened");
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        PublishTerms(dir, 1UL, 2UL);
        var before = await File.ReadAllBytesAsync(path, cancellationToken);
        using var store = NewStore(dir, new CountingFollowerLogFaultHooks());

        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(store, Meta(3UL), static (target, record) => target.Publish(in record));

        await AssertBytesAsync(path, before, cancellationToken);
    }

    private static async Task AssertBytesAsync(string path, byte[] expected, CancellationToken cancellationToken)
    {
        var actual = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        await SequenceAssert.EqualAsync(expected, actual);
    }

    private static GroupLogMetadata Meta(ulong term) => new(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, term, string.Empty, 0UL, 0UL);

    private static GroupMetadataStore NewStore(string dir, IFollowerLogFaultHooks hooks) =>
        new(GroupStoragePaths.GetMetadataPath(dir, GroupId), GroupStoragePaths.GetMetadataTempPath(dir, GroupId), GroupId, hooks, NullLogger.Instance);

    private static byte[] TwoSlotImage(ulong firstSequence, in GroupLogMetadata first, ulong secondSequence, in GroupLogMetadata second)
    {
        var image = new byte[GroupMetadataSlots.FileSize];
        GroupMetadataSlots.Encode(image.AsSpan(0, GroupMetadataSlots.SlotSize), firstSequence, in first);
        GroupMetadataSlots.Encode(image.AsSpan(GroupMetadataSlots.SlotSize), secondSequence, in second);
        return image;
    }

    /// <summary>Publishes one record per term through a fresh store, which is closed again before the call returns.</summary>
    /// <param name="dir">The persistence root.</param>
    /// <param name="terms">The terms to publish in order.</param>
    private static void PublishTerms(string dir, params ulong[] terms)
    {
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, GroupId));
        using var store = NewStore(dir, new CountingFollowerLogFaultHooks());
        for (var i = 0; i < terms.Length; i++)
            store.Publish(Meta(terms[i]));
    }
}
