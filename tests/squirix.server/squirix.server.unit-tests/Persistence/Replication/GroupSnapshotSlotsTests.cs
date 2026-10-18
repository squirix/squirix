using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>The slot rule of the group snapshot files: which of two slot images is trusted, and which are refused.</summary>
[Immutable]
public sealed class GroupSnapshotSlotsTests : ServerUnitTestBase
{
    private const string FirstPath = "dir/group.snapshot.0";
    private const string GroupId = "snapshot-slots";
    private const string SecondPath = "dir/group.snapshot.1";
    private const int SequenceOffset = 5;

    /// <summary>Two byte-identical valid slots are the mirror of one publish and are accepted.</summary>
    [Test]
    public async Task SelectAcceptsIdenticalMirror()
    {
        var slot = Slot(2UL, Image(5UL));

        var selection = Select(slot, Copy(slot));

        _ = await Assert.That((selection.Slot, selection.Sequence, Commit(in selection))).IsEqualTo((0, 2UL, 5UL));
    }

    /// <summary>A newer slot that is torn in any way is ignored in favor of the older valid one.</summary>
    [Test]
    public async Task SelectFallsBackWhenNewerSlotIsTorn()
    {
        var older = Slot(2UL, Image(4UL));
        var newer = Slot(3UL, Image(5UL));

        foreach (var damaged in TornVariants(newer))
        {
            var (snapshot, slot, sequence) = Select(older, damaged);

            _ = await Assert.That((slot, sequence, snapshot?.CommitIndex)).IsEqualTo((0, 2UL, 4UL));
        }
    }

    /// <summary>The valid slot with the higher sequence wins, whichever file holds it.</summary>
    [Test]
    public async Task SelectPrefersHigherSequence()
    {
        var older = Slot(2UL, Image(4UL));
        var newer = Slot(3UL, Image(5UL));

        var first = Select(newer, older);
        var second = Select(older, newer);

        _ = await Assert.That((first.Slot, first.Sequence, Commit(in first))).IsEqualTo((0, 3UL, 5UL));
        _ = await Assert.That((second.Slot, second.Sequence, Commit(in second))).IsEqualTo((1, 3UL, 5UL));
    }

    /// <summary>Two valid slots with one sequence but different images cannot both be the latest write.</summary>
    [Test]
    public async Task SelectRefusesDifferingEqualSequences()
    {
        var slots = (First: Slot(2UL, Image(4UL)), Second: Slot(2UL, Image(5UL)));

        var exception = NodeExceptionAssert.For<InvalidDataException>().Throws(slots, static state => _ = Select(state.First, state.Second));

        _ = await Assert.That(exception.Message).Contains("is corrupt", StringComparison.Ordinal);
    }

    /// <summary>A refused slot is an error even when the other slot is valid and holds the higher sequence.</summary>
    [Test]
    public async Task SelectRefusesLowerSequenceRefusedSlot()
    {
        var garbage = new byte[Image(4UL).Length];
        garbage.AsSpan().Fill(0xAB);
        var slots = (First: Slot(1UL, garbage), Second: Slot(2UL, Image(5UL)));

        var exception = NodeExceptionAssert.For<InvalidDataException>().Throws(slots, static state => _ = Select(state.First, state.Second));

        _ = await Assert.That(exception.Message).Contains(FirstPath, StringComparison.Ordinal);
    }

    /// <summary>A valid slot of another group is refused even when it holds the older sequence, so no publish overwrites it.</summary>
    [Test]
    public async Task SelectRefusesOlderForeignSlot()
    {
        var slots = (First: Slot(1UL, Image(4UL, "other-group")), Second: Slot(2UL, Image(5UL)));

        var exception = NodeExceptionAssert.For<InvalidDataException>().Throws(slots, static state => _ = Select(state.First, state.Second));

        _ = await Assert.That(exception.Message).Contains($"'{FirstPath}' belongs to a different group", StringComparison.Ordinal);
    }

    /// <summary>A slot file longer than the image bound plus the slot frame is refused by its size.</summary>
    [Test]
    public async Task SelectRefusesOversizeSlot()
    {
        var image = Image(5UL);
        var slots = (Slot: Slot(1UL, image), Max: image.Length - 1);

        var exception = NodeExceptionAssert.For<InvalidDataException>().Throws(slots, static state => _ = Select(state.Slot, null, state.Max));

        _ = await Assert.That(exception.Message).Contains("exceeds the maximum configured size", StringComparison.Ordinal);
    }

    /// <summary>A slot whose frame is sealed but whose image does not decode is refused rather than skipped.</summary>
    [Test]
    public async Task SelectRefusesSealedUndecodableImage()
    {
        var garbage = new byte[Image(4UL).Length];
        garbage.AsSpan().Fill(0xAB);
        var slots = (First: Slot(1UL, Image(4UL)), Second: Slot(2UL, garbage));

        var exception = NodeExceptionAssert.For<InvalidDataException>().Throws(slots, static state => _ = Select(state.First, state.Second));

        _ = await Assert.That(exception.Message).Contains("is corrupt", StringComparison.Ordinal);
    }

    /// <summary>A second slot file with no valid slot beside it means an acknowledged snapshot is lost, which is refused.</summary>
    [Test]
    public async Task SelectRefusesSecondSlotWithoutValidSlot()
    {
        var torn = Slot(1UL, Image(5UL))[..30];

        var absentFirst = NodeExceptionAssert.For<InvalidDataException>().Throws(torn, static state => _ = Select(null, state));
        var tornFirst = NodeExceptionAssert.For<InvalidDataException>().Throws(torn, static state => _ = Select(state, state));

        _ = await Assert.That(absentFirst.Message).Contains("is corrupt", StringComparison.Ordinal);
        _ = await Assert.That(tornFirst.Message).Contains("is corrupt", StringComparison.Ordinal);
    }

    /// <summary>A sealed slot whose image carries another format version is refused and names the versions.</summary>
    [Test]
    public async Task SelectRefusesUnsupportedImageVersion()
    {
        var image = Image(5UL);
        image[4] = 2;
        var slot = Slot(1UL, image);

        var exception = NodeExceptionAssert.For<InvalidDataException>().Throws(slot, static state => _ = Select(state, null));

        _ = await Assert.That(exception.Message).Contains("format version 2", StringComparison.Ordinal);
    }

    /// <summary>A new header persisted over an older, self-valid image is torn, because the checksum covers the header.</summary>
    [Test]
    public async Task SelectRejectsNewHeaderOverOlderImage()
    {
        var current = Slot(2UL, Image(5UL));
        var stale = Slot(1UL, Image(4UL));
        BinaryPrimitives.WriteUInt64LittleEndian(stale.AsSpan(SequenceOffset), 3UL);

        var selection = Select(stale, current);

        _ = await Assert.That((selection.Slot, selection.Sequence, Commit(in selection))).IsEqualTo((1, 2UL, 5UL));
    }

    /// <summary>A first publish that stopped while writing slot 0 leaves no snapshot, whether slot 0 is short, zero-filled or empty.</summary>
    [Test]
    public async Task SelectReturnsNoneAfterCutFirstPublish()
    {
        var slot = Slot(1UL, Image(5UL));

        _ = await Assert.That(Select(slot[..40], null).Snapshot == null).IsTrue();
        _ = await Assert.That(Select(new byte[slot.Length], null).Snapshot == null).IsTrue();
        _ = await Assert.That(Select([], null).Snapshot == null).IsTrue();
    }

    /// <summary>Without any slot file there is no snapshot.</summary>
    [Test]
    public async Task SelectReturnsNoneWithoutSlots()
    {
        var selection = Select(null, null);

        _ = await Assert.That(selection.Snapshot == null).IsTrue();
    }

    private static ulong Commit(in GroupSnapshotSelection selection) => selection.Snapshot?.CommitIndex ?? 0UL;

    private static byte[] Copy(byte[] source)
    {
        var copy = new byte[source.Length];
        source.CopyTo(copy, 0);
        return copy;
    }

    private static byte[] Image(ulong commitIndex, string groupId = GroupId)
    {
        var snapshot = new GroupSnapshot(groupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 1UL, commitIndex, [], DateTime.UnixEpoch);
        var image = new byte[GroupSnapshotStore.ComputePublishedLength(in snapshot, GroupSnapshotStore.DefaultMaxSnapshotBytes)];
        GroupSnapshotStore.EncodePublished(image, in snapshot);
        return image;
    }

    private static GroupSnapshotSelection Select(byte[]? first, byte[]? second, int maxSnapshotBytes = GroupSnapshotStore.DefaultMaxSnapshotBytes) =>
        GroupSnapshotSlots.Select(first ?? [], first != null, second ?? [], second != null, new GroupSnapshotSlotContext(GroupId, maxSnapshotBytes, FirstPath, SecondPath));

    private static byte[] Slot(ulong sequence, byte[] image)
    {
        var slot = new byte[image.Length + GroupSnapshotSlots.Overhead];
        image.CopyTo(slot.AsSpan(GroupSnapshotSlots.HeaderSize));
        GroupSnapshotSlots.WriteHeader(slot, sequence);
        GroupSnapshotSlots.Seal(slot, sequence);
        return slot;
    }

    private static byte[][] TornVariants(byte[] slot)
    {
        var flipped = Copy(slot);
        flipped[^1] ^= 0xFF;
        return [slot[..^3], new byte[slot.Length], [], flipped];
    }
}
