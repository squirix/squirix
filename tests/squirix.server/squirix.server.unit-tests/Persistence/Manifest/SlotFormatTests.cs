using System;
using System.Buffers.Binary;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>Tests for the two-slot manifest layout and selection rules.</summary>
[Immutable]
public sealed class SlotFormatTests
{
    private const string FilePath = "man-current";

    private const int CrcOffset = SlotFormat.RecordSize - sizeof(uint);

    private static readonly DateTime GoldenCreated = new(638_000_000_000_000_000L, DateTimeKind.Utc);

    private static readonly byte[] GoldenWithSnapshot =
    [
        0x53, 0x51, 0x4D, 0x50, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x05, 0x00,
        0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0xB3, 0xA6, 0x9E, 0xA1, 0xDA, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x93, 0x79, 0x81, 0x72,
    ];

    private static readonly byte[] GoldenWithoutSnapshot =
    [
        0x53, 0x51, 0x4D, 0x50, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x46, 0x71, 0x31, 0xB8,
    ];

    /// <summary>Encode emits the documented golden bytes: magic, version, sequence, manifest fields, padding, then CRC over all of them.</summary>
    [Test]
    public async Task EncodeMatchesGoldenWireBytes()
    {
        var withSnapshot = GC.AllocateArray<byte>(SlotFormat.RecordSize);
        var withoutSnapshot = GC.AllocateArray<byte>(SlotFormat.RecordSize);
        var snapshot = new SnapshotRef { Index = 5, LastAppliedSequence = 9UL, ReplayFromJournalSegment = 2, CreatedUtc = GoldenCreated };

        // The CRCs are pinned by an independent computation, not by Crc32C itself:
        // recomputing them here with production code would mask a broken checksum.
        SlotFormat.Encode(withSnapshot, new State { CurrentJournal = 3, NextSequence = 7UL, LastSnapshot = snapshot }, 1UL);
        SlotFormat.Encode(withoutSnapshot, new State { CurrentJournal = 3, NextSequence = 7UL }, 1UL);

        await SequenceAssert.EqualAsync(GoldenWithSnapshot, withSnapshot);
        await SequenceAssert.EqualAsync(GoldenWithoutSnapshot, withoutSnapshot);
    }

    /// <summary>Select reads the golden bytes back into the manifest, with the creation time exact to the tick.</summary>
    [Test]
    public async Task SelectReadsGoldenWireBytes()
    {
        var selection = SlotFormat.Select(GoldenWithSnapshot, [], SlotFormat.RecordSize, FilePath);
        var plain = SlotFormat.Select(GoldenWithoutSnapshot, [], SlotFormat.RecordSize, FilePath);

        _ = await Assert.That(selection.Slot).IsEqualTo(0);
        _ = await Assert.That(selection.Sequence).IsEqualTo(1UL);
        _ = await Assert.That(selection.State.CurrentJournal).IsEqualTo(3);
        _ = await Assert.That(selection.State.NextSequence).IsEqualTo(7UL);
        _ = await Assert.That(selection.State.LastSnapshot).IsEqualTo(new SnapshotRef { Index = 5, LastAppliedSequence = 9UL, ReplayFromJournalSegment = 2, CreatedUtc = GoldenCreated });
        _ = await Assert.That(selection.State.LastSnapshot?.CreatedUtc.Ticks).IsEqualTo(GoldenCreated.Ticks);
        _ = await Assert.That(plain.State.LastSnapshot).IsNull();
    }

    /// <summary>The newest valid slot wins and a damaged or unwritten slot never hides the other one.</summary>
    /// <param name="first">The shape of the record in slot 0.</param>
    /// <param name="second">The shape of the record in slot 1.</param>
    /// <param name="journal">The expected current journal of the selected manifest.</param>
    /// <param name="slot">The expected slot.</param>
    [Test]
    [Arguments("v5:10", "v4:9", 10, 0)]
    [Arguments("v4:9", "v5:10", 10, 1)]
    [Arguments("torn", "v3:8", 8, 1)]
    [Arguments("v3:8", "torn", 8, 0)]
    [Arguments("v3:8", "zero", 8, 0)]
    [Arguments("zero", "v3:8", 8, 1)]
    [Arguments("v3:8", "none", 8, 0)]
    [Arguments("v3:8", "v3:8", 8, 0)]
    [Arguments("zero", "zero", 1, 1)]
    [Arguments("none", "none", 1, 1)]
    public async Task SelectPicksNewestValidSlot(string first, string second, int journal, int slot)
    {
        var selection = SlotFormat.Select(Shape(first), Shape(second), SlotFormat.FileSize, FilePath);

        _ = await Assert.That(selection.State.CurrentJournal).IsEqualTo(journal);
        _ = await Assert.That(selection.Slot).IsEqualTo(slot);
    }

    /// <summary>A file without a valid slot that is not entirely unwritten is refused.</summary>
    /// <param name="first">The shape of the record in slot 0.</param>
    /// <param name="second">The shape of the record in slot 1.</param>
    [Test]
    [Arguments("torn", "zero")]
    [Arguments("zero", "torn")]
    [Arguments("torn", "torn")]
    [Arguments("v3:8", "v3:9")]
    public void SelectThrowsWithoutValidSlot(string first, string second) =>
        _ = NodeExceptionAssert.For<InvalidDataException>().Throws((first, second), static state => SlotFormat.Select(Shape(state.first), Shape(state.second), SlotFormat.FileSize, FilePath));

    /// <summary>A record whose checksum holds but whose fields break a manifest rule is torn, so the other slot still wins.</summary>
    /// <param name="offset">The offset of the byte to change in the damaged record.</param>
    /// <param name="value">The new value of that byte.</param>
    /// <param name="snapshot">Whether the damaged record holds a snapshot.</param>
    [Test]
    [Arguments(5, 0, false)]
    [Arguments(13, 0, false)]
    [Arguments(17, 0, false)]
    [Arguments(25, 2, false)]
    [Arguments(26, 1, false)]
    [Arguments(30, 1, false)]
    [Arguments(38, 1, false)]
    [Arguments(42, 1, false)]
    [Arguments(50, 1, false)]
    [Arguments(26, 0, true)]
    [Arguments(49, 0x80, true)]
    public async Task SelectTreatsRuleViolationAsTorn(int offset, byte value, bool snapshot)
    {
        var damaged = Shape(snapshot ? "s9:7" : "v9:7");
        damaged[offset] = value;
        BinaryPrimitives.WriteUInt32LittleEndian(damaged.AsSpan(CrcOffset), Crc32C.Compute(damaged.AsSpan(0, CrcOffset)));

        var selection = SlotFormat.Select(damaged, Shape("v3:8"), SlotFormat.FileSize, FilePath);

        _ = await Assert.That(selection.Slot).IsEqualTo(1);
        _ = await Assert.That(selection.State.CurrentJournal).IsEqualTo(8);
    }

    /// <summary>A pointer written by an earlier build is refused with a message that says so.</summary>
    [Test]
    public async Task SelectRefusesEarlierPointer()
    {
        byte[] earlier = [0x53, 0x51, 0x4D, 0x43, 0x2A, 0x00, 0x00, 0x00, 0x5D, 0xB7, 0x56, 0x95];

        var failure = NodeExceptionAssert.For<InvalidDataException>().Throws(earlier, static state => SlotFormat.Select(state, [], state.Length, FilePath));

        _ = await Assert.That(failure.Message).Contains("earlier build");
    }

    /// <summary>A slot record that held only a manifest index is refused with a message that says so, whichever slot holds it.</summary>
    /// <param name="inFirst">Whether the earlier record is in slot 0; otherwise it is in slot 1.</param>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task SelectRefusesEarlierSlotRecord(bool inFirst)
    {
        byte[] earlier = [0x53, 0x51, 0x4D, 0x50, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x00, 0x00, 0x00, 0xF8, 0x20, 0x38, 0x0D];

        var failure = NodeExceptionAssert.For<InvalidDataException>().Throws(
            (earlier, inFirst),
            static state => SlotFormat.Select(state.inFirst ? state.earlier : [], state.inFirst ? [] : state.earlier, SlotFormat.FileSize, FilePath));

        _ = await Assert.That(failure.Message).Contains("earlier build");
    }

    /// <summary>A file longer than the two slots is refused.</summary>
    [Test]
    public void SelectThrowsWhenFileIsTooLong() =>
        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(0, static _ => SlotFormat.Select([], [], SlotFormat.FileSize + 1, FilePath));

    /// <summary>A manifest or sequence a reader would reject is refused at encode time, so such a record is never written.</summary>
    /// <param name="journal">The current journal.</param>
    /// <param name="nextSequence">The next sequence.</param>
    /// <param name="badSnapshot">Whether the manifest names a snapshot with index zero.</param>
    /// <param name="sequence">The slot sequence.</param>
    [Test]
    [Arguments(0, 1UL, false, 1UL)]
    [Arguments(1, 0UL, false, 1UL)]
    [Arguments(1, 1UL, false, 0UL)]
    [Arguments(1, 1UL, true, 1UL)]
    public void EncodeRefusesRejectedManifest(int journal, ulong nextSequence, bool badSnapshot, ulong sequence) =>
        _ = NodeExceptionAssert.For<ArgumentException>().Throws(
            (journal, nextSequence, badSnapshot, sequence),
            static state =>
            {
                var manifest = new State
                {
                    CurrentJournal = state.journal,
                    NextSequence = state.nextSequence,
                    LastSnapshot = state.badSnapshot ? new SnapshotRef { Index = 0 } : null,
                };
                SlotFormat.Encode(GC.AllocateArray<byte>(SlotFormat.RecordSize), manifest, state.sequence);
            });

    private static byte[] Shape(string shape)
    {
        ArgumentNullException.ThrowIfNull(shape);
        switch (shape)
        {
            case "none":
                return [];
            case "zero":
                return GC.AllocateArray<byte>(SlotFormat.RecordSize);
            case "torn":
                var torn = Shape("v9:9");
                torn[^1] ^= 0xFF;
                return torn;
            default:
                var parts = shape[1..].Split(':');
                var record = GC.AllocateArray<byte>(SlotFormat.RecordSize);
                var manifest = new State
                {
                    CurrentJournal = int.Parse(parts[1], CultureInfo.InvariantCulture),
                    LastSnapshot = shape[0] == 's' ? new SnapshotRef { Index = 1, LastAppliedSequence = 1UL, CreatedUtc = GoldenCreated } : null,
                };
                SlotFormat.Encode(record, manifest, ulong.Parse(parts[0], CultureInfo.InvariantCulture));
                return record;
        }
    }
}
