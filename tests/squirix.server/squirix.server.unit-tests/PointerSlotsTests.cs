using System;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Tests for the two-slot CURRENT pointer layout and selection rules.</summary>
[Immutable]
public sealed class PointerSlotsTests
{
    private const string FilePath = "man-current";

    /// <summary>Encode emits the documented golden bytes: magic, version, sequence, index, then CRC over all of them.</summary>
    [Test]
    public async Task EncodeMatchesGoldenWireBytes()
    {
        var actual = GC.AllocateArray<byte>(PointerSlots.RecordSize);
        PointerSlots.Encode(actual, 1UL, 42);

        // "SQMP", version 1, sequence 1, index 42, CRC32C over the first 17 bytes.
        // The CRC is pinned by an independent computation, not by Crc32C itself:
        // recomputing it here with production code would mask a broken checksum.
        byte[] golden = [0x53, 0x51, 0x4D, 0x50, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x00, 0x00, 0x00, 0xF8, 0x20, 0x38, 0x0D];
        await SequenceAssert.EqualAsync(golden, actual);
    }

    /// <summary>Select reads the golden bytes back into the manifest index.</summary>
    [Test]
    public async Task SelectReadsGoldenWireBytes()
    {
        byte[] golden = [0x53, 0x51, 0x4D, 0x50, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x00, 0x00, 0x00, 0xF8, 0x20, 0x38, 0x0D];

        var selection = PointerSlots.Select(golden, [], PointerSlots.RecordSize, FilePath);

        _ = await Assert.That(selection).IsEqualTo(new PointerSelection(42, 0, 1UL));
    }

    /// <summary>The newest valid slot wins and a damaged or unwritten slot never hides the other one.</summary>
    /// <param name="first">The shape of the record in slot 0.</param>
    /// <param name="second">The shape of the record in slot 1.</param>
    /// <param name="index">The expected manifest index.</param>
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
    [Arguments("zero", "zero", 0, 1)]
    [Arguments("none", "none", 0, 1)]
    public async Task SelectPicksNewestValidSlot(string first, string second, int index, int slot)
    {
        var selection = PointerSlots.Select(Shape(first), Shape(second), PointerSlots.FileSize, FilePath);

        _ = await Assert.That(selection.ManifestIndex).IsEqualTo(index);
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
        _ = NodeExceptionAssert.For<InvalidDataException>().Throws((first, second), static state => PointerSlots.Select(Shape(state.first), Shape(state.second), PointerSlots.FileSize, FilePath));

    /// <summary>A pointer written by an earlier build is refused with a message that says so.</summary>
    [Test]
    public async Task SelectRefusesEarlierPointer()
    {
        byte[] earlier = [0x53, 0x51, 0x4D, 0x43, 0x2A, 0x00, 0x00, 0x00, 0x5D, 0xB7, 0x56, 0x95];

        var failure = NodeExceptionAssert.For<InvalidDataException>().Throws(earlier, static state => PointerSlots.Select(state, [], state.Length, FilePath));

        _ = await Assert.That(failure.Message).Contains("earlier build");
    }

    /// <summary>A file longer than the two slots is refused.</summary>
    [Test]
    public void SelectThrowsWhenFileIsTooLong() =>
        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(0, static _ => PointerSlots.Select([], [], PointerSlots.FileSize + 1, FilePath));

    private static byte[] Shape(string shape)
    {
        switch (shape)
        {
            case "none":
                return [];
            case "zero":
                return GC.AllocateArray<byte>(PointerSlots.RecordSize);
            case "torn":
                var torn = Shape("v9:9");
                torn[^1] ^= 0xFF;
                return torn;
            default:
                var parts = shape[1..].Split(':');
                var record = GC.AllocateArray<byte>(PointerSlots.RecordSize);
                PointerSlots.Encode(record, ulong.Parse(parts[0], CultureInfo.InvariantCulture), int.Parse(parts[1], CultureInfo.InvariantCulture));
                return record;
        }
    }
}
