using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>The generation rule of the group log files: which of two files is trusted, and which shapes are refused.</summary>
[Immutable]
public sealed class GroupLogSlotsTests : ServerUnitTestBase
{
    private const string Location = "dir";
    private const int VersionOffset = 4;
    private const int FileLength = 40;

    /// <summary>A written header selects its own generation.</summary>
    [Test]
    public async Task HeaderRoundTrips()
    {
        _ = await Assert.That(Select(Header(7UL), null)).IsEqualTo((0, 7UL));
        _ = await Assert.That(Select(null, Header(8UL))).IsEqualTo((1, 8UL));
    }

    /// <summary>The valid file with the higher generation wins, whichever slot holds it.</summary>
    [Test]
    public async Task SelectPrefersHigherGeneration()
    {
        var older = WithFrames(Header(2UL));
        var newer = WithFrames(Header(3UL));

        _ = await Assert.That(Select(newer, older)).IsEqualTo((0, 3UL));
        _ = await Assert.That(Select(older, newer)).IsEqualTo((1, 3UL));
    }

    /// <summary>Two valid files with one generation are refused.</summary>
    [Test]
    public async Task SelectRefusesEqualGenerations()
    {
        var header = WithFrames(Header(4UL));

        var ex = NodeExceptionAssert.For<InvalidDataException>().Throws(header, header, static (first, second) => Select(first, second));

        _ = await Assert.That(ex.Message).Contains("corrupt", StringComparison.Ordinal);
    }

    /// <summary>A header whose checksum fails loses to the other valid file.</summary>
    [Test]
    public async Task SelectIgnoresFlippedChecksum()
    {
        var torn = WithFrames(Header(9UL));
        torn[GroupLogSlots.HeaderSize - 1] ^= 0xFF;

        _ = await Assert.That(Select(torn, WithFrames(Header(2UL)))).IsEqualTo((1, 2UL));
    }

    /// <summary>A zeroed header with frames behind it is not trusted over the other valid file.</summary>
    [Test]
    public async Task SelectIgnoresZeroedHeader()
    {
        _ = await Assert.That(Select(WithFrames(Header(5UL)), GC.AllocateArray<byte>(FileLength))).IsEqualTo((0, 5UL));
        _ = await Assert.That(Select(GC.AllocateArray<byte>(FileLength), WithFrames(Header(5UL)))).IsEqualTo((1, 5UL));
    }

    /// <summary>A sound header of another format version is refused rather than skipped.</summary>
    [Test]
    public async Task SelectRefusesOtherVersion()
    {
        var other = WithFrames(Header(1UL));
        other[VersionOffset] = 2;
        BinaryPrimitives.WriteUInt32LittleEndian(other.AsSpan(GroupLogSlots.HeaderSize - sizeof(uint)), Crc32C.Compute(other.AsSpan(0, GroupLogSlots.HeaderSize - sizeof(uint))));

        var ex = NodeExceptionAssert.For<InvalidDataException>().Throws(other, WithFrames(Header(2UL)), static (first, second) => Select(first, second));

        _ = await Assert.That(ex.Message).Contains("version 2", StringComparison.Ordinal);
    }

    /// <summary>Generation 0 is never written, so a header carrying it is torn.</summary>
    [Test]
    public async Task SelectTreatsGenerationZeroAsTorn()
    {
        _ = await Assert.That(Select(WithFrames(Header(0UL)), WithFrames(Header(1UL)))).IsEqualTo((1, 1UL));
        _ = await Assert.That(Select(WithFrames(Header(1UL)), WithFrames(Header(0UL)))).IsEqualTo((0, 1UL));
    }

    /// <summary>A first creation that never completed leaves no log.</summary>
    [Test]
    public async Task SelectReturnsNoLogAfterCutCreation()
    {
        _ = await Assert.That(Select(null, null)).IsEqualTo((0, 0UL));
        _ = await Assert.That(Select([], null)).IsEqualTo((0, 0UL));
        _ = await Assert.That(Select(GC.AllocateArray<byte>(GroupLogSlots.HeaderSize), null)).IsEqualTo((0, 0UL));
        _ = await Assert.That(Select(GC.AllocateArray<byte>(5), null)).IsEqualTo((0, 0UL));
    }

    /// <summary>A file that could hold acknowledged frames but has no valid header is refused, whichever slot it sits in.</summary>
    [Test]
    public void SelectRefusesShapesWithoutValidHeader()
    {
        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(GC.AllocateArray<byte>(FileLength), static first => Select(first, null));
        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(static () => Select(null, []));
        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(static () => Select(GC.AllocateArray<byte>(5), GC.AllocateArray<byte>(3)));
    }

    private static byte[] Header(ulong generation)
    {
        var header = GC.AllocateArray<byte>(GroupLogSlots.HeaderSize);
        GroupLogSlots.WriteHeader(header, generation);
        return header;
    }

    private static byte[] WithFrames(byte[] header)
    {
        var file = GC.AllocateArray<byte>(FileLength);
        header.CopyTo(file, 0);
        return file;
    }

    private static (int Slot, ulong Generation) Select(byte[]? first, byte[]? second) =>
        GroupLogSlots.Select(
            first.AsSpan(0, Math.Min(first?.Length ?? 0, GroupLogSlots.HeaderSize)),
            first?.Length ?? -1,
            second.AsSpan(0, Math.Min(second?.Length ?? 0, GroupLogSlots.HeaderSize)),
            second?.Length ?? -1,
            Location);
}
