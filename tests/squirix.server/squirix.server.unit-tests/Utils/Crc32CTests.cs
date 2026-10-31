using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Golden vectors and reference equivalence for the CRC32C (Castagnoli) implementation.</summary>
[Immutable]
public sealed class Crc32CTests : ServerUnitTestBase
{
    private const int ExhaustiveLength = 64;
    private const int LargeLength = 65537;
    private const uint ReferencePolynomial = 0x82F63B78u;
    private const uint GeneratorMultiplier = 1664525u;
    private const uint GeneratorIncrement = 1013904223u;
    private const uint GeneratorSeed = 0x1234_5678u;
    private const int GeneratorShift = 24;
    private static readonly uint[] ReferenceTable = CreateReferenceTable();

    private static ReadOnlySpan<int> LargeLengths => [255, 4096, LargeLength];

    private static ReadOnlySpan<int> SplitPoints => [0, 1, 3, 4, 7, 8, 9, 15, 16, 17, 100, 4095, 4096, 65536, LargeLength];

    /// <summary>Incremental append over two chunks matches the canonical check value.</summary>
    [Test]
    public async Task AppendMatchesCompute()
    {
        var actual = ComputeAppended();
        _ = await Assert.That(actual).IsEqualTo(0xE3069283u);
        return;

        static uint ComputeAppended()
        {
            var data = "123456789"u8;
            return Crc32C.Finalize(Crc32C.Append(Crc32C.Append(Crc32C.InitialValue, data[..4]), data[4..]));
        }
    }

    /// <summary>Appending split chunks equals the one-shot checksum for every split point.</summary>
    [Test]
    public async Task IncrementalAppendMatchesCompute() => _ = await Assert.That(CountSplitMismatches()).IsEqualTo(0);

    /// <summary>Single-byte append matches appending a one-byte span.</summary>
    [Test]
    public async Task SingleByteAppendMatchesSpanAppend() => _ = await Assert.That(CountSingleByteMismatches()).IsEqualTo(0);

    /// <summary>Hardware-assisted results equal a byte-at-a-time table reference for lengths 0..64 and larger sizes.</summary>
    [Test]
    public async Task MatchesReferenceForAllLengths() => _ = await Assert.That(CountLengthMismatches()).IsEqualTo(0);

    /// <summary>Slices starting at offsets 1..7 match the reference.</summary>
    [Test]
    public async Task MatchesReferenceForUnalignedSlices() => _ = await Assert.That(CountUnalignedMismatches()).IsEqualTo(0);

    /// <summary>Canonical check value for the ASCII string 123456789 (RFC 3720, Appendix B.4).</summary>
    [Test]
    public async Task CheckValueForDigits() => _ = await Assert.That(Crc32C.Compute("123456789"u8)).IsEqualTo(0xE3069283u);

    /// <summary>Canonical vector for 32 zero bytes (RFC 3720, Appendix B.4).</summary>
    [Test]
    public async Task CheckValueForZeroBlock() => _ = await Assert.That(Crc32C.Compute(new byte[32])).IsEqualTo(0x8A9136AAu);

    /// <summary>Empty input yields the canonical zero checksum.</summary>
    [Test]
    public async Task EmptyInputYieldsZero() => _ = await Assert.That(Crc32C.Compute([])).IsEqualTo(0u);

    private static int CountLengthMismatches()
    {
        var buffer = CreateBuffer(LargeLength);
        var mismatches = 0;
        for (var length = 0; length <= ExhaustiveLength; length++)
        {
            if (!MatchesReference(buffer.AsSpan(0, length)))
                mismatches++;
        }

        foreach (var length in LargeLengths)
        {
            if (!MatchesReference(buffer.AsSpan(0, length)))
                mismatches++;
        }

        return mismatches;
    }

    private static int CountSingleByteMismatches()
    {
        var mismatches = 0;
        foreach (var value in CreateBuffer(256))
        {
            if (Crc32C.Append(Crc32C.InitialValue, value) != Crc32C.Append(Crc32C.InitialValue, [value]))
                mismatches++;
        }

        return mismatches;
    }

    private static int CountSplitMismatches()
    {
        var data = CreateBuffer(LargeLength);
        var expected = Crc32C.Compute(data);
        var mismatches = 0;
        foreach (var split in SplitPoints)
        {
            var crc = Crc32C.Append(Crc32C.InitialValue, data.AsSpan(0, split));
            crc = Crc32C.Append(crc, data.AsSpan(split));
            if (Crc32C.Finalize(crc) != expected)
                mismatches++;
        }

        return mismatches;
    }

    private static int CountUnalignedMismatches()
    {
        var buffer = CreateBuffer(LargeLength);
        var mismatches = 0;
        for (var offset = 1; offset < sizeof(ulong); offset++)
        {
            for (var length = 0; length <= ExhaustiveLength; length++)
            {
                if (!MatchesReference(buffer.AsSpan(offset, length)))
                    mismatches++;
            }

            if (!MatchesReference(buffer.AsSpan(offset, LargeLength - sizeof(ulong))))
                mismatches++;
        }

        return mismatches;
    }

    private static bool MatchesReference(ReadOnlySpan<byte> data) => Crc32C.Compute(data) == ReferenceCompute(data);

    private static byte[] CreateBuffer(int length)
    {
        var buffer = new byte[length];
        var state = GeneratorSeed;
        for (var i = 0; i < buffer.Length; i++)
        {
            state = unchecked((state * GeneratorMultiplier) + GeneratorIncrement);
            buffer[i] = Convert.ToByte(state >> GeneratorShift);
        }

        return buffer;
    }

    private static uint[] CreateReferenceTable()
    {
        var table = new uint[256];
        for (uint i = 0; i < table.Length; i++)
        {
            var c = i;
            for (var k = 0; k < 8; k++)
                c = (c & 1) != 0 ? ReferencePolynomial ^ (c >> 1) : c >> 1;
            table[i] = c;
        }

        return table;
    }

    private static uint ReferenceCompute(ReadOnlySpan<byte> data)
    {
        var crc = 0xFFFF_FFFFu;
        foreach (var b in data)
            crc = ReferenceTable[(crc ^ b) & 0xFF] ^ (crc >> 8);
        return ~crc;
    }
}
