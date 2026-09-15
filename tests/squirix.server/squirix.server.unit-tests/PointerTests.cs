using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Tests for the fixed-size SQMC CURRENT pointer.</summary>
[Immutable]
public sealed class PointerTests
{
    /// <summary>Read decodes the golden bytes back into the manifest index.</summary>
    [Test]
    public async Task ReadReadsGoldenWireBytes()
    {
        byte[] golden = [0x53, 0x51, 0x4D, 0x43, 0x2A, 0x00, 0x00, 0x00, 0x5D, 0xB7, 0x56, 0x95];

        _ = await Assert.That(Pointer.Read(golden)).IsEqualTo(42);
    }

    /// <summary>Verifies corrupted CRC bytes are rejected on read.</summary>
    [Test]
    public void ReadThrowsWhenCrcIsInvalid() => _ = NodeExceptionAssert.For<InvalidDataException>().Throws(0, static _ => ReadCorruptPointer());

    /// <summary>Write emits the documented golden bytes: magic, index, then CRC over both.</summary>
    [Test]
    public async Task WriteMatchesGoldenWireBytes()
    {
        var actual = WriteIndexBytes(42);

        // "SQMC", index 42 little-endian, CRC32C over the first 8 bytes.
        // The CRC is pinned by an independent computation, not by Crc32C itself:
        // recomputing it here with production code would mask a broken checksum.
        byte[] golden = [0x53, 0x51, 0x4D, 0x43, 0x2A, 0x00, 0x00, 0x00, 0x5D, 0xB7, 0x56, 0x95];
        await SequenceAssert.Equal(golden, actual);
        return;

        static byte[] WriteIndexBytes(int index)
        {
            Span<byte> buffer = stackalloc byte[Pointer.Size];
            Pointer.Write(buffer, index);
            var copy = new byte[buffer.Length];
            buffer.CopyTo(copy);
            return copy;
        }
    }

    /// <summary>Verifies a write/read round-trip for a manifest index.</summary>
    [Test]
    public async Task WriteReadRoundTripsIndex()
    {
        var actual = WriteAndReadIndex(42);
        _ = await Assert.That(actual).IsEqualTo(42);
        return;

        static int WriteAndReadIndex(int index)
        {
            Span<byte> buffer = stackalloc byte[Pointer.Size];
            Pointer.Write(buffer, index);
            return Pointer.Read(buffer);
        }
    }

    private static void ReadCorruptPointer()
    {
        Span<byte> buffer = stackalloc byte[Pointer.Size];
        Pointer.Write(buffer, 1);
        buffer[^1] ^= 0xFF;
        _ = Pointer.Read(buffer);
    }
}
