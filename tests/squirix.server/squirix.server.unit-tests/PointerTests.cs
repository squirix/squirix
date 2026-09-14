using System;
using System.IO;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>Tests for the fixed-size SQMC CURRENT pointer.</summary>
[Immutable]
public sealed class PointerTests
{
    /// <summary>Verifies corrupted CRC bytes are rejected on read.</summary>
    [Fact]
    public void ReadThrowsWhenCrcIsInvalid() => _ = NodeExceptionAssert.For<InvalidDataException>().Throws(0, static _ => ReadCorruptPointer());

    /// <summary>Write emits the documented golden bytes: magic, index, then CRC over both.</summary>
    [Fact]
    public void WriteMatchesGoldenWireBytes()
    {
        Span<byte> buffer = stackalloc byte[Pointer.Size];
        Pointer.Write(buffer, 42);

        // "SQMC", index 42 little-endian, CRC32C over the first 8 bytes.
        // The CRC is pinned by an independent computation, not by Crc32C itself:
        // recomputing it here with production code would mask a broken checksum.
        byte[] golden = [0x53, 0x51, 0x4D, 0x43, 0x2A, 0x00, 0x00, 0x00, 0x5D, 0xB7, 0x56, 0x95];
        Assert.True(golden.AsSpan().SequenceEqual(buffer));
    }

    /// <summary>Read decodes the golden bytes back into the manifest index.</summary>
    [Fact]
    public void ReadReadsGoldenWireBytes()
    {
        byte[] golden = [0x53, 0x51, 0x4D, 0x43, 0x2A, 0x00, 0x00, 0x00, 0x5D, 0xB7, 0x56, 0x95];

        Assert.Equal(42, Pointer.Read(golden));
    }

    /// <summary>Verifies write/read round-trip for a manifest index.</summary>
    [Fact]
    public void WriteReadRoundTripsIndex()
    {
        Span<byte> buffer = stackalloc byte[Pointer.Size];
        Pointer.Write(buffer, 42);
        Assert.Equal(42, Pointer.Read(buffer));
    }

    private static void ReadCorruptPointer()
    {
        Span<byte> buffer = stackalloc byte[Pointer.Size];
        Pointer.Write(buffer, 1);
        buffer[^1] ^= 0xFF;
        _ = Pointer.Read(buffer);
    }
}
