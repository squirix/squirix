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
