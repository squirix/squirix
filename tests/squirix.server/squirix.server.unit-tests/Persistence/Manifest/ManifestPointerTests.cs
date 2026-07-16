using System;
using System.IO;
using Squirix.Server.Storage.Manifest;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>Tests for the fixed-size SQMC CURRENT pointer.</summary>
public sealed class ManifestPointerTests
{
    /// <summary>Verifies write/read round-trip for a manifest index.</summary>
    [Fact]
    public void WriteReadRoundTripsIndex()
    {
        Span<byte> buffer = stackalloc byte[Pointer.Size];
        Pointer.Write(buffer, 42);
        Assert.Equal(42, Pointer.Read(buffer));
    }

    /// <summary>Verifies corrupted CRC bytes are rejected on read.</summary>
    [Fact]
    public void ReadThrowsWhenCrcIsInvalid()
    {
        _ = Assert.Throws<InvalidDataException>(static () =>
        {
            Span<byte> buffer = stackalloc byte[Pointer.Size];
            Pointer.Write(buffer, 1);
            buffer[^1] ^= 0xFF;
            _ = Pointer.Read(buffer);
        });
    }
}
