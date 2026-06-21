using System;
using System.IO;
using Squirix.Server.Storage.Manifest.Binary;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.BinaryManifest;

/// <summary>Tests for the fixed-size binary CURRENT pointer.</summary>
public sealed class BinaryManifestPointerTests
{
    /// <summary>Verifies write/read round-trip for a manifest index.</summary>
    [Fact]
    public void WriteReadRoundTripsIndex()
    {
        Span<byte> buffer = stackalloc byte[BinaryManifestPointer.Size];
        BinaryManifestPointer.Write(buffer, 42);
        Assert.Equal(42, BinaryManifestPointer.Read(buffer));
    }

    /// <summary>Verifies corrupted CRC bytes are rejected on read.</summary>
    [Fact]
    public void ReadThrowsWhenCrcIsInvalid()
    {
        _ = Assert.Throws<InvalidDataException>(() =>
        {
            var buffer = new byte[BinaryManifestPointer.Size];
            BinaryManifestPointer.Write(buffer, 1);
            buffer[^1] ^= 0xFF;
            _ = BinaryManifestPointer.Read(buffer);
        });
    }
}
