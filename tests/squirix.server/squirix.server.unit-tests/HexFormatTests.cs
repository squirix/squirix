using System;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>Covers SHA-256 uppercase hex formatting.</summary>
public sealed class HexFormatTests : ServerUnitTestBase
{
    /// <summary>Writes 64 uppercase hex characters for a 32-byte digest.</summary>
    [Fact]
    public void WriteSha256HexUpperFormatsDigest()
    {
        Span<byte> digest = stackalloc byte[32];
        digest.Fill(0xAB);

        Span<char> destination = stackalloc char[64];
        HexFormat.WriteSha256HexUpper(destination, digest);
        Assert.Equal(Convert.ToHexString(digest), new string(destination));
    }

    /// <summary>Rejects digests that are not exactly 32 bytes.</summary>
    [Fact]
    public void Sha256HexUpperRejectsBadDigestLength()
    {
        try
        {
            Span<byte> digest = stackalloc byte[16];
            Span<char> destination = stackalloc char[64];
            HexFormat.WriteSha256HexUpper(destination, digest);
            Assert.Fail("Expected ArgumentException.");
        }
        catch (ArgumentException ex)
        {
            Assert.Equal("digest", ex.ParamName);
        }
    }

    /// <summary>Rejects destinations shorter than 64 characters.</summary>
    [Fact]
    public void Sha256HexUpperRejectsShortDestination()
    {
        try
        {
            Span<byte> digest = stackalloc byte[32];
            Span<char> destination = stackalloc char[32];
            HexFormat.WriteSha256HexUpper(destination, digest);
            Assert.Fail("Expected ArgumentException.");
        }
        catch (ArgumentException ex)
        {
            Assert.Equal("destination", ex.ParamName);
        }
    }
}
