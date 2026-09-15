using System;
using System.Threading.Tasks;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Covers SHA-256 uppercase hex formatting.</summary>
public sealed class HexFormatTests : ServerUnitTestBase
{
    /// <summary>Rejects digests that are not exactly 32 bytes.</summary>
    [Test]
    public async Task Sha256HexUpperRejectsBadDigestLength()
    {
        var paramName = CaptureParamName();
        _ = await Assert.That(paramName).IsEqualTo("digest");
        return;

        static string? CaptureParamName()
        {
            try
            {
                Span<byte> digest = stackalloc byte[16];
                Span<char> destination = stackalloc char[64];
                HexFormat.WriteSha256HexUpper(destination, digest);
                Assert.Fail("Expected ArgumentException.");
                return null;
            }
            catch (ArgumentException ex)
            {
                return ex.ParamName;
            }
        }
    }

    /// <summary>Rejects destinations shorter than 64 characters.</summary>
    [Test]
    public async Task Sha256HexUpperRejectsShortDestination()
    {
        var paramName = CaptureParamName();
        _ = await Assert.That(paramName).IsEqualTo("destination");
        return;

        static string? CaptureParamName()
        {
            try
            {
                Span<byte> digest = stackalloc byte[32];
                Span<char> destination = stackalloc char[32];
                HexFormat.WriteSha256HexUpper(destination, digest);
                Assert.Fail("Expected ArgumentException.");
                return null;
            }
            catch (ArgumentException ex)
            {
                return ex.ParamName;
            }
        }
    }

    /// <summary>Writes 64 uppercase hex characters for a 32-byte digest.</summary>
    [Test]
    public async Task WriteSha256HexUpperFormatsDigest()
    {
        var (actual, expected) = FormatDigest();
        _ = await Assert.That(actual).IsEqualTo(expected);
        return;

        static (string Actual, string Expected) FormatDigest()
        {
            Span<byte> digest = stackalloc byte[32];
            digest.Fill(0xAB);

            Span<char> destination = stackalloc char[64];
            HexFormat.WriteSha256HexUpper(destination, digest);
            return (new string(destination), Convert.ToHexString(digest));
        }
    }
}
