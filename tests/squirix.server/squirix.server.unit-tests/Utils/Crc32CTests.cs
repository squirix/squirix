using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using Xunit;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Golden vectors for the software CRC32C (Castagnoli) implementation.</summary>
[Immutable]
public sealed class Crc32CTests : ServerUnitTestBase
{
    /// <summary>Empty input yields the canonical zero checksum.</summary>
    [Fact]
    public void EmptyInputYieldsZero() => Assert.Equal(0u, Crc32C.Compute([]));

    /// <summary>Canonical check value for the ASCII string 123456789 (RFC 3720, Appendix B.4).</summary>
    [Fact]
    public void CheckValueForDigits() => Assert.Equal(0xE3069283u, Crc32C.Compute("123456789"u8));

    /// <summary>Canonical vector for 32 zero bytes (RFC 3720, Appendix B.4).</summary>
    [Fact]
    public void CheckValueForZeroBlock() => Assert.Equal(0x8A9136AAu, Crc32C.Compute(new byte[32]));

    /// <summary>Incremental append over two chunks matches the canonical check value.</summary>
    [Fact]
    public void AppendMatchesCompute()
    {
        var data = "123456789"u8;

        Assert.Equal(0xE3069283u, Crc32C.Finalize(Crc32C.Append(Crc32C.Append(Crc32C.InitialValue, data[..4]), data[4..])));
    }
}
