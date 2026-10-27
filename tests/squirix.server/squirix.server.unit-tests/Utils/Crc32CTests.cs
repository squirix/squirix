using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Golden vectors for the software CRC32C (Castagnoli) implementation.</summary>
[Immutable]
public sealed class Crc32CTests : ServerUnitTestBase
{
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

    /// <summary>Canonical check value for the ASCII string 123456789 (RFC 3720, Appendix B.4).</summary>
    [Test]
    public async Task CheckValueForDigits() => _ = await Assert.That(Crc32C.Compute("123456789"u8)).IsEqualTo(0xE3069283u);

    /// <summary>Canonical vector for 32 zero bytes (RFC 3720, Appendix B.4).</summary>
    [Test]
    public async Task CheckValueForZeroBlock() => _ = await Assert.That(Crc32C.Compute(new byte[32])).IsEqualTo(0x8A9136AAu);

    /// <summary>Empty input yields the canonical zero checksum.</summary>
    [Test]
    public async Task EmptyInputYieldsZero() => _ = await Assert.That(Crc32C.Compute([])).IsEqualTo(0u);
}
