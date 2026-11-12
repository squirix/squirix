using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Verifies the canonical transfer checksum at the UTF-8 buffer boundary.</summary>
[Immutable]
public sealed class ReplicaStateChecksumTests
{
    /// <summary>The checksum uses the complete UTF-8 name at and above the stack buffer limit.</summary>
    /// <param name="length">The test name length in characters.</param>
    /// <param name="expectedCrc">The checksum of the canonical wire fields.</param>
    [Test]
    [Arguments(128, 0x3D2D8D7Du)]
    [Arguments(129, 0x70F9D13Fu)]
    public async Task ChecksumPreservesUtf8Name(int length, uint expectedCrc)
    {
        var totals = ReplicaStateHarness.TotalsOf(new ReplicaStateEntry(new string('я', length), new byte[] { 107 }, new byte[] { 118 }, 0L));

        _ = await Assert.That((totals.EntryCount, totals.Crc)).IsEqualTo((1UL, expectedCrc));
    }
}
