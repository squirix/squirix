using System;
using System.IO;
using System.Text;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>Covers manifest codec edge cases introduced with shared error literals.</summary>
public sealed class FileCodecTests : ServerUnitTestBase
{
    /// <summary>Rejects snapshot paths whose UTF-8 length exceeds the encoded ushort limit.</summary>
    [Fact]
    public void ComputeEncodedLengthRejectsOversizedSnapshotPath()
    {
        var ex = Assert.Throws<InvalidDataException>(static () =>
        {
            var manifest = new State
            {
                Format = 1,
                CurrentJournal = 1,
                NextSequence = 1,
                LastSnapshot = new SnapshotRef
                {
                    Path = new string('a', ushort.MaxValue + 1),
                    Index = 1,
                    LastAppliedSequence = 1,
                    CreatedUtc = DateTime.UtcNow,
                },
            };
            return FileCodec.ComputeEncodedLength(manifest);
        });
        Assert.Contains("maximum encoded length", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Rejects oversized UTF-8 path lengths for roll encoding.</summary>
    [Fact]
    public void ComputeRollEncodedLengthRejectsOversizedPathLength()
    {
        var ex = Assert.Throws<InvalidDataException>(static () => FileCodec.ComputeRollEncodedLength(new SnapshotRef { Path = "x" }, ushort.MaxValue + 1));
        Assert.Contains("maximum encoded length", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>WriteRollEncoded rejects oversized snapshot path payloads.</summary>
    [Fact]
    public void WriteRollEncodedRejectsOversizedSnapshotPathUtf8()
    {
        var bytes = Encoding.UTF8.GetBytes(new string('b', ushort.MaxValue + 1));
        var ex = Assert.Throws<InvalidDataException>(() => FileCodec.WriteRollEncoded(1, 1, 1, new SnapshotRef { Path = "x" }, bytes, []));
        Assert.Contains("maximum encoded length", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
