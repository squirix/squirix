using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Tests for the single-pass on-disk journal segment statistics used by the roll capacity check.</summary>
[Immutable]
public sealed class JournalReaderSegmentStatsTests : IsolatedStorageTestBase
{
    /// <summary>GetOnDiskSegmentStats counts journal segments and sums their byte lengths in one pass.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SegmentStatsCountsAndSumsBytes(CancellationToken cancellationToken)
    {
        await WriteSegmentAsync(Dir, 1, 10, cancellationToken);
        await WriteSegmentAsync(Dir, 2, 25, cancellationToken);
        await WriteSegmentAsync(Dir, 3, 7, cancellationToken);
        await File.WriteAllTextAsync(NodePathKit.Combine(Dir, "not-a-journal.txt"), "ignored", cancellationToken);

        var (segmentCount, totalBytes) = JournalReader.GetOnDiskSegmentStats(Dir);

        _ = await Assert.That(segmentCount).IsEqualTo(3);
        _ = await Assert.That(totalBytes).IsEqualTo(42);
        _ = await Assert.That(JournalReader.GetOnDiskSegmentStats(Dir).TotalBytes).IsEqualTo(totalBytes);
    }

    /// <summary>GetOnDiskSegmentStats returns an empty result when the directory does not exist.</summary>
    [Test]
    public async Task SegmentStatsEmptyForMissingDir()
    {
        var dir = NodePathKit.Combine(NodePathKit.GetProcTempPath("squirix-journal-stats"), "missing-directory");

        var (segmentCount, totalBytes) = JournalReader.GetOnDiskSegmentStats(dir);

        _ = await Assert.That(segmentCount).IsEqualTo(0);
        _ = await Assert.That(totalBytes).IsEqualTo(0L);
    }

    /// <summary>Writes a journal segment file with the given byte count.</summary>
    /// <param name="dir">The journal directory.</param>
    /// <param name="index">The segment index.</param>
    /// <param name="byteCount">The byte count.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    private static Task WriteSegmentAsync(string dir, int index, int byteCount, CancellationToken cancellationToken) => File.WriteAllBytesAsync(
        NodePathKit.Combine(dir, $"{FilePrefixes.Journal}{NodeInvariantIndexStrings.FormatD6(index)}{FileExtensions.Journal}"),
        new byte[byteCount],
        cancellationToken);
}
