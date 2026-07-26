using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Tests for the single-pass on-disk journal segment statistics used by the roll capacity check.</summary>
public sealed class JournalReaderSegmentStatsTests : ServerUnitTestBase
{
    /// <summary>GetOnDiskSegmentStats counts journal segments and sums their byte lengths in one pass.</summary>
    [Fact]
    public async Task GetOnDiskSegmentStatsCountsSegmentsAndSumsBytes()
    {
        using var dir = new TempDirectory("squirix-journal-stats");
        await WriteSegmentAsync(dir, 1, 10);
        await WriteSegmentAsync(dir, 2, 25);
        await WriteSegmentAsync(dir, 3, 7);
        await File.WriteAllTextAsync(NodePathKit.Combine(dir, "not-a-journal.txt"), "ignored", DefaultCancellationToken);

        var (segmentCount, totalBytes) = JournalReader.GetOnDiskSegmentStats(dir);

        Assert.Equal(3, segmentCount);
        Assert.Equal(42, totalBytes);
        Assert.Equal(totalBytes, JournalReader.GetOnDiskSegmentStats(dir).TotalBytes);
    }

    /// <summary>GetOnDiskSegmentStats returns an empty result when the directory does not exist.</summary>
    [Fact]
    public void GetOnDiskSegmentStatsReturnsEmptyDirectoryMissing()
    {
        var dir = NodePathKit.Combine(NodePathKit.GetProcTempPath("squirix-journal-stats"), "missing-directory");

        var (segmentCount, totalBytes) = JournalReader.GetOnDiskSegmentStats(dir);

        Assert.Equal(0, segmentCount);
        Assert.Equal(0L, totalBytes);
    }

    private static Task WriteSegmentAsync(string dir, int index, int byteCount) => File.WriteAllBytesAsync(
        NodePathKit.Combine(dir, $"{FilePrefixes.Journal}{InvariantIndexStrings.FormatD6(index)}{FileExtensions.Journal}"),
        new byte[byteCount],
        DefaultCancellationToken);
}
