using System;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Tests for the single-pass on-disk journal segment statistics used by the roll capacity check.</summary>
public sealed class JournalReaderSegmentStatsTests : UnitTestBase
{
    /// <summary>GetOnDiskSegmentStats counts journal segments and sums their byte lengths in one pass.</summary>
    [Fact]
    public async Task GetOnDiskSegmentStatsCountsSegmentsAndSumsBytes()
    {
        using var dir = new TempDirectory("squirix-journal-stats");
        await WriteSegmentAsync(dir, 1, 10);
        await WriteSegmentAsync(dir, 2, 25);
        await WriteSegmentAsync(dir, 3, 7);
        await File.WriteAllTextAsync(PathKit.Combine(dir, "not-a-journal.txt"), "ignored", DefaultCancellationToken);

        var stats = JournalReader.GetOnDiskSegmentStats(dir);

        Assert.Equal(3, stats.SegmentCount);
        Assert.Equal(42, stats.TotalBytes);
        Assert.Equal(stats.TotalBytes, JournalReader.GetOnDiskTotalBytes(dir));
    }

    /// <summary>GetOnDiskSegmentStats returns an empty result when the directory does not exist.</summary>
    [Fact]
    public void GetOnDiskSegmentStatsReturnsEmptyWhenDirectoryMissing()
    {
        var dir = PathKit.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));

        var stats = JournalReader.GetOnDiskSegmentStats(dir);

        Assert.Equal(0, stats.SegmentCount);
        Assert.Equal(0L, stats.TotalBytes);
    }

    private static Task WriteSegmentAsync(string dir, int index, int byteCount) => File.WriteAllBytesAsync(
        PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}{index.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Journal}"),
        new byte[byteCount],
        DefaultCancellationToken);
}
