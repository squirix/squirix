using System.IO;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Tests for bounded journal segment selection used by diagnostics.</summary>
[Immutable]
public sealed class JournalReaderSelectNewestSegmentsTests : IsolatedStorageTestBase
{
    /// <summary>EnumerateSegments returns empty for invalid operator paths without throwing.</summary>
    /// <param name="path">Invalid directory path.</param>
    [Theory]
    [InlineData("..")]
    [InlineData("a*b")]
    [InlineData("")]
    public static void EnumerateSegmentsReturnsEmptyForInvalidPaths(string path)
    {
        var segments = JournalReader.EnumerateSegments(path, 1);
        Assert.Empty(segments);
    }

    /// <summary>EnumerateSegments returns sorted indices and respects the requested start segment.</summary>
    [Fact]
    public void EnumerateSegmentsRespectsSegmentAndSortsAscending()
    {
        File.WriteAllText(NodePathKit.Combine(Dir, $"{FilePrefixes.Journal}{NodeInvariantIndexStrings.FormatD6(9)}{FileExtensions.Journal}"), "x");
        File.WriteAllText(NodePathKit.Combine(Dir, $"{FilePrefixes.Journal}{NodeInvariantIndexStrings.FormatD6(2)}{FileExtensions.Journal}"), "x");
        File.WriteAllText(NodePathKit.Combine(Dir, $"{FilePrefixes.Journal}{NodeInvariantIndexStrings.FormatD6(15)}{FileExtensions.Journal}"), "x");

        var segments = JournalReader.EnumerateSegments(Dir, 9);
        Assert.Equal(2, segments.Length);
        Assert.Equal(9, segments[0].Index);
        Assert.Equal(15, segments[1].Index);
    }

    /// <summary>EnumerateSegments returns empty when journal directory does not exist.</summary>
    [Fact]
    public void EnumerateSegmentsReturnsEmptyWhenDirectoryMissing()
    {
        var dir = NodePathKit.Combine(NodePathKit.GetProcTempPath("squirix-journal-enum"), "missing-directory");
        var segments = JournalReader.EnumerateSegments(dir, 1);
        Assert.Empty(segments);
    }

    /// <summary>EnumerateSegments ignores journal-shaped names whose numeric index does not parse.</summary>
    [Fact]
    public void EnumerateSegmentsSkipsJournalFilesNonNumericIndex()
    {
        File.WriteAllText(NodePathKit.Combine(Dir, $"{FilePrefixes.Journal}abcdef{FileExtensions.Journal}"), "x");
        File.WriteAllText(NodePathKit.Combine(Dir, $"{FilePrefixes.Journal}{NodeInvariantIndexStrings.FormatD6(42)}{FileExtensions.Journal}"), "x");
        var segments = JournalReader.EnumerateSegments(Dir, 1);
        var seg = Assert.Single(segments);
        Assert.Equal(42, seg.Index);
    }

    /// <summary>GetOnDiskSegmentStats returns zeros for invalid operator paths.</summary>
    [Fact]
    public void GetOnDiskSegmentStatsReturnsDefaultForInvalidPath()
    {
        var (segmentCount, totalBytes) = JournalReader.GetOnDiskSegmentStats("..");
        Assert.Equal(0, segmentCount);
        Assert.Equal(0, totalBytes);
    }
}
