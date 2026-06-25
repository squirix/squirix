using System.Globalization;
using System.IO;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Tests for bounded journal segment selection used by diagnostics.</summary>
public sealed class JournalReaderSelectNewestSegmentsTests
{
    /// <summary>EnumerateSegments returns sorted indices and respects the requested start segment.</summary>
    [Fact]
    public void EnumerateSegmentsRespectsSegmentAndSortsAscending()
    {
        using var dir = new TempDirectory("squirix-journal-enum-from");
        File.WriteAllText(NodePathKit.Combine(dir, $"{FilePrefixes.Journal}{InvariantIndexStrings.FormatD6(9)}{FileExtensions.Journal}"), "x");
        File.WriteAllText(NodePathKit.Combine(dir, $"{FilePrefixes.Journal}{InvariantIndexStrings.FormatD6(2)}{FileExtensions.Journal}"), "x");
        File.WriteAllText(NodePathKit.Combine(dir, $"{FilePrefixes.Journal}{InvariantIndexStrings.FormatD6(15)}{FileExtensions.Journal}"), "x");

        var segments = JournalReader.EnumerateSegments(dir, 9);
        Assert.Equal(2, segments.Length);
        Assert.Equal(9, segments[0].Index);
        Assert.Equal(15, segments[1].Index);
    }

    /// <summary>EnumerateSegments returns empty for invalid operator paths without throwing.</summary>
    /// <param name="path">Invalid directory path.</param>
    [Theory]
    [InlineData("..")]
    [InlineData("a*b")]
    [InlineData("")]
    public void EnumerateSegmentsReturnsEmptyForInvalidPaths(string path)
    {
        var segments = JournalReader.EnumerateSegments(path, 1);
        Assert.Empty(segments);
    }

    /// <summary>EnumerateSegments returns empty when journal directory does not exist.</summary>
    [Fact]
    public void EnumerateSegmentsReturnsEmptyWhenDirectoryMissing()
    {
        var dir = PathKit.Combine(PathKit.GetProcTempPath("squirix-journal-enum"), "missing-directory");
        var segments = JournalReader.EnumerateSegments(dir, 1);
        Assert.Empty(segments);
    }

    /// <summary>EnumerateSegments ignores journal-shaped names whose numeric index does not parse.</summary>
    [Fact]
    public void EnumerateSegmentsSkipsJournalFilesNonNumericIndex()
    {
        using var dir = new TempDirectory("squirix-journal-enum-filter");
        File.WriteAllText(PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}abcdef{StorageFileExtensions.Journal}"), "x");
        File.WriteAllText(PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}{42.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Journal}"), "x");
        var segments = JournalReader.EnumerateSegments(dir, 1);
        var seg = Assert.Single(segments);
        Assert.Equal(42, seg.Index);
    }

    /// <summary>GetOnDiskSegmentStats returns zeros for invalid operator paths.</summary>
    [Fact]
    public void GetOnDiskSegmentStatsReturnsDefaultForInvalidPath()
    {
        using var dir = new TempDirectory("squirix-journal-select");
        for (var i = 1; i <= 40; i++)
        {
            var path = PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}{i.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Journal}");
            File.WriteAllText(path, "x");
        }

        var selected = JournalReader.SelectNewestSegments(dir, 1, 16);
        Assert.Equal(16, selected.Count);

        var indices = new int[selected.Count];
        for (var i = 0; i < indices.Length; i++)
            indices[i] = selected.Dequeue().Index;

        Assert.Equal(25, indices[0]);
        Assert.Equal(40, indices[^1]);
    }
}
