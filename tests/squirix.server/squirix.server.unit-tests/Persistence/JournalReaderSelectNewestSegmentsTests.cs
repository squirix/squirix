using System;
using System.Collections.Generic;
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

        var segments = Materialize(JournalReader.EnumerateSegments(dir, 9));
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
        var dir = PathKit.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        var segments = Materialize(JournalReader.EnumerateSegments(dir, 1));
        Assert.Empty(segments);
    }

    /// <summary>EnumerateSegments ignores journal-shaped names whose numeric index does not parse.</summary>
    [Fact]
    public void EnumerateSegmentsSkipsJournalFilesNonNumericIndex()
    {
        using var dir = new TempDirectory("squirix-journal-enum-filter");
        File.WriteAllText(PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}abcdef{StorageFileExtensions.Journal}"), "x");
        File.WriteAllText(PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}{42.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Journal}"), "x");

        var segments = Materialize(JournalReader.EnumerateSegments(dir, 1));

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

    private static T[] Materialize<T>(IEnumerable<T> source)
    {
        var items = new List<T>();
        foreach (var item in source)
            items.Add(item);

        return items.ToArray();
    }
}
