using System;
using System.Globalization;
using System.IO;
using System.Linq;
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
    public void EnumerateSegmentsRespectsFromSegmentAndSortsAscending()
    {
        using var dir = new TempDirectory("squirix-journal-enum-from");
        File.WriteAllText(PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}{9.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Journal}"), "x");
        File.WriteAllText(PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}{2.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Journal}"), "x");
        File.WriteAllText(PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}{15.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Journal}"), "x");

        var segments = JournalReader.EnumerateSegments(dir, 9).ToArray();
        Assert.Equal(2, segments.Length);
        Assert.Equal(9, segments[0].Index);
        Assert.Equal(15, segments[1].Index);
    }

    /// <summary>EnumerateSegments returns empty when journal directory does not exist.</summary>
    [Fact]
    public void EnumerateSegmentsReturnsEmptyWhenDirectoryMissing()
    {
        var dir = PathKit.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        var segments = JournalReader.EnumerateSegments(dir, 1).ToArray();
        Assert.Empty(segments);
    }

    /// <summary>EnumerateSegments ignores journal-shaped names whose numeric index does not parse.</summary>
    [Fact]
    public void EnumerateSegmentsSkipsJournalFilesWithNonNumericIndex()
    {
        using var dir = new TempDirectory("squirix-journal-enum-filter");
        File.WriteAllText(PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}abcdef{StorageFileExtensions.Journal}"), "x");
        File.WriteAllText(PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}{42.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Journal}"), "x");

        var segments = JournalReader.EnumerateSegments(dir, 1).ToArray();

        var seg = Assert.Single(segments);
        Assert.Equal(42, seg.Index);
    }

    /// <summary>Verifies only the newest segments are retained when many exist on disk.</summary>
    [Fact]
    public void SelectNewestSegmentsKeepsOnlyNewestByIndex()
    {
        using var dir = new TempDirectory("squirix-journal-select");
        for (var i = 1; i <= 40; i++)
        {
            var path = PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}{i.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Journal}");
            File.WriteAllText(path, "x");
        }

        var selected = JournalReader.SelectNewestSegments(dir, 1, 16);
        Assert.Equal(16, selected.Length);
        Assert.Equal(40, selected[0].Index);
        Assert.Equal(25, selected[15].Index);
    }
}
