using System;
using System.IO;
using System.Linq;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Storage;

/// <summary>
/// Tests for bounded journal segment selection used by diagnostics.
/// </summary>
public sealed class JournalReaderSelectNewestSegmentsTests
{
    /// <summary>
    /// EnumerateSegments returns sorted indices and respects the requested start segment.
    /// </summary>
    [Fact]
    public void EnumerateSegmentsRespectsFromSegmentAndSortsAscending()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-journal-enum-from");
        try
        {
            File.WriteAllText(PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}{9:000000}{StorageFileExtensions.Journal}"), "x");
            File.WriteAllText(PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}{2:000000}{StorageFileExtensions.Journal}"), "x");
            File.WriteAllText(PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}{15:000000}{StorageFileExtensions.Journal}"), "x");

            var segments = JournalReader.EnumerateSegments(dir, 9).ToArray();
            Assert.Equal(2, segments.Length);
            Assert.Equal(9, segments[0].Index);
            Assert.Equal(15, segments[1].Index);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// EnumerateSegments returns empty when journal directory does not exist.
    /// </summary>
    [Fact]
    public void EnumerateSegmentsReturnsEmptyWhenDirectoryMissing()
    {
        var dir = PathKit.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        var segments = JournalReader.EnumerateSegments(dir, 1).ToArray();
        Assert.Empty(segments);
    }

    /// <summary>
    /// EnumerateSegments ignores journal-shaped names whose numeric index does not parse.
    /// </summary>
    [Fact]
    public void EnumerateSegmentsSkipsJournalFilesWithNonNumericIndex()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-journal-enum-filter");
        try
        {
            File.WriteAllText(PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}abcdef{StorageFileExtensions.Journal}"), "x");
            File.WriteAllText(PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}{42:000000}{StorageFileExtensions.Journal}"), "x");

            var segments = JournalReader.EnumerateSegments(dir, 1).ToArray();

            var seg = Assert.Single(segments);
            Assert.Equal(42, seg.Index);
        }
        finally
        {
            try
            {
                Directory.Delete(dir, true);
            }
            catch
            {
                /* best-effort cleanup */
            }
        }
    }

    /// <summary>
    /// Verifies only the newest segments are retained when many exist on disk.
    /// </summary>
    [Fact]
    public void SelectNewestSegmentsKeepsOnlyNewestByIndex()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-journal-select");
        try
        {
            for (var i = 1; i <= 40; i++)
            {
                var path = PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}{i:000000}{StorageFileExtensions.Journal}");
                File.WriteAllText(path, "x");
            }

            var selected = JournalReader.SelectNewestSegments(dir, 1, 16);
            Assert.Equal(16, selected.Length);
            Assert.Equal(40, selected[0].Index);
            Assert.Equal(25, selected[15].Index);
        }
        finally
        {
            try
            {
                Directory.Delete(dir, true);
            }
            catch
            {
                /* best-effort cleanup */
            }
        }
    }
}
