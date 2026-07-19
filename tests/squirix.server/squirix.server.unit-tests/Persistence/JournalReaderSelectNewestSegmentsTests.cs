using System.Globalization;
using System.IO;
using Squirix.Server.Storage.Journaling.Abstractions;
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
        File.WriteAllText(NodePathKit.Combine(dir, $"{FilePrefixes.Journal}{9.ToString("000000", CultureInfo.InvariantCulture)}{FileExtensions.Journal}"), "x");
        File.WriteAllText(NodePathKit.Combine(dir, $"{FilePrefixes.Journal}{2.ToString("000000", CultureInfo.InvariantCulture)}{FileExtensions.Journal}"), "x");
        File.WriteAllText(NodePathKit.Combine(dir, $"{FilePrefixes.Journal}{15.ToString("000000", CultureInfo.InvariantCulture)}{FileExtensions.Journal}"), "x");

        var segments = JournalReader.EnumerateSegments(dir, 9);
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
    public void EnumerateSegmentsSkipsJournalFilesWithNonNumericIndex()
    {
        using var dir = new TempDirectory("squirix-journal-enum-filter");
        File.WriteAllText(NodePathKit.Combine(dir, $"{FilePrefixes.Journal}abcdef{FileExtensions.Journal}"), "x");
        File.WriteAllText(NodePathKit.Combine(dir, $"{FilePrefixes.Journal}{42.ToString("000000", CultureInfo.InvariantCulture)}{FileExtensions.Journal}"), "x");
        var segments = JournalReader.EnumerateSegments(dir, 1);
        var seg = Assert.Single(segments);
        Assert.Equal(42, seg.Index);
    }
}
