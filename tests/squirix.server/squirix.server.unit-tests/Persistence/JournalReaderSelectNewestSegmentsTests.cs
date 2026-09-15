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

/// <summary>Tests for bounded journal segment selection used by diagnostics.</summary>
[Immutable]
public sealed class JournalReaderSelectNewestSegmentsTests : IsolatedStorageTestBase
{
    /// <summary>EnumerateSegments returns empty for invalid operator paths without throwing.</summary>
    /// <param name="path">Invalid directory path.</param>
    [Test]
    [Arguments("..")]
    [Arguments("a*b")]
    [Arguments("")]
    public async Task EnumerateSegmentsEmptyOnBadPath(string path)
    {
        var segments = JournalReader.EnumerateSegments(path, 1);
        _ = await Assert.That(segments).IsEmpty();
    }

    /// <summary>EnumerateSegments returns empty when journal directory does not exist.</summary>
    [Test]
    public async Task EnumerateSegmentsEmptyOnMissingDir()
    {
        var dir = NodePathKit.Combine(NodePathKit.GetProcTempPath("squirix-journal-enum"), "missing-directory");
        var segments = JournalReader.EnumerateSegments(dir, 1);
        _ = await Assert.That(segments).IsEmpty();
    }

    /// <summary>EnumerateSegments ignores journal-shaped names whose numeric index does not parse.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EnumerateSegmentsSkipsNonNumericNames(CancellationToken cancellationToken)
    {
        await File.WriteAllTextAsync(NodePathKit.Combine(Dir, $"{FilePrefixes.Journal}abcdef{FileExtensions.Journal}"), "x", cancellationToken);
        await File.WriteAllTextAsync(NodePathKit.Combine(Dir, $"{FilePrefixes.Journal}{NodeInvariantIndexStrings.FormatD6(42)}{FileExtensions.Journal}"), "x", cancellationToken);
        var segments = JournalReader.EnumerateSegments(Dir, 1);
        var seg = await Assert.That(segments).HasSingleItem();
        _ = await Assert.That(seg.Index).IsEqualTo(42);
    }

    /// <summary>EnumerateSegments returns sorted indices and respects the requested start segment.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EnumerateSegmentsSortedAscending(CancellationToken cancellationToken)
    {
        await File.WriteAllTextAsync(NodePathKit.Combine(Dir, $"{FilePrefixes.Journal}{NodeInvariantIndexStrings.FormatD6(9)}{FileExtensions.Journal}"), "x", cancellationToken);
        await File.WriteAllTextAsync(NodePathKit.Combine(Dir, $"{FilePrefixes.Journal}{NodeInvariantIndexStrings.FormatD6(2)}{FileExtensions.Journal}"), "x", cancellationToken);
        await File.WriteAllTextAsync(NodePathKit.Combine(Dir, $"{FilePrefixes.Journal}{NodeInvariantIndexStrings.FormatD6(15)}{FileExtensions.Journal}"), "x", cancellationToken);

        var segments = JournalReader.EnumerateSegments(Dir, 9);
        _ = await Assert.That(segments.Length).IsEqualTo(2);
        _ = await Assert.That(segments[0].Index).IsEqualTo(9);
        _ = await Assert.That(segments[1].Index).IsEqualTo(15);
    }

    /// <summary>GetOnDiskSegmentStats returns zeros for invalid operator paths.</summary>
    [Test]
    public async Task SegmentStatsDefaultsOnBadPath()
    {
        var (segmentCount, totalBytes) = JournalReader.GetOnDiskSegmentStats("..");
        _ = await Assert.That(segmentCount).IsEqualTo(0);
        _ = await Assert.That(totalBytes).IsEqualTo(0);
    }
}
