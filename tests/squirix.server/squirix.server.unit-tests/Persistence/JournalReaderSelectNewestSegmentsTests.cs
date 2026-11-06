using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Testing;
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
        _ = await Assert.That(JournalReader.EnumerateObsoleteSegmentPaths(path, 10)).IsEmpty();
    }

    /// <summary>EnumerateSegments returns empty when journal directory does not exist.</summary>
    [Test]
    public async Task EnumerateSegmentsEmptyOnMissingDir()
    {
        var dir = PathKit.Combine(PathKit.GetProcTempPath("squirix-journal-enum"), "missing-directory");
        var segments = JournalReader.EnumerateSegments(dir, 1);
        _ = await Assert.That(segments).IsEmpty();
        _ = await Assert.That(JournalReader.EnumerateObsoleteSegmentPaths(dir, 10)).IsEmpty();
    }

    /// <summary>Only positive, parseable segment indexes below the floor are eligible, regardless of name padding or creation order.</summary>
    /// <param name="floor">The first retained index.</param>
    /// <param name="expectedCount">The number of obsolete files.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1, 0)]
    [Arguments(2, 1)]
    [Arguments(10, 3)]
    public async Task ObsoleteSegmentsRespectNumericFloor(int floor, int expectedCount, CancellationToken cancellationToken)
    {
        foreach (var name in new[] { "10", "000002", "000000", "-1", "text", "2147483648", "1", "09" })
            await File.WriteAllTextAsync(Path.Join(Dir, $"{FilePrefixes.Journal}{name}{FileExtensions.Journal}"), "x", cancellationToken);
        _ = Directory.CreateDirectory(Path.Join(Dir, "nested"));
        await File.WriteAllTextAsync(Path.Join(Dir, "nested", $"{FilePrefixes.Journal}1{FileExtensions.Journal}"), "x", cancellationToken);
        await File.WriteAllTextAsync(Path.Join(Dir, "unrelated.log"), "x", cancellationToken);

        var actual = new HashSet<string>(JournalReader.EnumerateObsoleteSegmentPaths(Dir, floor), System.StringComparer.Ordinal);
        var expected = new HashSet<string>(System.StringComparer.Ordinal);
        if (floor > 1)
            _ = expected.Add(Path.Join(Dir, $"{FilePrefixes.Journal}1{FileExtensions.Journal}"));
        if (floor > 2)
            _ = expected.Add(Path.Join(Dir, $"{FilePrefixes.Journal}000002{FileExtensions.Journal}"));
        if (floor > 9)
            _ = expected.Add(Path.Join(Dir, $"{FilePrefixes.Journal}09{FileExtensions.Journal}"));

        _ = await Assert.That(actual.Count).IsEqualTo(expectedCount);
        _ = await Assert.That(actual.SetEquals(expected)).IsTrue();
    }

    /// <summary>EnumerateSegments ignores journal-shaped names whose numeric index does not parse.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EnumerateSegmentsSkipsNonNumericNames(CancellationToken cancellationToken)
    {
        await File.WriteAllTextAsync(PathKit.Combine(Dir, $"{FilePrefixes.Journal}abcdef{FileExtensions.Journal}"), "x", cancellationToken);
        await File.WriteAllTextAsync(PathKit.Combine(Dir, $"{FilePrefixes.Journal}{NodeInvariantIndexStrings.FormatD6(42)}{FileExtensions.Journal}"), "x", cancellationToken);
        var segments = JournalReader.EnumerateSegments(Dir, 1);
        var seg = await Assert.That(segments).HasSingleItem();
        _ = await Assert.That(seg.Index).IsEqualTo(42);
    }

    /// <summary>EnumerateSegments returns sorted indices and respects the requested start segment.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EnumerateSegmentsSortedAscending(CancellationToken cancellationToken)
    {
        await File.WriteAllTextAsync(PathKit.Combine(Dir, $"{FilePrefixes.Journal}{NodeInvariantIndexStrings.FormatD6(9)}{FileExtensions.Journal}"), "x", cancellationToken);
        await File.WriteAllTextAsync(PathKit.Combine(Dir, $"{FilePrefixes.Journal}{NodeInvariantIndexStrings.FormatD6(2)}{FileExtensions.Journal}"), "x", cancellationToken);
        await File.WriteAllTextAsync(PathKit.Combine(Dir, $"{FilePrefixes.Journal}{NodeInvariantIndexStrings.FormatD6(15)}{FileExtensions.Journal}"), "x", cancellationToken);

        var segments = JournalReader.EnumerateSegments(Dir, 9);
        _ = await Assert.That(segments.Length).IsEqualTo(2);
        _ = await Assert.That(segments[0].Index).IsEqualTo(9);
        _ = await Assert.That(segments[1].Index).IsEqualTo(15);

        await File.WriteAllTextAsync(Path.Join(Dir, $"{FilePrefixes.Journal}999999{FileExtensions.Journal}"), "x", cancellationToken);
        await File.WriteAllTextAsync(Path.Join(Dir, $"{FilePrefixes.Journal}1000000{FileExtensions.Journal}"), "x", cancellationToken);

        var all = JournalReader.EnumerateSegments(Dir, 1);
        _ = await Assert.That((all[^2].Index, all[^1].Index)).IsEqualTo((999_999, 1_000_000));
        _ = await Assert.That(JournalReader.GetSegmentRange(Dir)).IsEqualTo((2, 1_000_000));

        var tail = JournalReader.EnumerateSegments(Dir, 999_999);
        _ = await Assert.That(tail.Length).IsEqualTo(2);
        _ = await Assert.That((tail[0].Index, tail[1].Index)).IsEqualTo((999_999, 1_000_000));
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
