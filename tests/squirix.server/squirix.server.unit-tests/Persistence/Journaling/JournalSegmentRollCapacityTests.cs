using System;
using System.Globalization;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Limits;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Tests for Pipelined journal segment roll capacity enforcement.</summary>
public sealed class JournalSegmentRollCapacityTests
{
    private const int OneMegabyte = 1024 * 1024;

    /// <summary>Total byte cap rejects an append that would exceed configured journal size.</summary>
    [Fact]
    public void EnsureAppendCapacityOrThrowThrowsWhenAppendExceedsTotalByteCap()
    {
        var policy = new JournalSegmentPolicy(new PersistenceOptions { JournalMaxTotalBytesMb = 1 });
        var error = Assert.Throws<JournalCapacityExceededException>(() => policy.EnsureAppendCapacityOrThrow(OneMegabyte, 1));
        Assert.Contains("total bytes", error.Message, StringComparison.Ordinal);
    }

    /// <summary>Allows a roll when the on-disk segment count is below the configured cap.</summary>
    [Fact]
    public void EnsureSegmentRollCapacityOrThrowAllowsRollWhenUnderLimit()
    {
        using var dir = new TempDirectory("squirix-journal-roll-cap-ok");
        var dataDir = dir.Path;
        for (var i = 1; i <= 3; i++)
            CreateSegmentFile(dataDir, i);

        var policy = new JournalSegmentPolicy(new PersistenceOptions { JournalMaxSegmentCount = 4 });
        JournalReadPath.EnsureSegmentRollCapacityOrThrow(dataDir, policy);
        Assert.Equal(3, JournalReadPath.SelectNewestSegments(dataDir, 1, 16).Length);
    }

    /// <summary>Throws when rolling would exceed the configured segment count cap.</summary>
    [Fact]
    public void EnsureSegmentRollCapacityOrThrowThrowsWhenAtSegmentLimit()
    {
        using var dir = new TempDirectory("squirix-journal-roll-cap-block");
        var dataDir = dir.Path;
        for (var i = 1; i <= 4; i++)
            CreateSegmentFile(dataDir, i);

        var policy = new JournalSegmentPolicy(new PersistenceOptions { JournalMaxSegmentCount = 4 });
        var error = Assert.Throws<JournalCapacityExceededException>(() => JournalReadPath.EnsureSegmentRollCapacityOrThrow(dataDir, policy));
        Assert.Contains("segment count", error.Message, StringComparison.Ordinal);
    }

    /// <summary>Throws when rolling would exceed the configured total byte cap even under the segment count cap.</summary>
    [Fact]
    public void EnsureSegmentRollCapacityOrThrowThrowsWhenTotalBytesExceeded()
    {
        using var dir = new TempDirectory("squirix-journal-roll-total-cap");
        var dataDir = dir.Path;
        CreateSegmentFile(dataDir, 1, OneMegabyte + 1);

        var policy = new JournalSegmentPolicy(new PersistenceOptions { JournalMaxSegmentCount = 32, JournalMaxTotalBytesMb = 1 });
        var error = Assert.Throws<JournalCapacityExceededException>(() => JournalReadPath.EnsureSegmentRollCapacityOrThrow(dataDir, policy));
        Assert.Contains("total bytes", error.Message, StringComparison.Ordinal);
    }

    /// <summary>Per-segment byte cap triggers roll before the next frame would overflow the active segment.</summary>
    [Fact]
    public void ShouldRollSegmentWhenIncomingFrameExceedsSegmentByteCap()
    {
        var policy = new JournalSegmentPolicy(new PersistenceOptions { JournalMaxSegmentMb = 1 });
        Assert.True(policy.ShouldRollSegment(OneMegabyte, 1));
    }

    private static void CreateSegmentFile(string dir, int index, int contentLength = 1) => FileKit.WriteAllText(
        PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}{index.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Journal}"),
        new string('x', contentLength));
}
