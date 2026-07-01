using System;
using System.Globalization;
using System.IO;
using System.Threading;
using Squirix.Server.Storage.Journaling.Limits;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.Read;

/// <summary>Journal segment enumeration and replay.</summary>
internal static class JournalReadPath
{
    internal static string BuildSegmentPath(string dataDir, int segmentIndex) => PathEx.Combine(
        dataDir,
        $"{StorageFilePrefixes.Journal}{segmentIndex.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Journal}");

    /// <summary>Throws when rolling the active segment would exceed journal capacity limits.</summary>
    /// <param name="dataDir">Persistence directory containing journal segment files.</param>
    /// <param name="policy">Configured segment limits.</param>
    internal static void EnsureSegmentRollCapacityOrThrow(string dataDir, JournalSegmentPolicy policy)
    {
        ArgumentException.ThrowIfNullOrEmpty(dataDir);
        ArgumentNullException.ThrowIfNull(policy);

        var onDiskCount = JournalReader.SelectNewestSegments(dataDir, 1, policy.SegmentCountProbeLimit).Count;
        policy.EnsureRollCapacityOrThrow(onDiskCount, JournalReader.GetOnDiskTotalBytes(dataDir));
    }

    internal static JournalSegment[] EnumerateSegments(string dataDir, int fromSegment) => JournalReader.EnumerateSegments(dataDir, fromSegment);

    internal static JournalReplaySequence ReadAll(string dataDir, int fromSegment, CancellationToken cancellationToken) => new(dataDir, fromSegment, cancellationToken);

    internal static InvalidDataException CreateSegmentReadFailure(string path, bool tolerateTruncatedTail, Exception ex) => new(
        $"failed reading journal segment '{path}' (tolerateTruncatedTail={tolerateTruncatedTail}): {ex.Message}",
        ex);
}
