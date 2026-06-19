using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.PipelinedWal.Read;

/// <summary>Backend-neutral journal segment enumeration and replay.</summary>
internal static class JournalReadPath
{
    public static IEnumerable<JournalSegment> EnumerateSegments(string dataDir, int fromSegment) =>
        JournalReader.EnumerateSegments(dataDir, fromSegment);

    public static IEnumerable<JournalRecord> ReadAll(string dataDir, int fromSegment, CancellationToken cancellationToken)
    {
        var segments = new List<JournalSegment>();
        foreach (var segment in EnumerateSegments(dataDir, fromSegment))
            segments.Add(segment);

        for (var i = 0; i < segments.Count; i++)
        {
            var tolerateTruncatedTail = i == segments.Count - 1;
            foreach (var record in new WalJournalSegmentReader(segments[i].Path, tolerateTruncatedTail, cancellationToken))
                yield return record;
        }
    }

    public static JournalSegment[] SelectNewestSegments(string dataDir, int fromSegment, int maxCount) =>
        JournalReader.SelectNewestSegments(dataDir, fromSegment, maxCount);

    internal static string BuildSegmentPath(string dataDir, int segmentIndex) =>
        PathEx.Combine(dataDir, $"{StorageFilePrefixes.Journal}{segmentIndex.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Journal}");
}
