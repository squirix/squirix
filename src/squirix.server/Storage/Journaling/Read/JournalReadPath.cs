using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using JetBrains.Annotations;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.JsonFramed;
using Squirix.Server.Storage.Journaling.Limits;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.Read;

/// <summary>Backend-neutral journal segment enumeration and replay.</summary>
internal static class JournalReadPath
{
    internal static string BuildSegmentPath(string dataDir, int segmentIndex) => PathEx.Combine(
        dataDir,
        $"{StorageFilePrefixes.Journal}{segmentIndex.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Journal}");

    /// <summary>Throws when rolling the active segment would exceed Pipelined journal capacity limits.</summary>
    /// <param name="dataDir">Persistence directory containing journal segment files.</param>
    /// <param name="policy">Configured Pipelined segment limits.</param>
    internal static void EnsureSegmentRollCapacityOrThrow(string dataDir, JournalSegmentPolicy policy)
    {
        ArgumentException.ThrowIfNullOrEmpty(dataDir);
        ArgumentNullException.ThrowIfNull(policy);

        var onDiskCount = SelectNewestSegments(dataDir, 1, policy.SegmentCountProbeLimit).Length;
        policy.EnsureRollCapacityOrThrow(onDiskCount, JournalReader.GetOnDiskTotalBytes(dataDir));
    }

    internal static IEnumerable<JournalSegment> EnumerateSegments(string dataDir, int fromSegment) => JournalReader.EnumerateSegments(dataDir, fromSegment);

    internal static IEnumerable<JournalRecord> ReadAll(string dataDir, int fromSegment, CancellationToken cancellationToken)
    {
        var segments = new List<JournalSegment>();
        foreach (var segment in EnumerateSegments(dataDir, fromSegment))
            segments.Add(segment);

        for (var i = 0; i < segments.Count; i++)
        {
            var tolerateTruncatedTail = i == segments.Count - 1;
            var reader = JournalSegmentReaderFactory.Open(segments[i].Path, tolerateTruncatedTail, cancellationToken);
            foreach (var record in ReadSegmentRecords(reader))
                yield return record;
        }
    }

    internal static JournalSegment[] SelectNewestSegments(string dataDir, int fromSegment, int maxCount) => JournalReader.SelectNewestSegments(dataDir, fromSegment, maxCount);

    [MustDisposeResource]
    private static IEnumerator<JournalRecord> CreateSegmentEnumerator(IJournalSegmentReader reader)
    {
        try
        {
            return reader.GetEnumerator();
        }
        catch (Exception ex) when (ex is InvalidDataException or IOException)
        {
            throw CreateSegmentReadFailure(reader, ex);
        }
    }

    private static InvalidDataException CreateSegmentReadFailure(IJournalSegmentReader reader, Exception ex) =>
        new(
            $"failed reading journal segment '{reader.Path}' (tolerateTruncatedTail={reader.TolerateTruncatedTail}): {ex.Message}",
            ex);

    private static bool MoveNextSegmentRecord(IEnumerator<JournalRecord> enumerator, IJournalSegmentReader reader)
    {
        try
        {
            return enumerator.MoveNext();
        }
        catch (Exception ex) when (ex is InvalidDataException or IOException)
        {
            throw CreateSegmentReadFailure(reader, ex);
        }
    }

    private static IEnumerable<JournalRecord> ReadSegmentRecords(IJournalSegmentReader reader)
    {
        using var enumerator = CreateSegmentEnumerator(reader);
        while (MoveNextSegmentRecord(enumerator, reader))
            yield return enumerator.Current;
    }
}
