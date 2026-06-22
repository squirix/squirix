using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using JetBrains.Annotations;
using Squirix.Server.Storage.Journaling.Abstractions;
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

        var onDiskCount = JournalReader.SelectNewestSegments(dataDir, 1, policy.SegmentCountProbeLimit).Length;
        policy.EnsureRollCapacityOrThrow(onDiskCount, JournalReader.GetOnDiskTotalBytes(dataDir));
    }

    internal static JournalSegment[] EnumerateSegments(string dataDir, int fromSegment) => JournalReader.EnumerateSegments(dataDir, fromSegment);

    internal static IEnumerable<JournalRecord> ReadAll(string dataDir, int fromSegment, CancellationToken cancellationToken)
    {
        var segments = EnumerateSegments(dataDir, fromSegment);

        for (var i = 0; i < segments.Length; i++)
        {
            var tolerateTruncatedTail = i == segments.Length - 1;
            var reader = new BinaryJournalSegmentReader(segments[i].Path, tolerateTruncatedTail, cancellationToken);
            foreach (var record in ReadSegmentRecords(reader))
                yield return record;
        }
    }

    [MustDisposeResource]
    private static IEnumerator<JournalRecord> CreateSegmentEnumerator(BinaryJournalSegmentReader reader)
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

    private static InvalidDataException CreateSegmentReadFailure(BinaryJournalSegmentReader reader, Exception ex) =>
        new(
            $"failed reading journal segment '{reader.Path}' (tolerateTruncatedTail={reader.TolerateTruncatedTail}): {ex.Message}",
            ex);

    private static bool MoveNextSegmentRecord(IEnumerator<JournalRecord> enumerator, BinaryJournalSegmentReader reader)
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

    private static IEnumerable<JournalRecord> ReadSegmentRecords(BinaryJournalSegmentReader reader)
    {
        using var enumerator = CreateSegmentEnumerator(reader);
        while (MoveNextSegmentRecord(enumerator, reader))
            yield return enumerator.Current;
    }
}
