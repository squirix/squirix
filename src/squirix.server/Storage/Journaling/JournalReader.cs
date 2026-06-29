using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using Squirix.Server.Storage.Journaling.Read;

namespace Squirix.Server.Storage.Journaling;

internal static class JournalReader
{
    public static JournalSegment[] EnumerateSegments(string dataDir, int fromSegment)
    {
        if (!Directory.Exists(dataDir) || !TryGetJournalFiles(dataDir, out var files) || files.Length is 0)
            return [];

        Array.Sort(files, StringComparer.Ordinal);

        var segments = new JournalSegment[files.Length];
        var writeIndex = 0;
        for (var i = 0; i < files.Length; i++)
        {
            if (!TryParseJournalSegment(files[i], fromSegment, out var segment))
                continue;

            segments[writeIndex++] = segment;
        }

        if (writeIndex is 0)
            return [];

        if (writeIndex != segments.Length)
            Array.Resize(ref segments, writeIndex);

        return segments;
    }

    /// <summary>Sums on-disk journal segment file lengths under <paramref name="dataDir" />.</summary>
    /// <param name="dataDir">Persistence directory containing journal segment files.</param>
    /// <returns>Total byte length of parsed journal segment files.</returns>
    public static long GetOnDiskTotalBytes(string dataDir) => GetOnDiskSegmentStats(dataDir).TotalBytes;

    /// <summary>Counts journal segment files and sums their byte lengths in a single directory enumeration.</summary>
    /// <param name="dataDir">Persistence directory containing journal segment files.</param>
    /// <returns>Segment count and total byte length of parsed journal segment files.</returns>
    public static JournalSegmentStats GetOnDiskSegmentStats(string dataDir)
    {
        if (!Directory.Exists(dataDir) || !TryGetJournalFiles(dataDir, out var files))
            return default;

        var segmentCount = 0;
        var totalBytes = 0L;
        for (var i = 0; i < files.Length; i++)
        {
            var path = files[i];
            if (!TryParseJournalSegment(path, 1, out _))
                continue;

            segmentCount++;
            if (TryGetSegmentLength(path, out var length))
                totalBytes += length;
        }

        return new JournalSegmentStats(segmentCount, totalBytes);
    }

    public static JournalReplaySequence ReadAll(string dataDir, int fromSegment, CancellationToken cancellationToken) =>
        JournalReadPath.ReadAll(dataDir, fromSegment, cancellationToken);

    /// <summary>
    /// Returns up to <paramref name="maxCount" /> journal segments with the largest indices, sorted descending by index.
    /// Memory use is O(<paramref name="maxCount" />), not O(total segments).
    /// </summary>
    /// <param name="dataDir">Persistence directory containing journal segment files.</param>
    /// <param name="fromSegment">Minimum segment index to consider (inclusive).</param>
    /// <param name="maxCount">Maximum number of segments to return; non-positive yields an empty array.</param>
    /// <returns>Segments with the greatest indices, ordered from newest (highest index) to oldest among the selection.</returns>
    public static PriorityQueue<JournalSegment, int> SelectNewestSegments(string dataDir, int fromSegment, int maxCount)
    {
        if (maxCount <= 0 || !Directory.Exists(dataDir) || !TryGetJournalFiles(dataDir, out var files))
            return new PriorityQueue<JournalSegment, int>();

        var pq = new PriorityQueue<JournalSegment, int>();
        for (var i = 0; i < files.Length; i++)
        {
            if (!TryParseJournalSegment(files[i], fromSegment, out var seg))
                continue;

            if (pq.Count < maxCount)
            {
                pq.Enqueue(seg, seg.Index);
                continue;
            }

            if (seg.Index <= pq.Peek().Index)
                continue;

            _ = pq.Dequeue();
            pq.Enqueue(seg, seg.Index);
        }

        return pq;
    }

    private static bool TryGetJournalFiles(string dataDir, out string[] files)
    {
        try
        {
            files = Directory.GetFiles(dataDir, $"{StorageFilePrefixes.Journal}*{StorageFileExtensions.Journal}", SearchOption.TopDirectoryOnly);
            return true;
        }
        catch (IOException)
        {
            files = [];
            return false;
        }
        catch (UnauthorizedAccessException)
        {
            files = [];
            return false;
        }
    }

    private static bool TryGetSegmentLength(string path, out long length)
    {
        length = 0L;
        try
        {
            length = new FileInfo(path).Length;
            return true;
        }
        catch (IOException)
        {
            return false;
        }
        catch (UnauthorizedAccessException)
        {
            return false;
        }
    }

    private static bool TryParseJournalSegment(string path, int fromSegment, out JournalSegment segment)
    {
        segment = default;
        if (!TryParseJournalSegmentIndex(Path.GetFileName(path).AsSpan(), out var idx))
            return false;

        if (idx < fromSegment)
            return false;

        segment = new JournalSegment { Index = idx, Path = path };
        return true;
    }

    private static bool TryParseJournalSegmentIndex(ReadOnlySpan<char> name, out int index)
    {
        index = 0;
        var prefix = StorageFilePrefixes.Journal.AsSpan();
        var extension = StorageFileExtensions.Journal.AsSpan();
        if (name.IsEmpty)
            return false;

        if (!name.StartsWith(prefix, StringComparison.Ordinal))
            return false;

        if (!name.EndsWith(extension, StringComparison.Ordinal))
            return false;

        var numberPart = name.Slice(prefix.Length, name.Length - prefix.Length - extension.Length);
        if (numberPart.IsEmpty)
            return false;

        return int.TryParse(numberPart, NumberStyles.None, CultureInfo.InvariantCulture, out index);
    }

    /// <summary>On-disk journal segment file count and aggregate byte length.</summary>
    /// <param name="SegmentCount">Number of parsed journal segment files.</param>
    /// <param name="TotalBytes">Sum of parsed journal segment file lengths.</param>
    [StructLayout(LayoutKind.Auto)]
    internal readonly record struct JournalSegmentStats(int SegmentCount, long TotalBytes);
}
