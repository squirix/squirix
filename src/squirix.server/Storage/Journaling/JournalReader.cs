using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using Squirix.Server.Storage.JournalProto;

namespace Squirix.Server.Storage.Journaling;

internal static class JournalReader
{
    public static IEnumerable<JournalSegment> EnumerateSegments(string dataDir, int fromSegment)
    {
        if (!Directory.Exists(dataDir) || !TryEnumerateJournalFiles(dataDir, out var files))
            return [];

        var results = new List<JournalSegment>();
        foreach (var path in files)
        {
            if (TryParseJournalSegment(path, fromSegment, out var segment))
                results.Add(segment);
        }

        results.Sort(static (a, b) => a.Index.CompareTo(b.Index));
        return results;
    }

    public static IEnumerable<JournalEnvelope> ReadAll(string dataDir, int fromSegment, CancellationToken cancellationToken)
    {
        var segments = new List<JournalSegment>();
        foreach (var segment in EnumerateSegments(dataDir, fromSegment))
            segments.Add(segment);

        for (var i = 0; i < segments.Count; i++)
        {
            var pair = segments[i];
            var tolerateTruncatedTail = i == segments.Count - 1;
            using var segment = new MappedJournalSegmentReader(pair.Path, tolerateTruncatedTail, cancellationToken).GetEnumerator();
            while (segment.MoveNext())
            {
                var env = segment.Current;
                yield return env;
            }
        }
    }

    /// <summary>
    /// Returns up to <paramref name="maxCount" /> journal segments with the largest indices, sorted descending by index.
    /// Memory use is O(<paramref name="maxCount" />), not O(total segments).
    /// </summary>
    /// <param name="dataDir">Persistence directory containing journal segment files.</param>
    /// <param name="fromSegment">Minimum segment index to consider (inclusive).</param>
    /// <param name="maxCount">Maximum number of segments to return; non-positive yields an empty array.</param>
    /// <returns>Segments with the greatest indices, ordered from newest (highest index) to oldest among the selection.</returns>
    public static JournalSegment[] SelectNewestSegments(string dataDir, int fromSegment, int maxCount)
    {
        if (maxCount <= 0 || !Directory.Exists(dataDir) || !TryEnumerateJournalFiles(dataDir, out var files))
            return [];

        var pq = new PriorityQueue<JournalSegment, int>();
        foreach (var path in files)
        {
            if (!TryParseJournalSegment(path, fromSegment, out var seg))
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

        return MaterializeNewestSegments(pq);
    }

    private static JournalSegment[] MaterializeNewestSegments(PriorityQueue<JournalSegment, int> pq)
    {
        var taken = new JournalSegment[pq.Count];
        var index = 0;
        while (pq.Count > 0)
            taken[index++] = pq.Dequeue();

        Array.Sort(taken, static (a, b) => b.Index.CompareTo(a.Index));
        return taken;
    }

    private static bool TryEnumerateJournalFiles(string dataDir, out IEnumerable<string> files)
    {
        try
        {
            files = Directory.EnumerateFiles(dataDir, $"{StorageFilePrefixes.Journal}*{StorageFileExtensions.Journal}", SearchOption.TopDirectoryOnly);
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

    private static bool TryParseJournalSegment(string path, int fromSegment, out JournalSegment segment)
    {
        segment = default;
        var name = Path.GetFileName(path);
        if (!name.StartsWith(StorageFilePrefixes.Journal, StringComparison.Ordinal))
            return false;

        if (!name.EndsWith(StorageFileExtensions.Journal, StringComparison.Ordinal))
            return false;

        var prefixLen = StorageFilePrefixes.Journal.Length;
        var extensionLen = StorageFileExtensions.Journal.Length;
        var digitsLen = name.Length - prefixLen - extensionLen;
        if (digitsLen <= 0)
            return false;

        string digits;
        try
        {
            digits = name.Substring(prefixLen, digitsLen);
        }
        catch (ArgumentOutOfRangeException)
        {
            return false;
        }

        if (!int.TryParse(digits, NumberStyles.None, CultureInfo.InvariantCulture, out var idx))
            return false;

        if (idx < fromSegment)
            return false;

        segment = new JournalSegment { Index = idx, Path = path };
        return true;
    }
}
