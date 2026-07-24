using System;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.Abstractions;

internal static class JournalReader
{
    /// <summary>Counts journal segment files and sums their byte lengths in a single directory enumeration.</summary>
    /// <param name="dataDir">Persistence directory containing journal segment files.</param>
    /// <returns>Segment count and total byte length of parsed journal segment files.</returns>
    internal static (int SegmentCount, long TotalBytes) GetOnDiskSegmentStats(string dataDir)
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

        return (segmentCount, totalBytes);
    }

    internal static JournalSegment[] EnumerateSegments(string dataDir, int fromSegment)
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

    private static bool TryGetJournalFiles(string dataDir, out string[] files)
    {
        try
        {
            var validatedDataDir = FilePathValidator.ResolveValidatedDirectoryPath(dataDir);
            files = Directory.GetFiles(validatedDataDir, $"{FilePrefixes.Journal}*{FileExtensions.Journal}", SearchOption.TopDirectoryOnly);
            return true;
        }
        catch (ArgumentException)
        {
            files = [];
            return false;
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

    private static bool TryParseJournalSegment(string path, int fromSegment, [NotNullWhen(true)] out JournalSegment? segment)
    {
        segment = null;
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
        var prefix = FilePrefixes.Journal.AsSpan();
        var extension = FileExtensions.Journal.AsSpan();
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
}
