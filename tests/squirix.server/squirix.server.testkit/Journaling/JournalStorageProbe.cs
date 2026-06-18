using System.IO;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.TestKit.Journaling;

/// <summary>Read-only helpers for measuring on-disk journal usage in persistence tests.</summary>
public static class JournalStorageProbe
{
    /// <summary>Returns the total byte length of all journal segment files under <paramref name="dataDir" />.</summary>
    /// <param name="dataDir">Node persistence directory containing journal segment files.</param>
    public static long GetTotalJournalBytes(string dataDir)
    {
        if (string.IsNullOrWhiteSpace(dataDir) || !Directory.Exists(dataDir))
            return 0L;

        var total = 0L;
        foreach (var path in Directory.EnumerateFiles(dataDir, $"{FilePrefixes.Journal}*{FileExtensions.Journal}", SearchOption.TopDirectoryOnly))
        {
            if (TryGetExistingFileLength(path, out var length))
                total += length;
        }

        return total;
    }

    /// <summary>Returns the number of journal segment files under <paramref name="dataDir" />.</summary>
    /// <param name="dataDir">Node persistence directory containing journal segment files.</param>
    public static int CountJournalSegments(string dataDir)
    {
        if (string.IsNullOrWhiteSpace(dataDir) || !Directory.Exists(dataDir))
            return 0;

        var count = 0;
        foreach (var path in Directory.EnumerateFiles(dataDir, $"{FilePrefixes.Journal}*{FileExtensions.Journal}", SearchOption.TopDirectoryOnly))
        {
            if (TryGetExistingFileLength(path, out _))
                count++;
        }

        return count;
    }

    private static bool TryGetExistingFileLength(string path, out long length)
    {
        length = 0L;
        try
        {
            var info = new FileInfo(path);
            if (!info.Exists)
                return false;

            length = info.Length;
            return true;
        }
        catch (IOException)
        {
            // Compaction may delete a segment while we enumerate on-disk journal files.
            return false;
        }
    }
}
