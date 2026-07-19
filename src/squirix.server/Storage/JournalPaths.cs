using System.Globalization;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage;

/// <summary>Stable journal segment path helpers shared by journaling and retention.</summary>
internal static class JournalPaths
{
    internal static string BuildSegmentPath(string dataDir, int segmentIndex) => PathEx.Combine(
        dataDir,
        $"{FilePrefixes.Journal}{segmentIndex.ToString("000000", CultureInfo.InvariantCulture)}{FileExtensions.Journal}");
}
