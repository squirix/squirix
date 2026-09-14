using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage;

/// <summary>Stable journal segment path helpers shared by journaling and retention.</summary>
internal static class JournalPaths
{
    internal static string BuildSegmentPath(string dataDir, int segmentIndex) => PathEx.Combine(
        dataDir,
        $"{FilePrefixes.Journal}{InvariantDigitStrings.FormatD6(segmentIndex)}{FileExtensions.Journal}");

    /// <summary>
    /// Builds the temp path used to stage a segment file header before atomic publication. The
    /// <c language="csharp">.tmp</c> suffix keeps it invisible to segment enumeration
    /// (<c language="csharp">{prefix}*{ext}</c> glob), matching the compaction staging convention.
    /// </summary>
    /// <param name="dataDir">Persistence directory containing journal segment files.</param>
    /// <param name="segmentIndex">One-based journal segment index.</param>
    /// <returns>Temp path for staging the segment header.</returns>
    /// <remarks>
    /// Roll temp indices (<c language="csharp">current + 1</c>) always exceed the last available segment,
    /// while compaction stages at or below it; combined with exclusive maintenance, the shared
    /// <c language="csharp">jrn-*.tmp</c> namespace cannot collide.
    /// </remarks>
    internal static string BuildRollTempPath(string dataDir, int segmentIndex) => PathEx.Combine(
        dataDir,
        $"{FilePrefixes.Journal}{InvariantDigitStrings.FormatD6(segmentIndex)}.tmp");
}
