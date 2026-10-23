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
    /// A roll stages at <c language="csharp">current + 1</c> and a compaction at the last available segment plus one, which is the same
    /// index when the current segment is the last one; exclusive maintenance and the startup cleanup of leftover temp files keep the
    /// shared <c language="csharp">jrn-*.tmp</c> namespace from colliding. The manifest names a compaction temp file only at its
    /// current segment, where startup publishes it.
    /// </remarks>
    internal static string BuildRollTempPath(string dataDir, int segmentIndex) => PathEx.Combine(
        dataDir,
        $"{FilePrefixes.Journal}{InvariantDigitStrings.FormatD6(segmentIndex)}.tmp");
}
