using System;
using System.Globalization;
using System.IO;
using System.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.Read;

/// <summary>Journal segment enumeration and replay.</summary>
internal static class JournalReadPath
{
    internal static string BuildSegmentPath(string dataDir, int segmentIndex) => PathEx.Combine(
        dataDir,
        $"{FilePrefixes.Journal}{segmentIndex.ToString("000000", CultureInfo.InvariantCulture)}{FileExtensions.Journal}");

    internal static JournalSegment[] EnumerateSegments(string dataDir, int fromSegment) => JournalReader.EnumerateSegments(dataDir, fromSegment);

    internal static JournalReplaySequence ReadAll(string dataDir, int fromSegment, CancellationToken cancellationToken) => new(dataDir, fromSegment, cancellationToken);

    internal static InvalidDataException CreateSegmentReadFailure(string path, bool tolerateTruncatedTail, Exception ex) => new(
        $"failed reading journal segment '{path}' (tolerateTruncatedTail={tolerateTruncatedTail}): {ex.Message}",
        ex);
}
