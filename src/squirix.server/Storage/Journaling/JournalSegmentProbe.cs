using System.IO;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling.Read;

namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// What the journal thread will find when it opens a segment that is not open yet (at startup and after a maintenance end), so that append
/// admission can read it before that open (issue #703).
/// </summary>
/// <param name="ActiveBytesAfterOpen">Active segment length right after the open: the file length, or the file header size for a missing or empty file, because that open writes the header.</param>
/// <param name="OpenCreatesSegment">Whether the file is missing, so the open creates it and adds a segment the journal thread does not check against the segment count limit.</param>
[Immutable]
internal readonly record struct JournalSegmentProbe(long ActiveBytesAfterOpen, bool OpenCreatesSegment)
{
    /// <summary>Probes the segment the journal thread opens next.</summary>
    /// <param name="dataDir">Persistence directory containing journal segment files.</param>
    /// <param name="segmentIndex">One-based index of the segment the journal thread opens next.</param>
    /// <returns>The probe of that segment.</returns>
    /// <exception cref="IOException">The segment exists, but its length cannot be read.</exception>
    internal static JournalSegmentProbe Probe(string dataDir, int segmentIndex)
    {
        var segment = new FileInfo(JournalReadPath.BuildSegmentPath(dataDir, segmentIndex));
        var exists = segment.Exists;
        var length = exists ? segment.Length : 0L;
        return new JournalSegmentProbe(length == 0L ? JournalFraming.FileHeaderSize : length, !exists);
    }
}
