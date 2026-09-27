using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// One producer-side read of the journal capacity state that append admission checks a frame against (issue #703). The fields are listed in
/// read order: the pending counters first, then the journal thread's flags, then its counters.
/// </summary>
/// <param name="PendingBytes">Summed frame length of the appends admitted and not yet written.</param>
/// <param name="PendingCount">Number of the appends admitted and not yet written.</param>
/// <param name="OpenCreatesSegment">Whether the journal thread's next segment open creates and counts a segment file.</param>
/// <param name="RollTargetCounted">Whether the journal thread's next roll target is already on disk, counted, and at most a file header.</param>
/// <param name="TotalBytes">Journal total bytes.</param>
/// <param name="ActiveSegmentBytes">Active segment length; while the segment is not open, the length its next open will see.</param>
/// <param name="SegmentCount">Journal segment count.</param>
[Immutable]
internal readonly record struct JournalAdmissionSnapshot(long PendingBytes, int PendingCount, bool OpenCreatesSegment, bool RollTargetCounted, long TotalBytes, long ActiveSegmentBytes, int SegmentCount);
