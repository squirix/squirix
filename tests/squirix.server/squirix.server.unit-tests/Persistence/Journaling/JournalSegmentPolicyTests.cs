using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>
/// Boundaries of append admission (issue #703): the journal thread's own capacity comparisons, widened by one segment header per append
/// still ahead (plus this one) and by one possible new segment per such append when a roll may be needed.
/// </summary>
[Immutable]
public sealed class JournalSegmentPolicyTests
{
    private const int FrameSegment = 1024 * 1024;

    private const long Segment = 1024L * 1024L;

    private const int SegmentCountLimit = 4;

    private const long Total = 2L * 1024L * 1024L;

    /// <summary>A frame within every bound is admitted, including exact equality with each limit.</summary>
    /// <param name="totalBytes">Journal total bytes.</param>
    /// <param name="pendingBytes">Summed length of the appends still ahead.</param>
    /// <param name="pendingCount">Number of the appends still ahead.</param>
    /// <param name="activeBytes">Active segment length.</param>
    /// <param name="segmentCount">Journal segment count.</param>
    /// <param name="frameBytes">Length of the frame to admit.</param>
    [Test]
    [Arguments(Total - 1005L, 0L, 0, 100L, 1, 1000)]
    [Arguments(Total - 1510L, 500L, 1, 100L, 1, 1000)]
    [Arguments(1000L, 0L, 0, Segment - 999L, 3, 1000)]
    [Arguments(1000L, 600L, 1, Segment - 1500L, 2, 1000)]
    [Arguments(1000L, 0L, 0, Segment - 1000L, SegmentCountLimit, 1000)]
    [Arguments(0L, 0L, 0, 5L, 1, FrameSegment - 5)]
    public void AdmissionAcceptsWithinBounds(long totalBytes, long pendingBytes, int pendingCount, long activeBytes, int segmentCount, int frameBytes) =>
        CreatePolicy().EnsureAdmissionOrThrow(totalBytes, pendingBytes, pendingCount, activeBytes, segmentCount, frameBytes);

    /// <summary>A frame one byte or one segment past a bound is refused with the reason of that bound.</summary>
    /// <param name="totalBytes">Journal total bytes.</param>
    /// <param name="pendingBytes">Summed length of the appends still ahead.</param>
    /// <param name="pendingCount">Number of the appends still ahead.</param>
    /// <param name="activeBytes">Active segment length.</param>
    /// <param name="segmentCount">Journal segment count.</param>
    /// <param name="frameBytes">Length of the frame to admit.</param>
    /// <param name="reason">Text the refusal message must contain.</param>
    [Test]
    [Arguments(Total - 1004L, 0L, 0, 100L, 1, 1000, "total bytes")]
    [Arguments(Total - 1509L, 500L, 1, 100L, 1, 1000, "total bytes")]
    [Arguments(1000L, 0L, 0, Segment - 999L, SegmentCountLimit, 1000, "segment count")]
    [Arguments(1000L, 600L, 1, Segment - 1500L, 3, 1000, "segment count")]
    [Arguments(0L, 0L, 0, 5L, 1, FrameSegment - 4, "segment size")]
    public async Task AdmissionRefusesPastBounds(long totalBytes, long pendingBytes, int pendingCount, long activeBytes, int segmentCount, int frameBytes, string reason)
    {
        var thrown = NodeExceptionAssert.For<JournalCapacityExceededException>().Throws(
            (Policy: CreatePolicy(), Total: totalBytes, PendingBytes: pendingBytes, PendingCount: pendingCount, Active: activeBytes, Segments: segmentCount, Frame: frameBytes),
            static s => s.Policy.EnsureAdmissionOrThrow(s.Total, s.PendingBytes, s.PendingCount, s.Active, s.Segments, s.Frame));

        _ = await Assert.That(thrown.Message).Contains(reason);
    }

    private static JournalSegmentPolicy CreatePolicy() => new(new PersistenceOptions { JournalMaxSegmentMb = 1, JournalMaxSegmentCount = SegmentCountLimit, JournalMaxTotalBytesMb = 2 });
}
