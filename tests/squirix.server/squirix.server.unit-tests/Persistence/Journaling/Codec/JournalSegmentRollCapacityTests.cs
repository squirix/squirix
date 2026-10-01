using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Codec;

/// <summary>
/// Tests for Pipelined journal segment roll capacity enforcement, and for append admission: the journal thread's own
/// capacity comparisons, widened by bounds for the appends still ahead of a frame.
/// </summary>
[Immutable]
public sealed class JournalSegmentRollCapacityTests
{
    private const int AdmissionSegmentCountLimit = 32;

    private const long AdmissionTotal = 64L * 1024L * 1024L;

    private const int OneMegabyte = 1024 * 1024;

    private const int ScenarioCount = 5000;

    private const long Segment = 1024L * 1024L;

    private const long Usable = Segment - JournalFraming.FileHeaderSize;

    /// <summary>
    /// A frame within every admission bound is admitted, including exact equality with each limit; the test passes by the admission not
    /// throwing. A backlog of many small frames near a segment end predicts one roll, not one per frame.
    /// </summary>
    /// <param name="totalBytes">Journal total bytes.</param>
    /// <param name="pendingBytes">Summed length of the appends still ahead.</param>
    /// <param name="pendingCount">Number of the appends still ahead.</param>
    /// <param name="openCreatesSegment">Whether the next open creates the current segment.</param>
    /// <param name="activeBytes">Active segment length.</param>
    /// <param name="segmentCount">Journal segment count.</param>
    /// <param name="frameBytes">Length of the frame to admit.</param>
    [Test]
    [Arguments(AdmissionTotal - 1005L, 0L, 0, false, 100L, 1, 1000)]
    [Arguments(AdmissionTotal - 1510L, 500L, 1, false, 100L, 1, 1000)]
    [Arguments(1000L, 0L, 0, false, Segment - 999L, AdmissionSegmentCountLimit - 1, 1000)]
    [Arguments(1000L, 600L, 1, false, Segment - 1500L, AdmissionSegmentCountLimit - 1, 1000)]
    [Arguments(1000L, 100L * 1024L, 100, false, Segment - (50L * 1024L), 5, 1000)]
    [Arguments(1000L, 100L * 1024L, 100, false, Segment - (50L * 1024L), AdmissionSegmentCountLimit - 1, 1000)]
    [Arguments(1000L, Usable, 2, false, 5L, AdmissionSegmentCountLimit - 3, 1000)]
    [Arguments(1000L, Usable, 1, false, 5L, AdmissionSegmentCountLimit - 2, 1000)]
    [Arguments(0L, 0L, 0, true, Segment - 999L, AdmissionSegmentCountLimit - 2, 1000)]
    [Arguments(1000L, 0L, 0, false, Segment - 1000L, AdmissionSegmentCountLimit, 1000)]
    [Arguments(0L, 0L, 0, false, 5L, 1, OneMegabyte - 5)]
    public void AdmissionAcceptsWithinBounds(long totalBytes, long pendingBytes, int pendingCount, bool openCreatesSegment, long activeBytes, int segmentCount, int frameBytes)
    {
        var snapshot = new JournalAdmissionSnapshot(pendingBytes, pendingCount, openCreatesSegment, false, totalBytes, activeBytes, segmentCount);
        CreateAdmissionPolicy().EnsureAdmissionOrThrow(in snapshot, frameBytes);
    }

    /// <summary>
    /// The open of a missing current segment adds a segment the journal thread never checks, so a frame that may roll right after it is
    /// refused when that segment takes the last slot.
    /// </summary>
    [Test]
    public async Task AdmissionCountsCreatedSegment()
    {
        var policy = CreateAdmissionPolicy();
        var fits = new JournalAdmissionSnapshot(0L, 0, true, false, 0L, Segment - 999L, AdmissionSegmentCountLimit - 2);
        var full = fits with { SegmentCount = AdmissionSegmentCountLimit - 1 };

        policy.EnsureAdmissionOrThrow(in fits, 1000);
        var thrown = NodeExceptionAssert.For<JournalCapacityExceededException>().Throws((Policy: policy, Snapshot: full), static s => s.Policy.EnsureAdmissionOrThrow(in s.Snapshot, 1000));

        _ = await Assert.That(thrown.Message).Contains("segment count", StringComparison.Ordinal);
    }

    /// <summary>
    /// A roll into a pre-created target the journal already counted adds no segment, so at the segment count limit the frame that rolls
    /// into it is admitted; a backlog that may roll past that target needs a new segment and is refused.
    /// </summary>
    [Test]
    public async Task AdmissionReusesCountedRollTarget()
    {
        var policy = CreateAdmissionPolicy();
        var intoTarget = new JournalAdmissionSnapshot(0L, 0, false, true, 1000L, Segment - 999L, AdmissionSegmentCountLimit);
        var pastTarget = new JournalAdmissionSnapshot(Usable, 1, false, true, 1000L, 5L, AdmissionSegmentCountLimit);

        policy.EnsureAdmissionOrThrow(in intoTarget, 1000);
        var thrown = NodeExceptionAssert.For<JournalCapacityExceededException>().Throws((Policy: policy, Snapshot: pastTarget), static s => s.Policy.EnsureAdmissionOrThrow(in s.Snapshot, 1000));

        _ = await Assert.That(thrown.Message).Contains("segment count", StringComparison.Ordinal);
    }

    /// <summary>A frame one byte or one segment past an admission bound is refused with the reason of that bound.</summary>
    /// <param name="totalBytes">Journal total bytes.</param>
    /// <param name="pendingBytes">Summed length of the appends still ahead.</param>
    /// <param name="pendingCount">Number of the appends still ahead.</param>
    /// <param name="activeBytes">Active segment length.</param>
    /// <param name="segmentCount">Journal segment count.</param>
    /// <param name="frameBytes">Length of the frame to admit.</param>
    /// <param name="reason">Text the refusal message must contain.</param>
    [Test]
    [Arguments(AdmissionTotal - 1004L, 0L, 0, 100L, 1, 1000, "total bytes")]
    [Arguments(AdmissionTotal - 1509L, 500L, 1, 100L, 1, 1000, "total bytes")]
    [Arguments(1000L, 0L, 0, Segment - 999L, AdmissionSegmentCountLimit, 1000, "segment count")]
    [Arguments(1000L, Usable, 2, 5L, AdmissionSegmentCountLimit - 2, 1000, "segment count")]
    [Arguments(1000L, Usable, 1, 5L, AdmissionSegmentCountLimit - 1, 1000, "segment count")]
    [Arguments(0L, 0L, 0, 5L, 1, OneMegabyte - 4, "segment size")]
    public async Task AdmissionRefusesPastBounds(long totalBytes, long pendingBytes, int pendingCount, long activeBytes, int segmentCount, int frameBytes, string reason)
    {
        var snapshot = new JournalAdmissionSnapshot(pendingBytes, pendingCount, false, false, totalBytes, activeBytes, segmentCount);
        var thrown = NodeExceptionAssert.For<JournalCapacityExceededException>().Throws(
            (Policy: CreateAdmissionPolicy(), Snapshot: snapshot, Frame: frameBytes),
            static s => s.Policy.EnsureAdmissionOrThrow(in s.Snapshot, s.Frame));

        _ = await Assert.That(thrown.Message).Contains(reason, StringComparison.Ordinal);
    }

    /// <summary>Total byte cap rejects an append that would exceed configured journal size.</summary>
    [Test]
    public async Task AppendCapThrowsPastTotalByteLimit()
    {
        var policy = new JournalSegmentPolicy(new PersistenceOptions { JournalMaxTotalBytesMb = 1 });
        var error = NodeExceptionAssert.For<JournalCapacityExceededException>().Throws(policy, static value => value.EnsureAppendCapacityOrThrow(OneMegabyte, 1));
        _ = await Assert.That(error.Message).Contains("total bytes", StringComparison.Ordinal);
    }

    /// <summary>The smallest segment the options accept holds the largest frame: a recorded reply at the gRPC limit plus the frame overhead.</summary>
    [Test]
    public async Task SmallestSegmentHoldsLargestFrame()
    {
        var policy = new JournalSegmentPolicy(new PersistenceOptions { JournalMaxSegmentMb = JournalSegmentLimits.MinSegmentMb });

        // A put of the largest entry and a reply at the gRPC limit, each with the most overhead a frame carries.
        policy.EnsureFitsEmptySegmentOrThrow(EntryLimits.MaxEntrySizeBytes + JournalSegmentLimits.MaxFrameOverheadBytes);
        policy.EnsureFitsEmptySegmentOrThrow(EntryLimits.GrpcMaxSendMessageSizeBytes + JournalSegmentLimits.MaxFrameOverheadBytes);
        var whole = NodeExceptionAssert.For<JournalCapacityExceededException>()
                                       .Throws(policy, static value => value.EnsureFitsEmptySegmentOrThrow(JournalSegmentLimits.MinSegmentMb * OneMegabyte));

        _ = await Assert.That(whole.Message).Contains("segment size", StringComparison.Ordinal);
    }

    /// <summary>
    /// A frame that fills an empty segment exactly (file header plus frame equal to the segment size) fits; one byte more never fits any
    /// segment and is refused, whatever the journal state.
    /// </summary>
    [Test]
    public async Task EmptySegmentFitRefusesPastSegmentSize()
    {
        var policy = CreateAdmissionPolicy();

        policy.EnsureFitsEmptySegmentOrThrow(Convert.ToInt32(Usable));
        var thrown = NodeExceptionAssert.For<JournalCapacityExceededException>().Throws(policy, static value => value.EnsureFitsEmptySegmentOrThrow(Convert.ToInt32(Usable) + 1));

        _ = await Assert.That(thrown.Message).Contains("segment size", StringComparison.Ordinal);
    }

    /// <summary>
    /// Seeded simulation of the journal thread's next-fit roll rule over random backlogs (small, large, and alternating half-segment and
    /// tiny frames), some with a counted header-only roll target: whenever the incoming frame rolls, the bound covers every segment added
    /// up to and including that roll and stays within twice that count plus two, and admission refuses the frame when one segment slot
    /// fewer is left than the journal thread needs.
    /// </summary>
    [Test]
    public async Task NewSegmentBoundCoversNextFitRolls()
    {
        var policy = new JournalSegmentPolicy(new PersistenceOptions
        {
            JournalMaxSegmentMb = 1,
            JournalMaxSegmentCount = JournalSegmentLimits.HardMaxSegmentCount,
            JournalMaxTotalBytesMb = JournalSegmentLimits.HardMaxTotalBytesMb,
        });
        var random = new SplitMix64(0x703UL);
        var rolled = 0;
        var outOfBound = 0;
        for (var scenario = 0; scenario < ScenarioCount; scenario++)
        {
            var backlog = new int[random.NextInt(0, 64)];
            var pendingBytes = 0L;
            for (var i = 0; i < backlog.Length; i++)
            {
                backlog[i] = NextFrame(random, scenario % 3);
                pendingBytes += backlog[i];
            }

            var frame = NextFrame(random, scenario % 3);
            var active = random.NextLong(JournalFraming.FileHeaderSize, Segment);
            var rollTargetCounted = random.NextInt(0, 2) == 0;
            var rolls = SimulateRolls(active, backlog, frame, out var frameRolls);
            if (!frameRolls)
                continue;

            // The first roll goes into the counted target and adds no segment.
            var added = rollTargetCounted ? rolls - 1L : rolls;
            rolled++;
            var bound = policy.BoundNewSegments(pendingBytes, backlog.Length, frame, rollTargetCounted);
            if (bound < added || bound > (2L * added) + 2L)
                outOfBound++;

            // One slot fewer than the journal thread needs: it would reject the roll, so admission must refuse the frame.
            var snapshot = new JournalAdmissionSnapshot(pendingBytes, backlog.Length, false, rollTargetCounted, 0L, active, JournalSegmentLimits.HardMaxSegmentCount - Convert.ToInt32(added) + 1);
            _ = NodeExceptionAssert.For<JournalCapacityExceededException>().Throws((Policy: policy, Snapshot: snapshot, Frame: frame), static s => s.Policy.EnsureAdmissionOrThrow(in s.Snapshot, s.Frame));
        }

        _ = await Assert.That(outOfBound).IsEqualTo(0);
        _ = await Assert.That(rolled).IsGreaterThan(ScenarioCount / 4);
    }

    /// <summary>Roll is rejected when the next segment would exceed the configured segment-count limit.</summary>
    [Test]
    public async Task RollThrowsPastSegmentCountLimit()
    {
        var policy = new JournalSegmentPolicy(new PersistenceOptions { JournalMaxSegmentCount = 2, JournalMaxTotalBytesMb = 64 });
        var error = NodeExceptionAssert.For<JournalCapacityExceededException>().Throws(policy, static value => value.EnsureRollCapacityOrThrow(2, 0));
        _ = await Assert.That(error.Message).Contains("segment count", StringComparison.Ordinal);
    }

    /// <summary>Roll is rejected when on-disk total bytes already exceed the configured journal size.</summary>
    [Test]
    public async Task RollThrowsPastTotalByteLimit()
    {
        var policy = new JournalSegmentPolicy(new PersistenceOptions { JournalMaxTotalBytesMb = 1, JournalMaxSegmentCount = 32 });
        var error = NodeExceptionAssert.For<JournalCapacityExceededException>().Throws(policy, static value => value.EnsureRollCapacityOrThrow(1, OneMegabyte + 1));
        _ = await Assert.That(error.Message).Contains("total bytes", StringComparison.Ordinal);
    }

    /// <summary>Per-segment byte cap triggers roll before the next frame would overflow the active segment.</summary>
    [Test]
    public async Task RollTriggersPastSegmentByteCap()
    {
        var policy = new JournalSegmentPolicy(new PersistenceOptions { JournalMaxSegmentMb = 1 });
        _ = await Assert.That(policy.ShouldRollSegment(OneMegabyte, 1)).IsTrue();
    }

    private static JournalSegmentPolicy CreateAdmissionPolicy() => new(new PersistenceOptions
    {
        JournalMaxSegmentMb = 1,
        JournalMaxSegmentCount = AdmissionSegmentCountLimit,
        JournalMaxTotalBytesMb = 64,
    });

    /// <summary>Draws a frame length that fits an empty one-megabyte segment.</summary>
    /// <param name="random">Seeded generator.</param>
    /// <param name="profile">0: small frames; 1: large frames; 2: frames just over half a segment mixed with tiny ones (next fit's worst case).</param>
    /// <returns>The frame length.</returns>
    private static int NextFrame(SplitMix64 random, int profile) => profile switch
    {
        0 => random.NextInt(1, 4096),
        1 => random.NextInt(Convert.ToInt32(Usable / 4L), Convert.ToInt32(Usable)),
        _ => random.NextInt(0, 1) == 0 ? random.NextInt(1, 64) : random.NextInt(Convert.ToInt32(Usable / 2L) + 1, Convert.ToInt32(Usable / 2L) + 4096),
    };

    /// <summary>Runs the journal thread's next-fit roll rule: roll when the active length plus the frame exceeds the segment size, and a new segment starts at the file header.</summary>
    /// <param name="active">Active segment length when the backlog starts.</param>
    /// <param name="backlog">Frames placed before the incoming frame.</param>
    /// <param name="frame">Incoming frame.</param>
    /// <param name="frameRolls">Whether the incoming frame rolls.</param>
    /// <returns>Rolls of the backlog, plus one when the incoming frame rolls.</returns>
    private static long SimulateRolls(long active, int[] backlog, int frame, out bool frameRolls)
    {
        var added = 0L;
        for (var i = 0; i < backlog.Length; i++)
        {
            if (active + backlog[i] > Segment)
            {
                added++;
                active = JournalFraming.FileHeaderSize;
            }

            active += backlog[i];
        }

        frameRolls = active + frame > Segment;
        return frameRolls ? added + 1L : added;
    }

    /// <summary>Deterministic SplitMix64 generator, so the simulation replays the same scenarios on every run.</summary>
    [Mutable]
    private sealed class SplitMix64
    {
        private ulong _state;

        internal SplitMix64(ulong seed)
        {
            _state = seed;
        }

        /// <summary>Draws an integer in the inclusive range.</summary>
        /// <param name="min">Smallest value.</param>
        /// <param name="max">Largest value.</param>
        /// <returns>The drawn value.</returns>
        internal int NextInt(int min, int max) => Convert.ToInt32(NextLong(min, max));

        /// <summary>Draws a long integer in the inclusive range.</summary>
        /// <param name="min">Smallest value.</param>
        /// <param name="max">Largest value.</param>
        /// <returns>The drawn value.</returns>
        internal long NextLong(long min, long max) => min + Convert.ToInt64(Next() % Convert.ToUInt64(max - min + 1L));

        private ulong Next()
        {
            unchecked
            {
                _state += 0x9E3779B97F4A7C15UL;
                var z = _state;
                z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
                z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
                return z ^ (z >> 31);
            }
        }
    }
}
