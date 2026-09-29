using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Read;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Startup repair cuts only a provable torn tail and fails loudly on every other frame damage.</summary>
[Immutable]
public sealed class JournalStartupTailRepairTests : JournalStartupRepairTestBase
{
    /// <summary>A checksum mismatch followed only by zeros to the end of the file is a torn last frame and is truncated.</summary>
    /// <param name="segmentIndex">The active segment (1) or the roll-target segment (2).</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1)]
    [Arguments(2)]
    public async Task BadChecksumFollowedByZerosIsTruncated(int segmentIndex, CancellationToken cancellationToken)
    {
        var first = BuildFrame(1UL, "a");
        var torn = BuildFrame(2UL, "b");
        torn[^1] ^= 0xFF;
        var path = await WriteSegmentFileAsync(segmentIndex, Concat(GoodHeader(), first, torn, new byte[32]), cancellationToken);

        var repairs = Prepare();

        _ = await Assert.That(await FileEqualsAsync(path, Concat(GoodHeader(), first), cancellationToken)).IsTrue();
        _ = await Assert.That(IsSingleRepair(repairs, path, JournalRepairKind.TornTailTruncated, JournalFraming.FileHeaderSize + first.Length + torn.Length + 32, torn.Length + 32)).IsTrue();
        _ = await Assert.That(CountRecords(segmentIndex)).IsEqualTo(1);
    }

    /// <summary>A checksum mismatch followed by a non-zero byte beyond its extent fails startup and leaves the file untouched.</summary>
    /// <param name="segmentIndex">The active segment (1) or the roll-target segment (2).</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1)]
    [Arguments(2)]
    public async Task BadChecksumFollowedByDataThrows(int segmentIndex, CancellationToken cancellationToken)
    {
        var torn = BuildFrame(2UL, "b");
        torn[^1] ^= 0xFF;
        byte[] trailing = [0, 0, 1, 0];
        var original = Concat(GoodHeader(), BuildFrame(1UL, "a"), torn, trailing);
        var path = await WriteSegmentFileAsync(segmentIndex, original, cancellationToken);

        var ex = Repair();

        _ = await Assert.That(ex.Message).Contains(path, StringComparison.Ordinal);
        _ = await Assert.That(await FileEqualsAsync(path, original, cancellationToken)).IsTrue();
    }

    /// <summary>An oversized declared length followed only by zeros is a torn frame header and is truncated.</summary>
    /// <param name="segmentIndex">The active segment (1) or the roll-target segment (2).</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1)]
    [Arguments(2)]
    public async Task OversizedLengthThenZerosIsTruncated(int segmentIndex, CancellationToken cancellationToken)
    {
        var first = BuildFrame(1UL, "a");
        var path = await WriteSegmentFileAsync(segmentIndex, Concat(GoodHeader(), first, OversizedLength(), new byte[16]), cancellationToken);

        var repairs = Prepare();

        _ = await Assert.That(await FileEqualsAsync(path, Concat(GoodHeader(), first), cancellationToken)).IsTrue();
        _ = await Assert.That(IsSingleRepair(repairs, path, JournalRepairKind.TornTailTruncated, JournalFraming.FileHeaderSize + first.Length + 20, 20)).IsTrue();
    }

    /// <summary>An oversized declared length followed by non-zero data fails startup and leaves the file untouched.</summary>
    /// <param name="segmentIndex">The active segment (1) or the roll-target segment (2).</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1)]
    [Arguments(2)]
    public async Task OversizedLengthFollowedByDataThrows(int segmentIndex, CancellationToken cancellationToken)
    {
        var original = Concat(GoodHeader(), BuildFrame(1UL, "a"), OversizedLength(), BuildFrame(2UL, "b"));
        var path = await WriteSegmentFileAsync(segmentIndex, original, cancellationToken);

        var ex = Repair();

        _ = await Assert.That(ex.Message).Contains(path, StringComparison.Ordinal);
        _ = await Assert.That(await FileEqualsAsync(path, original, cancellationToken)).IsTrue();
    }

    /// <summary>A corrupt frame in the middle of the segment fails startup instead of discarding the valid frames after it.</summary>
    /// <param name="segmentIndex">The active segment (1) or the roll-target segment (2).</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1)]
    [Arguments(2)]
    public async Task MiddleCorruptFrameThrows(int segmentIndex, CancellationToken cancellationToken)
    {
        var corrupt = BuildFrame(2UL, "b");
        corrupt[^1] ^= 0xFF;
        var original = Concat(GoodHeader(), BuildFrame(1UL, "a"), corrupt, BuildFrame(3UL, "c"));
        var path = await WriteSegmentFileAsync(segmentIndex, original, cancellationToken);

        var ex = Repair();

        _ = await Assert.That(ex.Message).Contains(path, StringComparison.Ordinal);
        _ = await Assert.That(await FileEqualsAsync(path, original, cancellationToken)).IsTrue();
    }

    /// <summary>A last frame cut inside its length field, payload or checksum is truncated and earlier frames stay intact.</summary>
    /// <param name="segmentIndex">The active segment (1) or the roll-target segment (2).</param>
    /// <param name="keptBytesOfLastFrame">How many bytes of the last frame survive.</param>
    /// <param name="dropFromEnd">Whether the count is measured from the end of the last frame instead of its start.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1, 2, false)]
    [Arguments(1, 6, false)]
    [Arguments(1, 2, true)]
    [Arguments(2, 2, false)]
    [Arguments(2, 6, false)]
    [Arguments(2, 2, true)]
    public async Task TornLastFrameIsTruncated(int segmentIndex, int keptBytesOfLastFrame, bool dropFromEnd, CancellationToken cancellationToken)
    {
        var first = BuildFrame(1UL, "a");
        var last = BuildFrame(2UL, "b");
        var kept = dropFromEnd ? last.Length - keptBytesOfLastFrame : keptBytesOfLastFrame;
        var path = await WriteSegmentFileAsync(segmentIndex, Concat(GoodHeader(), first, last[..kept]), cancellationToken);

        var repairs = Prepare();

        _ = await Assert.That(await FileEqualsAsync(path, Concat(GoodHeader(), first), cancellationToken)).IsTrue();
        _ = await Assert.That(IsSingleRepair(repairs, path, JournalRepairKind.TornTailTruncated, JournalFraming.FileHeaderSize + first.Length + kept, kept)).IsTrue();
        _ = await Assert.That(CountRecords(segmentIndex)).IsEqualTo(1);
    }
}
