using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Startup repair of journal segment headers restores or rewrites only provably safe headers and fails loudly otherwise.</summary>
[Immutable]
public sealed class JournalStartupHeaderRepairTests : JournalStartupRepairTestBase
{
    /// <summary>A segment header cannot be restored when the first frame is damaged too; startup fails and the file is untouched.</summary>
    /// <param name="segmentIndex">The active segment (1) or the roll-target segment (2).</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1)]
    [Arguments(2)]
    public async Task DamagedHeaderWithDamagedFirstFrameThrows(int segmentIndex, CancellationToken cancellationToken)
    {
        var frame = BuildFrame(1UL, "a");
        frame[^1] ^= 0xFF;
        var original = Concat(BadHeader(), frame, BuildFrame(2UL, "b"));
        var path = await WriteSegmentFileAsync(segmentIndex, original, cancellationToken);

        var ex = Repair();

        _ = await Assert.That(ex.Message).Contains(path, StringComparison.Ordinal);
        _ = await Assert.That(await FileEqualsAsync(path, original, cancellationToken)).IsTrue();
    }

    /// <summary>A damaged magic in front of intact frames is restored in place without discarding any frame.</summary>
    /// <param name="segmentIndex">The active segment (1) or the roll-target segment (2).</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1)]
    [Arguments(2)]
    public async Task DamagedMagicIsRestoredKeepingFrames(int segmentIndex, CancellationToken cancellationToken)
    {
        var frames = Concat(BuildFrame(1UL, "a"), BuildFrame(2UL, "b"));
        var path = await WriteSegmentFileAsync(segmentIndex, Concat(BadHeader(), frames), cancellationToken);

        var repairs = Prepare();

        var after = await File.ReadAllBytesAsync(path, cancellationToken);
        _ = await Assert.That(after.AsSpan().SequenceEqual(Concat(GoodHeader(), frames))).IsTrue();
        _ = await Assert.That(IsSingleRepair(repairs, path, JournalRepairKind.HeaderRestored, after.Length, 0)).IsTrue();
        _ = await Assert.That(CountRecords(segmentIndex)).IsEqualTo(2);
    }

    /// <summary>A zeroed version byte in front of intact frames is restored in place without discarding any frame.</summary>
    /// <param name="segmentIndex">The active segment (1) or the roll-target segment (2).</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1)]
    [Arguments(2)]
    public async Task ZeroedVersionIsRestoredKeepingFrames(int segmentIndex, CancellationToken cancellationToken)
    {
        var frames = Concat(BuildFrame(1UL, "a"), BuildFrame(2UL, "b"));
        var header = GoodHeader();
        header[4] = 0;
        var path = await WriteSegmentFileAsync(segmentIndex, Concat(header, frames), cancellationToken);

        var repairs = Prepare();

        var after = await File.ReadAllBytesAsync(path, cancellationToken);
        _ = await Assert.That(after.AsSpan().SequenceEqual(Concat(GoodHeader(), frames))).IsTrue();
        _ = await Assert.That(IsSingleRepair(repairs, path, JournalRepairKind.HeaderRestored, after.Length, 0)).IsTrue();
        _ = await Assert.That(CountRecords(segmentIndex)).IsEqualTo(2);
    }

    /// <summary>A pre-created segment that is shorter than the header is rewritten with a fresh header.</summary>
    /// <param name="segmentIndex">The active segment (1) or the roll-target segment (2).</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1)]
    [Arguments(2)]
    public async Task ShortTornHeaderIsRewritten(int segmentIndex, CancellationToken cancellationToken)
    {
        var path = await WriteSegmentFileAsync(segmentIndex, [0x53, 0x4A], cancellationToken);

        var repairs = Prepare();

        _ = await Assert.That(await FileEqualsAsync(path, GoodHeader(), cancellationToken)).IsTrue();
        _ = await Assert.That(IsSingleRepair(repairs, path, JournalRepairKind.TornCreationHeaderRewritten, 2, 2)).IsTrue();
    }

    /// <summary>A file with an invalid header and no frames is rewritten with a fresh header.</summary>
    /// <param name="segmentIndex">The active segment (1) or the roll-target segment (2).</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1)]
    [Arguments(2)]
    public async Task InvalidHeaderWithoutFramesIsRewritten(int segmentIndex, CancellationToken cancellationToken)
    {
        var path = await WriteSegmentFileAsync(segmentIndex, BadHeader(), cancellationToken);

        var repairs = Prepare();

        _ = await Assert.That(await FileEqualsAsync(path, GoodHeader(), cancellationToken)).IsTrue();
        _ = await Assert.That(IsSingleRepair(repairs, path, JournalRepairKind.TornCreationHeaderRewritten, JournalFraming.FileHeaderSize, JournalFraming.FileHeaderSize)).IsTrue();
    }

    /// <summary>A zero-filled pre-sized segment is rewritten with a fresh header.</summary>
    /// <param name="segmentIndex">The active segment (1) or the roll-target segment (2).</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1)]
    [Arguments(2)]
    public async Task ZeroFilledSegmentIsRewritten(int segmentIndex, CancellationToken cancellationToken)
    {
        var path = await WriteSegmentFileAsync(segmentIndex, new byte[256], cancellationToken);

        var repairs = Prepare();

        _ = await Assert.That(await FileEqualsAsync(path, GoodHeader(), cancellationToken)).IsTrue();
        _ = await Assert.That(IsSingleRepair(repairs, path, JournalRepairKind.TornCreationHeaderRewritten, 256, 256)).IsTrue();
    }

    /// <summary>The coordinator factory logs every repair startup recovery reports.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FactoryLogsReportedRepairs(CancellationToken cancellationToken)
    {
        var first = BuildFrame(1UL, "a");
        var last = BuildFrame(2UL, "b");
        _ = await WriteSegmentFileAsync(1, Concat(GoodHeader(), first, last[..3]), cancellationToken);
        var persistence = NewPersistence(Dir);
        using var ledger = new Ledger(persistence);
        await ledger.WriteAsync(NewManifest(), cancellationToken);
        var log = new EventRecordingLogger();

        await using var journal = JournalCoordinatorFactory.Create(
            persistence,
            await ledger.ReadCurrentOrDefaultAsync(cancellationToken),
            ledger,
            new AsyncManualResetEvent(true),
            log);

        _ = await Assert.That(log.Count(1018)).IsEqualTo(1);
        _ = await Assert.That(log.Find(1018)?.Level).IsEqualTo(LogLevel.Warning);
    }

    /// <summary>A segment under an unsupported non-zero format version fails startup and is never rewritten.</summary>
    /// <param name="segmentIndex">The active segment (1) or the roll-target segment (2).</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1)]
    [Arguments(2)]
    public async Task UnsupportedVersionThrowsWithoutRewrite(int segmentIndex, CancellationToken cancellationToken)
    {
        var header = GoodHeader();
        header[4] = 1;
        var original = Concat(header, BuildFrame(1UL, "a"));
        var path = await WriteSegmentFileAsync(segmentIndex, original, cancellationToken);

        var ex = Repair();

        _ = await Assert.That(ex.Message).Contains("version 1", StringComparison.Ordinal);
        _ = await Assert.That(await FileEqualsAsync(path, original, cancellationToken)).IsTrue();
    }
}
