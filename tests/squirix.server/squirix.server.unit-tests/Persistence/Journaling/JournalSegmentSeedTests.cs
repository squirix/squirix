using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>
/// Append admission reads the journal thread's capacity state before the journal thread opens the current segment (issue #703), so at
/// startup and after a maintenance end the active segment counter and the open flag must already describe what that open will find.
/// </summary>
[Immutable]
public sealed class JournalSegmentSeedTests : IsolatedStorageTestBase
{
    private const int SmallPayload = 16;

    /// <summary>A fresh journal seeds the active segment counter with the header the first open writes.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task FreshStartSeedsHeaderBytes(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, cancellationToken);
        var seeded = journal.Journal.EventLoop.ActiveSegmentWrittenBytes;

        await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), cancellationToken);
        await journal.Journal.AwaitDurabilityCommitAsync(cancellationToken);

        _ = await Assert.That(seeded).IsEqualTo(JournalFraming.FileHeaderSize);
        _ = await Assert.That(journal.Journal.EventLoop.ActiveSegmentWrittenBytes).IsEqualTo(SegmentLength(1));
    }

    /// <summary>
    /// A maintenance that leaves no reset segment file seeds the header the next open writes and marks that open as adding a segment; the
    /// next append creates and counts it.
    /// </summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task MaintenanceEndSeedsMissingSegment(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, cancellationToken);
        await AppendDurablyAsync(journal, "a", SmallPayload, cancellationToken);

        var segmentPath = JournalReadPath.BuildSegmentPath(Dir, 1);
        await journal.Journal.ExecuteMaintenanceExclusiveAsync(
            _ =>
            {
                File.Delete(segmentPath);
                return ValueTask.CompletedTask;
            },
            cancellationToken);
        var seeded = journal.Journal.EventLoop.ActiveSegmentWrittenBytes;
        var openCreatesSegment = journal.Journal.EventLoop.OpenCreatesSegment;
        var countAfterEnd = journal.Journal.EventLoop.JournalSegmentCount;
        await AppendDurablyAsync(journal, "b", SmallPayload, cancellationToken);

        _ = await Assert.That(seeded).IsEqualTo(JournalFraming.FileHeaderSize);
        _ = await Assert.That(openCreatesSegment).IsTrue();
        _ = await Assert.That(countAfterEnd).IsEqualTo(0);
        _ = await Assert.That(journal.Journal.EventLoop.OpenCreatesSegment).IsFalse();
        _ = await Assert.That(journal.Journal.EventLoop.JournalSegmentCount).IsEqualTo(1);
        _ = await Assert.That(journal.Journal.EventLoop.ActiveSegmentWrittenBytes).IsEqualTo(SegmentLength(1));
    }

    /// <summary>After a no-op maintenance, the active segment counter holds the reset segment's length instead of zero.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task MaintenanceEndSeedsSegmentBytes(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, cancellationToken);
        await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), cancellationToken);
        await journal.Journal.AwaitDurabilityCommitAsync(cancellationToken);

        await journal.Journal.ExecuteMaintenanceExclusiveAsync(static _ => ValueTask.CompletedTask, cancellationToken);

        _ = await Assert.That(journal.Journal.EventLoop.ActiveSegmentWrittenBytes).IsEqualTo(SegmentLength(1));
        _ = await Assert.That(journal.Journal.EventLoop.ActiveSegmentWrittenBytes).IsGreaterThan(JournalFraming.FileHeaderSize);
    }

    /// <summary>A restart seeds the active segment counter with the current segment's on-disk length before any append opens it.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task RestartSeedsSegmentBytes(bool groupCommit, CancellationToken cancellationToken)
    {
        await using (var first = await StallableJournal.CreateAsync(Dir, groupCommit, cancellationToken))
        {
            await first.Journal.AppendPutUnderGateAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), cancellationToken);
            await first.Journal.AppendPutUnderGateAsync(CacheKey.Default("b"), JournalEntryPayloadKit.EncodePut("b"), cancellationToken);
            await first.Journal.AwaitDurabilityCommitAsync(cancellationToken);
        }

        var onDisk = SegmentLength(1);
        await using var restarted = await StallableJournal.CreateAsync(Dir, groupCommit, cancellationToken);

        _ = await Assert.That(restarted.Journal.EventLoop.ActiveSegmentWrittenBytes).IsEqualTo(onDisk);
        _ = await Assert.That(onDisk).IsGreaterThan(JournalFraming.FileHeaderSize);
    }

    private static async Task AppendDurablyAsync(StallableJournal journal, string key, int payloadLength, CancellationToken cancellationToken)
    {
        await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default(key), new byte[payloadLength], cancellationToken);
        await journal.Journal.AwaitDurabilityCommitAsync(cancellationToken);
    }

    private long SegmentLength(int segmentIndex) => new FileInfo(JournalReadPath.BuildSegmentPath(Dir, segmentIndex)).Length;
}
