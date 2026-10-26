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
/// Append admission reads the journal thread's capacity state before the journal thread opens the current segment, so at
/// startup the active segment counter and the open flag must already describe what that open will find.
/// </summary>
[Immutable]
public sealed class JournalSegmentSeedTests : IsolatedStorageTestBase
{
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

    private long SegmentLength(int segmentIndex) => new FileInfo(JournalReadPath.BuildSegmentPath(Dir, segmentIndex)).Length;
}
