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
/// Append admission reads the journal thread's capacity counters from the producer side (issue #703), so the active segment byte counter
/// must equal what the journal thread will see once it opens the segment, even before that open.
/// </summary>
[Immutable]
public sealed class JournalAppendAdmissionTests : IsolatedStorageTestBase
{
    /// <summary>A fresh journal seeds the active segment counter with the header the first open writes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FreshStartSeedsHeaderBytes(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var seeded = journal.Journal.ActiveSegmentWrittenBytes;

        await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), cancellationToken);
        await journal.Journal.AwaitDurabilityCommitAsync(cancellationToken);

        _ = await Assert.That(seeded).IsEqualTo(JournalFraming.FileHeaderSize);
        _ = await Assert.That(journal.Journal.ActiveSegmentWrittenBytes).IsEqualTo(SegmentLength(1));
    }

    /// <summary>After a no-op maintenance, the active segment counter holds the reset segment's length instead of zero.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MaintenanceEndSeedsSegmentBytes(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), cancellationToken);
        await journal.Journal.AwaitDurabilityCommitAsync(cancellationToken);

        await journal.Journal.ExecuteMaintenanceExclusiveAsync(static _ => ValueTask.CompletedTask, cancellationToken);

        _ = await Assert.That(journal.Journal.ActiveSegmentWrittenBytes).IsEqualTo(SegmentLength(1));
        _ = await Assert.That(journal.Journal.ActiveSegmentWrittenBytes).IsGreaterThan(JournalFraming.FileHeaderSize);
    }

    /// <summary>A restart seeds the active segment counter with the current segment's on-disk length before any append opens it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartSeedsSegmentBytes(CancellationToken cancellationToken)
    {
        await using (var first = await StallableJournal.CreateAsync(Dir, false, cancellationToken))
        {
            await first.Journal.AppendPutUnderGateAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), cancellationToken);
            await first.Journal.AppendPutUnderGateAsync(CacheKey.Default("b"), JournalEntryPayloadKit.EncodePut("b"), cancellationToken);
            await first.Journal.AwaitDurabilityCommitAsync(cancellationToken);
        }

        var onDisk = SegmentLength(1);
        await using var restarted = await StallableJournal.CreateAsync(Dir, false, cancellationToken);

        _ = await Assert.That(restarted.Journal.ActiveSegmentWrittenBytes).IsEqualTo(onDisk);
        _ = await Assert.That(onDisk).IsGreaterThan(JournalFraming.FileHeaderSize);
    }

    private long SegmentLength(int segmentIndex) => new FileInfo(JournalReadPath.BuildSegmentPath(Dir, segmentIndex)).Length;
}
