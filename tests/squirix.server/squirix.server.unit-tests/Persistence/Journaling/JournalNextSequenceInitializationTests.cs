using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Verifies journal coordinator sequence initialization scans only the active manifest journal range.</summary>
[Immutable]
public sealed class JournalNextSequenceInitializationTests : IsolatedStorageTestBase
{
    /// <summary>Disjoint topology (manifest current journal newer than any segment) fails the same way as journal-only recovery.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InitFailsOnMissingLastSegment(CancellationToken cancellationToken)
    {
        var persistence = NewPersistence(Dir);
        using var manifestStore = new Ledger(persistence);
        var only = BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "only", "v");
        BinaryJournalTestSegmentWriter.WriteJournalSegment(Dir, 1, only);
        var state = new State
        {
            Format = 1,
            CurrentJournal = 3,
            NextSequence = 2,
            LastSnapshot = null,
        };
        await manifestStore.WriteAsync(state, cancellationToken);
        var manifest = await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken);
        var ex = NodeExceptionAssert.For<InvalidDataException>().Throws(
            (persistence, manifest, manifestStore),
            static p => JournalCoordinatorFactory.Create(p.persistence, p.manifest, p.manifestStore, new AsyncManualResetEvent(true)));

        _ = await Assert.That(ex.Message).Contains("cannot determine a valid replay start", StringComparison.Ordinal);
    }

    /// <summary>CRC corruption in a segment below manifest CurrentJournal does not affect sequence initialization.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ObsoleteSegmentCorruptionIgnored(CancellationToken cancellationToken)
    {
        var persistence = NewPersistence(Dir);
        using var manifestStore = new Ledger(persistence);
        var obsoletePath = NodePathKit.Combine(Dir, $"{FilePrefixes.Journal}000001{FileExtensions.Journal}");
        var stale = BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "stale", "x");
        BinaryJournalTestSegmentWriter.WriteSegment(obsoletePath, stale);
        var bytes = await File.ReadAllBytesAsync(obsoletePath, cancellationToken);
        bytes[^1] ^= 0xFF;
        await File.WriteAllBytesAsync(obsoletePath, bytes, cancellationToken);

        var live = BinaryJournalTestSegmentWriter.BuildPutRecord(10UL, "live", "y");
        BinaryJournalTestSegmentWriter.WriteJournalSegment(Dir, 2, live);
        var manifest = new State
        {
            Format = 1,
            CurrentJournal = 2,
            NextSequence = 10,
            LastSnapshot = null,
        };
        await manifestStore.WriteAsync(manifest, cancellationToken);
        await using var journal = JournalCoordinatorFactory.Create(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        _ = await Assert.That(journal.NextSequence).IsEqualTo(11UL);
    }

    /// <summary>After compaction, sequence initialization matches the compacted tail without reading deleted lower segments.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PostCompactionSequenceSkipsObsolete(CancellationToken cancellationToken)
    {
        var persistence = NewPersistence(Dir);
        using var manifestStore = new Ledger(persistence);

        await using (var journal = JournalCoordinatorFactory.Create(
                         persistence,
                         await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
                         manifestStore,
                         new AsyncManualResetEvent(true)))
        {
            var p = JournalEntryPayloadKit.EncodePut("keep");
            await journal.AppendPutUnderGateAsync(CacheKey.Default("keep"), p, cancellationToken);
            await journal.AwaitDurabilityCommitAsync(cancellationToken);
        }

        await JournalCompactor.CompactAsync(persistence, manifestStore, StoreFactory.CreateReader(), cancellationToken);

        var manifest = await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken);
        var maxSeq = 0UL;
        using var records = JournalReadPath.ReadAll(persistence.DataDir, manifest.CurrentJournal, cancellationToken);
        while (records.MoveNext())
        {
            var record = records.Current;
            if (record.Sequence > maxSeq)
                maxSeq = record.Sequence;
        }

        await using var restartedJournal = JournalCoordinatorFactory.Create(persistence, manifest, manifestStore, new AsyncManualResetEvent(true));
        _ = await Assert.That(restartedJournal.NextSequence).IsEqualTo(maxSeq + 1);
        _ = await Assert.That(restartedJournal.CurrentSegmentIndex).IsEqualTo(manifest.CurrentJournal);
    }

    /// <summary>Scan start follows the first on-disk segment when it is already above manifest CurrentJournal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ScanDerivesSequenceManifestJournal(CancellationToken cancellationToken)
    {
        var persistence = NewPersistence(Dir);
        using var manifestStore = new Ledger(persistence);
        var envelope = BinaryJournalTestSegmentWriter.BuildPutRecord(20UL, "k", "v");
        BinaryJournalTestSegmentWriter.WriteJournalSegment(Dir, 5, envelope);
        var manifest = new State
        {
            Format = 1,
            CurrentJournal = 3,
            NextSequence = 2,
            LastSnapshot = null,
        };
        await manifestStore.WriteAsync(manifest, cancellationToken);

        await using var journal = JournalCoordinatorFactory.Create(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        _ = await Assert.That(journal.NextSequence).IsEqualTo(21UL);
    }

    /// <summary>The next sequence follows records at/after manifest CurrentJournal; obsolete lower segments are not consulted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SequenceDerivesActiveManifestJournal(CancellationToken cancellationToken)
    {
        var persistence = NewPersistence(Dir);
        using var manifestStore = new Ledger(persistence);
        var old = BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "old", "a");
        var live = BinaryJournalTestSegmentWriter.BuildPutRecord(5UL, "live", "b");
        var live2 = BinaryJournalTestSegmentWriter.BuildPutRecord(6UL, "live2", "c");
        BinaryJournalTestSegmentWriter.WriteJournalSegment(Dir, 1, old);
        BinaryJournalTestSegmentWriter.WriteJournalSegment(Dir, 3, [live, live2]);
        var manifest = new State
        {
            Format = 1,
            CurrentJournal = 3,
            NextSequence = 5,
            LastSnapshot = null,
        };
        await manifestStore.WriteAsync(manifest, cancellationToken);
        await using var journal = JournalCoordinatorFactory.Create(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        _ = await Assert.That(journal.NextSequence).IsEqualTo(7UL);
    }

    /// <summary>After a segment roll recorded in the manifest, a new writer continues monotonic allocation without rereading rolled segments.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SequenceMonotonicAcrossSegmentRoll(CancellationToken cancellationToken)
    {
        var persistence = NewPersistence(Dir);
        using var manifestStore = new Ledger(persistence);

        var s1 = BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "s1", "a");
        var s2 = BinaryJournalTestSegmentWriter.BuildPutRecord(2UL, "s2", "b");
        var s2B = BinaryJournalTestSegmentWriter.BuildPutRecord(3UL, "s2b", "c");
        BinaryJournalTestSegmentWriter.WriteJournalSegment(Dir, 1, s1);
        BinaryJournalTestSegmentWriter.WriteJournalSegment(Dir, 2, [s2, s2B]);
        var manifest = new State
        {
            Format = 1,
            CurrentJournal = 2,
            NextSequence = 4,
            LastSnapshot = null,
        };
        await manifestStore.WriteAsync(manifest, cancellationToken);
        await using var journal = JournalCoordinatorFactory.Create(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        _ = await Assert.That(journal.NextSequence).IsEqualTo(4UL);
        _ = await Assert.That(journal.CurrentSegmentIndex).IsEqualTo(2);

        var payload = JournalEntryPayloadKit.EncodePut("after");
        await journal.AppendPutUnderGateAsync(CacheKey.Default("after"), payload, cancellationToken);
        await journal.AwaitDurabilityCommitAsync(cancellationToken);
        _ = await Assert.That(journal.NextSequence).IsEqualTo(5UL);
    }

    /// <summary>LastAppliedSequence from snapshot metadata raises the sequence floor before scanning the active journal tail.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SequenceRespectsSnapshotScan(CancellationToken cancellationToken)
    {
        var persistence = NewPersistence(Dir);
        using var manifestStore = new Ledger(persistence);
        var envelope = BinaryJournalTestSegmentWriter.BuildPutRecord(51UL, "k", "v");
        BinaryJournalTestSegmentWriter.WriteJournalSegment(Dir, 2, envelope);
        var manifest = new State
        {
            Format = 1,
            CurrentJournal = 2,
            NextSequence = 1,
            LastSnapshot = new SnapshotRef
            {
                Index = 0,
                CreatedUtc = DateTime.UtcNow,
                LastAppliedSequence = 50,
                Path = null,
                ReplayFromJournalSegment = 1,
            },
        };
        await manifestStore.WriteAsync(manifest, cancellationToken);
        await using var journal = JournalCoordinatorFactory.Create(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        _ = await Assert.That(journal.NextSequence).IsEqualTo(52UL);
    }

    /// <summary>Truncated tail in the active segment still caps the discovered sequence the same way as full-file replay.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TruncatedTailBoundsSequence(CancellationToken cancellationToken)
    {
        var persistence = NewPersistence(Dir);
        using var manifestStore = new Ledger(persistence);
        var path = NodePathKit.Combine(Dir, $"{FilePrefixes.Journal}000002{FileExtensions.Journal}");
        var a = BinaryJournalTestSegmentWriter.BuildPutRecord(5UL, "a", "x");
        var b = BinaryJournalTestSegmentWriter.BuildPutRecord(6UL, "b", "y");
        BinaryJournalTestSegmentWriter.WriteSegment(path, [a, b]);
        using (var handle = File.OpenHandle(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
            RandomAccess.SetLength(handle, RandomAccess.GetLength(handle) - 1);

        var manifest = new State
        {
            Format = 1,
            CurrentJournal = 2,
            NextSequence = 5,
            LastSnapshot = null,
        };
        await manifestStore.WriteAsync(manifest, cancellationToken);

        await using var journal = JournalCoordinatorFactory.Create(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        _ = await Assert.That(journal.NextSequence).IsEqualTo(6UL);
    }

    private static PersistenceOptions NewPersistence(string dataDir) => new()
    {
        DataDir = dataDir,
        JournalMaxSegmentMb = 16,
        FlushInterval = 5,
        ManifestRetentionCount = 1,
    };
}
