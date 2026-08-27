using System;
using System.IO;
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
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Verifies journal coordinator sequence initialization scans only the active manifest journal range.</summary>
[Immutable]
public sealed class JournalNextSequenceInitializationTests : IsolatedStorageTestBase
{
    /// <summary>Disjoint topology (manifest current journal newer than any segment) fails the same way as journal-only recovery.</summary>
    [Fact]
    public async Task InitFailsOnMissingLastSegment()
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
        await manifestStore.WriteAsync(state, DefaultCancellationToken);
        var manifest = await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken);
        var ex = NodeExceptionAssert.For<InvalidDataException>().Throws(
            (persistence, manifest, manifestStore),
            static p => JournalCoordinatorFactory.Create(p.persistence, p.manifest, p.manifestStore, new AsyncManualResetEvent(true)));

        Assert.Contains("cannot determine a valid replay start", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>CRC corruption in a segment below manifest CurrentJournal does not affect sequence initialization.</summary>
    [Fact]
    public async Task ObsoleteSegmentCorruptionIgnored()
    {
        var persistence = NewPersistence(Dir);
        using var manifestStore = new Ledger(persistence);
        var obsoletePath = NodePathKit.Combine(Dir, $"{FilePrefixes.Journal}000001{FileExtensions.Journal}");
        var stale = BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "stale", "x");
        BinaryJournalTestSegmentWriter.WriteSegment(obsoletePath, stale);
        var bytes = await File.ReadAllBytesAsync(obsoletePath, DefaultCancellationToken);
        bytes[^1] ^= 0xFF;
        await File.WriteAllBytesAsync(obsoletePath, bytes, DefaultCancellationToken);

        var live = BinaryJournalTestSegmentWriter.BuildPutRecord(10UL, "live", "y");
        BinaryJournalTestSegmentWriter.WriteJournalSegment(Dir, 2, live);
        var manifest = new State
        {
            Format = 1,
            CurrentJournal = 2,
            NextSequence = 10,
            LastSnapshot = null,
        };
        await manifestStore.WriteAsync(manifest, DefaultCancellationToken);
        await using var journal = JournalCoordinatorFactory.Create(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        Assert.Equal(11UL, journal.NextSequence);
    }

    /// <summary>After compaction, sequence initialization matches the compacted tail without reading deleted lower segments.</summary>
    [Fact]
    public async Task PostCompactionSequenceSkipsObsolete()
    {
        var persistence = NewPersistence(Dir);
        using var manifestStore = new Ledger(persistence);

        await using (var journal = JournalCoordinatorFactory.Create(
                         persistence,
                         await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
                         manifestStore,
                         new AsyncManualResetEvent(true)))
        {
            var p = JournalEntryPayloadKit.EncodePut("keep");
            await journal.AppendPutAsync(CacheKey.Default("keep"), p, DefaultCancellationToken);
            await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);
        }

        await JournalCompactor.CompactAsync(persistence, manifestStore, StoreFactory.CreateReader(persistence), DefaultCancellationToken);

        var manifest = await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken);
        var maxSeq = 0UL;
        using var records = JournalReadPath.ReadAll(persistence.DataDir, manifest.CurrentJournal, DefaultCancellationToken);
        while (records.MoveNext())
        {
            var record = records.Current;
            if (record.Sequence > maxSeq)
                maxSeq = record.Sequence;
        }

        await using var restartedJournal = JournalCoordinatorFactory.Create(persistence, manifest, manifestStore, new AsyncManualResetEvent(true));
        Assert.Equal(maxSeq + 1, restartedJournal.NextSequence);
        Assert.Equal(manifest.CurrentJournal, restartedJournal.CurrentSegmentIndex);
    }

    /// <summary>Scan start follows the first on-disk segment when it is already above manifest CurrentJournal.</summary>
    [Fact]
    public async Task ScanDerivesSequenceManifestJournal()
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
        await manifestStore.WriteAsync(manifest, DefaultCancellationToken);

        await using var journal = JournalCoordinatorFactory.Create(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        Assert.Equal(21UL, journal.NextSequence);
    }

    /// <summary>The next sequence follows records at/after manifest CurrentJournal; obsolete lower segments are not consulted.</summary>
    [Fact]
    public async Task SequenceDerivesActiveManifestJournal()
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
        await manifestStore.WriteAsync(manifest, DefaultCancellationToken);
        await using var journal = JournalCoordinatorFactory.Create(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        Assert.Equal(7UL, journal.NextSequence);
    }

    /// <summary>After a segment roll recorded in the manifest, a new writer continues monotonic allocation without rereading rolled segments.</summary>
    [Fact]
    public async Task SequenceMonotonicAcrossSegmentRoll()
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
        await manifestStore.WriteAsync(manifest, DefaultCancellationToken);
        await using var journal = JournalCoordinatorFactory.Create(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        Assert.Equal(4UL, journal.NextSequence);
        Assert.Equal(2, journal.CurrentSegmentIndex);

        var payload = JournalEntryPayloadKit.EncodePut("after");
        await journal.AppendPutAsync(CacheKey.Default("after"), payload, DefaultCancellationToken);
        await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);
        Assert.Equal(5UL, journal.NextSequence);
    }

    /// <summary>LastAppliedSequence from snapshot metadata raises the sequence floor before scanning the active journal tail.</summary>
    [Fact]
    public async Task SequenceRespectsSnapshotScan()
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
        await manifestStore.WriteAsync(manifest, DefaultCancellationToken);
        await using var journal = JournalCoordinatorFactory.Create(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        Assert.Equal(52UL, journal.NextSequence);
    }

    /// <summary>Truncated tail in the active segment still caps the discovered sequence the same way as full-file replay.</summary>
    [Fact]
    public async Task TruncatedTailBoundsSequence()
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
        await manifestStore.WriteAsync(manifest, DefaultCancellationToken);

        await using var journal = JournalCoordinatorFactory.Create(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        Assert.Equal(6UL, journal.NextSequence);
    }

    private static PersistenceOptions NewPersistence(string dataDir) => new()
    {
        DataDir = dataDir,
        JournalMaxSegmentMb = 16,
        FlushIntervalMs = 5,
        ManifestRetentionCount = 1,
    };
}
