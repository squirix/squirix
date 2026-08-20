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
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Verifies journal coordinator sequence initialization scans only the active manifest journal range.</summary>
[Immutable]
public sealed class JournalNextSequenceInitializationTests : IsolatedStorageTestBase
{
    /// <summary>Disjoint topology (manifest current journal newer than any segment) fails the same way as journal-only recovery.</summary>
    [Fact]
    public async Task InitializationFailsManifestLastAvailableSegment()
    {
        var persistence = NewPersistence(Dir);
        using var manifestStore = new Ledger(persistence);
        var only = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(1UL, "only", "v");
        await BinaryJournalTestSegmentWriter.WriteJournalSegmentAsync(Dir, 1, only);
        await manifestStore.WriteAsync(
            new State
            {
                Format = 1,
                CurrentJournal = 3,
                NextSequence = 2,
                LastSnapshot = null,
            },
            DefaultCancellationToken);

        var manifest = await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken);
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(
            JournalCoordinatorFactory.CreateAsync(persistence, manifest, manifestStore, new JournalStartupGate(), DefaultCancellationToken));

        Assert.Contains("cannot determine a valid replay start", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Next sequence follows records at/after manifest CurrentJournal; obsolete lower segments are not consulted.</summary>
    [Fact]
    public async Task NextSequenceDerivesActiveManifestCurrentJournal()
    {
        var persistence = NewPersistence(Dir);
        using var manifestStore = new Ledger(persistence);
        var old = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(1UL, "old", "a");
        var live = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(5UL, "live", "b");
        var live2 = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(6UL, "live2", "c");
        await BinaryJournalTestSegmentWriter.WriteJournalSegmentAsync(Dir, 1, old);
        await BinaryJournalTestSegmentWriter.WriteJournalSegmentAsync(Dir, 3, [live, live2]);
        var manifest = new State
        {
            Format = 1,
            CurrentJournal = 3,
            NextSequence = 5,
            LastSnapshot = null,
        };
        await manifestStore.WriteAsync(manifest, DefaultCancellationToken);
        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);
        Assert.Equal(7UL, journal.NextSequence);
    }

    /// <summary>LastAppliedSequence from snapshot metadata raises the sequence floor before scanning the active journal tail.</summary>
    [Fact]
    public async Task NextSequenceRespectsSnapshotActiveJournalScan()
    {
        var persistence = NewPersistence(Dir);
        using var manifestStore = new Ledger(persistence);
        var envelope = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(51UL, "k", "v");
        await BinaryJournalTestSegmentWriter.WriteJournalSegmentAsync(Dir, 2, envelope);
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
        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);
        Assert.Equal(52UL, journal.NextSequence);
    }

    /// <summary>Scan start follows the first on-disk segment when it is already above manifest CurrentJournal.</summary>
    [Fact]
    public async Task NextSequenceScanUsesSegmentManifestCurrentJournal()
    {
        var persistence = NewPersistence(Dir);
        using var manifestStore = new Ledger(persistence);
        var envelope = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(20UL, "k", "v");
        await BinaryJournalTestSegmentWriter.WriteJournalSegmentAsync(Dir, 5, envelope);
        var manifest = new State
        {
            Format = 1,
            CurrentJournal = 3,
            NextSequence = 2,
            LastSnapshot = null,
        };
        await manifestStore.WriteAsync(manifest, DefaultCancellationToken);

        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);
        Assert.Equal(21UL, journal.NextSequence);
    }

    /// <summary>After a segment roll recorded in the manifest, a new writer continues monotonic allocation without rereading rolled segments.</summary>
    [Fact]
    public async Task NextSequenceStaysMonotonicManifestSegmentBoundary()
    {
        var persistence = NewPersistence(Dir);
        using var manifestStore = new Ledger(persistence);

        var s1 = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(1UL, "s1", "a");
        var s2 = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(2UL, "s2", "b");
        var s2B = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(3UL, "s2b", "c");
        await BinaryJournalTestSegmentWriter.WriteJournalSegmentAsync(Dir, 1, s1);
        await BinaryJournalTestSegmentWriter.WriteJournalSegmentAsync(Dir, 2, [s2, s2B]);
        var manifest = new State
        {
            Format = 1,
            CurrentJournal = 2,
            NextSequence = 4,
            LastSnapshot = null,
        };
        await manifestStore.WriteAsync(manifest, DefaultCancellationToken);
        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);
        Assert.Equal(4UL, journal.NextSequence);
        Assert.Equal(2, journal.CurrentSegmentIndex);

        var payload = JournalEntryPayloadKit.EncodePut("after");
        await journal.AppendPutAsync(CacheKey.Default("after"), payload, DefaultCancellationToken);
        await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);
        Assert.Equal(5UL, journal.NextSequence);
    }

    /// <summary>CRC corruption in a segment below manifest CurrentJournal does not affect sequence initialization.</summary>
    [Fact]
    public async Task ObsoleteJournalCorruptionBelowAffectNextSequence()
    {
        var persistence = NewPersistence(Dir);
        using var manifestStore = new Ledger(persistence);
        var obsoletePath = NodePathKit.Combine(Dir, $"{FilePrefixes.Journal}000001{FileExtensions.Journal}");
        var stale = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(1UL, "stale", "x");
        await BinaryJournalTestSegmentWriter.WriteSegmentAsync(obsoletePath, stale);
        var bytes = await File.ReadAllBytesAsync(obsoletePath, DefaultCancellationToken);
        bytes[^1] ^= 0xFF;
        await File.WriteAllBytesAsync(obsoletePath, bytes, DefaultCancellationToken);

        var live = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(10UL, "live", "y");
        await BinaryJournalTestSegmentWriter.WriteJournalSegmentAsync(Dir, 2, live);
        var manifest = new State
        {
            Format = 1,
            CurrentJournal = 2,
            NextSequence = 10,
            LastSnapshot = null,
        };
        await manifestStore.WriteAsync(manifest, DefaultCancellationToken);
        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);
        Assert.Equal(11UL, journal.NextSequence);
    }

    /// <summary>After compaction, sequence initialization matches the compacted tail without reading deleted lower segments.</summary>
    [Fact]
    public async Task PostCompactionNextSequenceManifestObsoleteSegments()
    {
        var persistence = NewPersistence(Dir);
        using var manifestStore = new Ledger(persistence);

        await using (var journal = await JournalCoordinatorFactory.CreateAsync(
                         persistence,
                         await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
                         manifestStore,
                         new JournalStartupGate(),
                         DefaultCancellationToken))
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

        await using var restartedJournal = await JournalCoordinatorFactory.CreateAsync(persistence, manifest, manifestStore, new JournalStartupGate(), DefaultCancellationToken);
        Assert.Equal(maxSeq + 1, restartedJournal.NextSequence);
        Assert.Equal(manifest.CurrentJournal, restartedJournal.CurrentSegmentIndex);
    }

    /// <summary>Truncated tail in the active segment still caps discovered sequence the same way as full-file replay.</summary>
    [Fact]
    public async Task TruncatedFrameActiveJournalBoundsNextSequence()
    {
        var persistence = NewPersistence(Dir);
        using var manifestStore = new Ledger(persistence);
        var path = NodePathKit.Combine(Dir, $"{FilePrefixes.Journal}000002{FileExtensions.Journal}");
        var a = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(5UL, "a", "x");
        var b = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(6UL, "b", "y");
        await BinaryJournalTestSegmentWriter.WriteSegmentAsync(path, [a, b]);
        await using (var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
            fs.SetLength(fs.Length - 1);

        await manifestStore.WriteAsync(
            new State
            {
                Format = 1,
                CurrentJournal = 2,
                NextSequence = 5,
                LastSnapshot = null,
            },
            DefaultCancellationToken);

        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);
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
