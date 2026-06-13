using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Json;
using Squirix.Server.Storage.JournalProto;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Storage.Journaling;

/// <summary>
/// Verifies journal writer sequence initialization scans only the active manifest journal range.
/// </summary>
public sealed class JournalWriterNextSequenceInitializationTests : ServerUnitTestBase
{
    /// <summary>
    /// Disjoint topology (manifest current journal newer than any segment) fails the same way as journal-only recovery.
    /// </summary>
    [Fact]
    public void InitializationFailsWhenManifestCurrentJournalIsNewerThanLastAvailableSegment()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-journal-next-seq-disjoint");
        try
        {
            var persistence = NewPersistence(dir);
            var manifestStore = new ManifestStore(persistence);
            WriteJournalSegment(dir, 1, [BuildPutEnvelope(1UL, "only", "v")]);
            manifestStore.Write(
                new Manifest
                {
                    Format = 1,
                    CurrentJournal = 3,
                    NextSequence = 2,
                    LastSnapshot = null,
                });

            var ex = Assert.Throws<InvalidDataException>(() => _ = new JournalWriter(persistence, manifestStore.ReadCurrentOrDefault(), manifestStore, new JournalStartupGate()));

            Assert.Contains("manifestCurrentJournal=3", ex.Message, StringComparison.Ordinal);
            Assert.Contains("firstAvailableJournal=1", ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// Next sequence follows records at/after manifest CurrentJournal; obsolete lower segments are not consulted.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task NextSequenceDerivesFromActiveJournalRangeStartingAtManifestCurrentJournal()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-journal-next-seq-active-range");
        try
        {
            var persistence = NewPersistence(dir);
            var manifestStore = new ManifestStore(persistence);
            WriteJournalSegment(dir, 1, [BuildPutEnvelope(1UL, "old", "a")]);
            WriteJournalSegment(dir, 3, [BuildPutEnvelope(5UL, "live", "b"), BuildPutEnvelope(6UL, "live2", "c")]);
            manifestStore.Write(
                new Manifest
                {
                    Format = 1,
                    CurrentJournal = 3,
                    NextSequence = 5,
                    LastSnapshot = null,
                });

            await using var journal = new JournalWriter(persistence, manifestStore.ReadCurrentOrDefault(), manifestStore, new JournalStartupGate());
            Assert.Equal(7UL, journal.NextSequence);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// LastAppliedSequence from snapshot metadata raises the sequence floor before scanning the active journal tail.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task NextSequenceRespectsSnapshotLastAppliedSequenceBeforeActiveJournalScan()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-journal-next-seq-snap-watermark");
        try
        {
            var persistence = NewPersistence(dir);
            var manifestStore = new ManifestStore(persistence);
            WriteJournalSegment(dir, 2, [BuildPutEnvelope(51UL, "k", "v")]);
            manifestStore.Write(
                new Manifest
                {
                    Format = 1,
                    CurrentJournal = 2,
                    NextSequence = 1,
                    LastSnapshot = new Manifest.SnapshotRef
                    {
                        Index = 0,
                        CreatedUtc = DateTime.UtcNow,
                        LastAppliedSequence = 50,
                        Path = null,
                        ReplayFromJournalSegment = 1,
                    },
                });

            await using var journal = new JournalWriter(persistence, manifestStore.ReadCurrentOrDefault(), manifestStore, new JournalStartupGate());
            Assert.Equal(52UL, journal.NextSequence);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// Scan start follows the first on-disk segment when it is already above manifest CurrentJournal.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task NextSequenceScanUsesMaxOfFirstAvailableSegmentAndManifestCurrentJournal()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-journal-next-seq-first-available");
        try
        {
            var persistence = NewPersistence(dir);
            var manifestStore = new ManifestStore(persistence);
            WriteJournalSegment(dir, 5, [BuildPutEnvelope(20UL, "k", "v")]);
            manifestStore.Write(
                new Manifest
                {
                    Format = 1,
                    CurrentJournal = 3,
                    NextSequence = 2,
                    LastSnapshot = null,
                });

            await using var journal = new JournalWriter(persistence, manifestStore.ReadCurrentOrDefault(), manifestStore, new JournalStartupGate());
            Assert.Equal(21UL, journal.NextSequence);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// After a segment roll recorded in the manifest, a new writer continues monotonic allocation without rereading rolled segments.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task NextSequenceStaysMonotonicAcrossManifestSegmentBoundary()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-journal-next-seq-roll-boundary");
        try
        {
            var persistence = NewPersistence(dir);
            var manifestStore = new ManifestStore(persistence);

            WriteJournalSegment(dir, 1, [BuildPutEnvelope(1UL, "s1", "a")]);
            WriteJournalSegment(dir, 2, [BuildPutEnvelope(2UL, "s2", "b"), BuildPutEnvelope(3UL, "s2b", "c")]);
            manifestStore.Write(
                new Manifest
                {
                    Format = 1,
                    CurrentJournal = 2,
                    NextSequence = 4,
                    LastSnapshot = null,
                });

            await using var journal = new JournalWriter(persistence, manifestStore.ReadCurrentOrDefault(), manifestStore, new JournalStartupGate());
            Assert.Equal(4UL, journal.NextSequence);
            Assert.Equal(2, journal.CurrentSegmentIndex);

            var payload = DiscriminatedEntryJsonWriter.BuildEntryJson("after", null, null, 1, null);
            await journal.AppendPutAsync(CacheKey.Default("after"), payload, null, DefaultCancellationToken);
            await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);
            Assert.Equal(5UL, journal.NextSequence);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// CRC corruption in a segment below manifest CurrentJournal does not affect sequence initialization.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task ObsoleteJournalCorruptionBelowManifestCurrentJournalDoesNotAffectNextSequence()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-journal-next-seq-obsolete-crc");
        try
        {
            var persistence = NewPersistence(dir);
            var manifestStore = new ManifestStore(persistence);
            var obsoletePath = PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}000001{StorageFileExtensions.Journal}");
            WriteSegmentWithFrames(obsoletePath, [BuildPutEnvelope(1UL, "stale", "x")]);
            var bytes = await File.ReadAllBytesAsync(obsoletePath, DefaultCancellationToken);
            bytes[^1] ^= 0xFF;
            await File.WriteAllBytesAsync(obsoletePath, bytes, DefaultCancellationToken);

            WriteJournalSegment(dir, 2, [BuildPutEnvelope(10UL, "live", "y")]);
            manifestStore.Write(
                new Manifest
                {
                    Format = 1,
                    CurrentJournal = 2,
                    NextSequence = 10,
                    LastSnapshot = null,
                });

            await using var journal = new JournalWriter(persistence, manifestStore.ReadCurrentOrDefault(), manifestStore, new JournalStartupGate());
            Assert.Equal(11UL, journal.NextSequence);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// After compaction, sequence initialization matches the compacted tail without reading deleted lower segments.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task PostCompactionNextSequenceMatchesManifestWithoutObsoleteSegments()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-journal-next-seq-post-compact");
        try
        {
            var persistence = NewPersistence(dir);
            var manifestStore = new ManifestStore(persistence);

            await using (var journal = new JournalWriter(persistence, manifestStore.ReadCurrentOrDefault(), manifestStore, new JournalStartupGate()))
            {
                var p = DiscriminatedEntryJsonWriter.BuildEntryJson("keep", null, null, 1, null);
                await journal.AppendPutAsync(CacheKey.Default("keep"), p, null, DefaultCancellationToken);
                await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);
            }

            await JournalCompactor.CompactAsync(persistence, manifestStore, DefaultCancellationToken);

            var manifest = manifestStore.ReadCurrentOrDefault();
            var maxSeq = 0UL;
            foreach (var env in JournalReader.ReadAll(persistence.DataDir, manifest.CurrentJournal, DefaultCancellationToken))
            {
                if (env.Seq > maxSeq)
                    maxSeq = env.Seq;
            }

            await using var restartedJournal = new JournalWriter(persistence, manifest, manifestStore, new JournalStartupGate());
            Assert.Equal(maxSeq + 1, restartedJournal.NextSequence);
            Assert.Equal(manifest.CurrentJournal, restartedJournal.CurrentSegmentIndex);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// Truncated tail in the active segment still caps discovered sequence the same way as full-file replay.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task TruncatedFrameInActiveJournalSegmentBoundsNextSequence()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-journal-next-seq-active-truncate");
        try
        {
            var persistence = NewPersistence(dir);
            var manifestStore = new ManifestStore(persistence);
            var path = PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}000002{StorageFileExtensions.Journal}");
            WriteSegmentWithFrames(path, [BuildPutEnvelope(5UL, "a", "x"), BuildPutEnvelope(6UL, "b", "y")]);
            await using (var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
            {
                fs.SetLength(fs.Length - 1);
            }

            manifestStore.Write(
                new Manifest
                {
                    Format = 1,
                    CurrentJournal = 2,
                    NextSequence = 5,
                    LastSnapshot = null,
                });

            await using var journal = new JournalWriter(persistence, manifestStore.ReadCurrentOrDefault(), manifestStore, new JournalStartupGate());
            Assert.Equal(6UL, journal.NextSequence);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    private static JournalEnvelope BuildPutEnvelope(ulong seq, string key, string value)
    {
        var body = DiscriminatedEntryJsonWriter.BuildEntryJson(value, null, null, 1, null);
        return new JournalEnvelope
        {
            Seq = seq,
            UnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Put = new Put
            {
                Item = new EntryPair
                {
                    Key = key,
                    Namespace = CacheNames.DefaultNamespace,
                    EntryJson = ByteString.CopyFrom(body),
                },
            },
        };
    }

    private static PersistenceOptions NewPersistence(string dataDir) => new()
    {
        DataDir = dataDir,
        JournalMaxSegmentMb = 16,
        FlushIntervalMs = 5,
        ManifestRetentionCount = 1,
    };

    private static void WriteJournalSegment(string dir, int index, IReadOnlyList<JournalEnvelope> envelopes)
    {
        var path = PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}{index:000000}{StorageFileExtensions.Journal}");
        WriteSegmentWithFrames(path, envelopes);
    }

    private static void WriteSegmentWithFrames(string path, IReadOnlyList<JournalEnvelope> envelopes)
    {
        using var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read);
        JournalFraming.WriteFileHeader(stream);
        foreach (var envelope in envelopes)
        {
            var payload = RecordCodec.Serialize(envelope);
            JournalFraming.WriteFrame(stream, payload);
        }

        stream.Flush(true);
    }
}
