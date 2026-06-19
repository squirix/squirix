using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Json;
using Squirix.Server.Storage.Journaling.PipelinedWal;
using Squirix.Server.Storage.JournalProto;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Verifies journal writer sequence initialization scans only the active manifest journal range.</summary>
public sealed class JournalWriterNextSequenceInitializationTests : UnitTestBase
{
    /// <summary>Disjoint topology (manifest current journal newer than any segment) fails the same way as journal-only recovery.</summary>
    [Fact]
    public async Task InitializationFailsWhenManifestCurrentJournalIsNewerThanLastAvailableSegment()
    {
        using var dir = new TempDirectory("squirix-journal-next-seq-disjoint");
        var persistence = NewPersistence(dir);
        using var manifestStore = new ManifestStore(persistence);
        var only = await BuildPutEnvelopeAsync(1UL, "only", "v");
        await WriteJournalSegmentAsync(dir, 1, [only]);
        await manifestStore.WriteAsync(
            new Manifest
            {
                Format = 1,
                CurrentJournal = 3,
                NextSequence = 2,
                LastSnapshot = null,
            },
            DefaultCancellationToken);

        var ex = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            _ = await JournalWriter.CreateAsync(
                persistence,
                await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
                manifestStore,
                new JournalStartupGate(),
                DefaultCancellationToken));

        Assert.Contains("manifestCurrentJournal=3", ex.Message, StringComparison.Ordinal);
        Assert.Contains("firstAvailableJournal=1", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Next sequence follows records at/after manifest CurrentJournal; obsolete lower segments are not consulted.</summary>
    [Fact]
    public async Task NextSequenceDerivesFromActiveJournalRangeStartingAtManifestCurrentJournal()
    {
        using var dir = new TempDirectory("squirix-journal-next-seq-active-range");
        var persistence = NewPersistence(dir);
        using var manifestStore = new ManifestStore(persistence);
        var old = await BuildPutEnvelopeAsync(1UL, "old", "a");
        var live = await BuildPutEnvelopeAsync(5UL, "live", "b");
        var live2 = await BuildPutEnvelopeAsync(6UL, "live2", "c");
        await WriteJournalSegmentAsync(dir, 1, [old]);
        await WriteJournalSegmentAsync(dir, 3, [live, live2]);
        var manifest = new Manifest
        {
            Format = 1,
            CurrentJournal = 3,
            NextSequence = 5,
            LastSnapshot = null,
        };
        await manifestStore.WriteAsync(manifest, DefaultCancellationToken);
        await using var journal = await JournalWriter.CreateAsync(persistence, await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken), manifestStore, new JournalStartupGate(), DefaultCancellationToken);
        Assert.Equal(7UL, journal.NextSequence);
    }

    /// <summary>LastAppliedSequence from snapshot metadata raises the sequence floor before scanning the active journal tail.</summary>
    [Fact]
    public async Task NextSequenceRespectsSnapshotLastAppliedSequenceBeforeActiveJournalScan()
    {
        using var dir = new TempDirectory("squirix-journal-next-seq-snap-watermark");
        var persistence = NewPersistence(dir);
        using var manifestStore = new ManifestStore(persistence);
        var envelope = await BuildPutEnvelopeAsync(51UL, "k", "v");
        await WriteJournalSegmentAsync(dir, 2, [envelope]);
        var manifest = new Manifest
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
        };
        await manifestStore.WriteAsync(manifest, DefaultCancellationToken);
        await using var journal = await JournalWriter.CreateAsync(persistence, await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken), manifestStore, new JournalStartupGate(), DefaultCancellationToken);
        Assert.Equal(52UL, journal.NextSequence);
    }

    /// <summary>Scan start follows the first on-disk segment when it is already above manifest CurrentJournal.</summary>
    [Fact]
    public async Task NextSequenceScanUsesMaxOfFirstAvailableSegmentAndManifestCurrentJournal()
    {
        using var dir = new TempDirectory("squirix-journal-next-seq-first-available");
        var persistence = NewPersistence(dir);
        using var manifestStore = new ManifestStore(persistence);
        var envelope = await BuildPutEnvelopeAsync(20UL, "k", "v");
        await WriteJournalSegmentAsync(dir, 5, [envelope]);
        var manifest = new Manifest
        {
            Format = 1,
            CurrentJournal = 3,
            NextSequence = 2,
            LastSnapshot = null,
        };
        await manifestStore.WriteAsync(manifest, DefaultCancellationToken);

        await using var journal = await JournalWriter.CreateAsync(persistence, await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken), manifestStore, new JournalStartupGate(), DefaultCancellationToken);
        Assert.Equal(21UL, journal.NextSequence);
    }

    /// <summary>After a segment roll recorded in the manifest, a new writer continues monotonic allocation without rereading rolled segments.</summary>
    [Fact]
    public async Task NextSequenceStaysMonotonicAcrossManifestSegmentBoundary()
    {
        using var dir = new TempDirectory("squirix-journal-next-seq-roll-boundary");
        var persistence = NewPersistence(dir);
        using var manifestStore = new ManifestStore(persistence);

        var s1 = await BuildPutEnvelopeAsync(1UL, "s1", "a");
        var s2 = await BuildPutEnvelopeAsync(2UL, "s2", "b");
        var s2B = await BuildPutEnvelopeAsync(3UL, "s2b", "c");
        await WriteJournalSegmentAsync(dir, 1, [s1]);
        await WriteJournalSegmentAsync(dir, 2, [s2, s2B]);
        var manifest = new Manifest
        {
            Format = 1,
            CurrentJournal = 2,
            NextSequence = 4,
            LastSnapshot = null,
        };
        await manifestStore.WriteAsync(manifest, DefaultCancellationToken);
        await using var journal = await JournalWriter.CreateAsync(persistence, await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken), manifestStore, new JournalStartupGate(), DefaultCancellationToken);
        Assert.Equal(4UL, journal.NextSequence);
        Assert.Equal(2, journal.CurrentSegmentIndex);

        var payload = await DiscriminatedEntryJsonWriter.BuildEntryJsonAsync("after", null, null, 1, null);
        await journal.AppendPutAsync(CacheKey.Default("after"), payload, null, DefaultCancellationToken);
        await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);
        Assert.Equal(5UL, journal.NextSequence);
    }

    /// <summary>CRC corruption in a segment below manifest CurrentJournal does not affect sequence initialization.</summary>
    [Fact]
    public async Task ObsoleteJournalCorruptionBelowManifestCurrentJournalDoesNotAffectNextSequence()
    {
        using var dir = new TempDirectory("squirix-journal-next-seq-obsolete-crc");
        var persistence = NewPersistence(dir);
        using var manifestStore = new ManifestStore(persistence);
        var obsoletePath = PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}000001{StorageFileExtensions.Journal}");
        var stale = await BuildPutEnvelopeAsync(1UL, "stale", "x");
        await WriteSegmentWithFramesAsync(obsoletePath, [stale]);
        var bytes = await File.ReadAllBytesAsync(obsoletePath, DefaultCancellationToken);
        bytes[^1] ^= 0xFF;
        await File.WriteAllBytesAsync(obsoletePath, bytes, DefaultCancellationToken);

        var live = await BuildPutEnvelopeAsync(10UL, "live", "y");
        await WriteJournalSegmentAsync(dir, 2, [live]);
        var manifest = new Manifest
        {
            Format = 1,
            CurrentJournal = 2,
            NextSequence = 10,
            LastSnapshot = null,
        };
        await manifestStore.WriteAsync(manifest, DefaultCancellationToken);
        await using var journal = await JournalWriter.CreateAsync(persistence, await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken), manifestStore, new JournalStartupGate(), DefaultCancellationToken);
        Assert.Equal(11UL, journal.NextSequence);
    }

    /// <summary>After compaction, sequence initialization matches the compacted tail without reading deleted lower segments.</summary>
    [Fact]
    public async Task PostCompactionNextSequenceMatchesManifestWithoutObsoleteSegments()
    {
        using var dir = new TempDirectory("squirix-journal-next-seq-post-compact");
        var persistence = NewPersistence(dir);
        using var manifestStore = new ManifestStore(persistence);

        await using (var journal = await JournalWriter.CreateAsync(persistence, await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken), manifestStore, new JournalStartupGate(), DefaultCancellationToken))
        {
            var p = await DiscriminatedEntryJsonWriter.BuildEntryJsonAsync("keep", null, null, 1, null);
            await journal.AppendPutAsync(CacheKey.Default("keep"), p, null, DefaultCancellationToken);
            await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);
        }

        await JournalCompactor.CompactAsync(persistence, manifestStore, DefaultCancellationToken);

        var manifest = await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken);
        var maxSeq = 0UL;
        foreach (var env in JournalReader.ReadAll(persistence.DataDir, manifest.CurrentJournal, DefaultCancellationToken))
        {
            if (env.Seq > maxSeq)
                maxSeq = env.Seq;
        }

        await using var restartedJournal = await JournalWriter.CreateAsync(persistence, manifest, manifestStore, new JournalStartupGate(), DefaultCancellationToken);
        Assert.Equal(maxSeq + 1, restartedJournal.NextSequence);
        Assert.Equal(manifest.CurrentJournal, restartedJournal.CurrentSegmentIndex);
    }

    /// <summary>Truncated tail in the active segment still caps discovered sequence the same way as full-file replay.</summary>
    [Fact]
    public async Task TruncatedFrameInActiveJournalSegmentBoundsNextSequence()
    {
        using var dir = new TempDirectory("squirix-journal-next-seq-active-truncate");
        var persistence = NewPersistence(dir);
        using var manifestStore = new ManifestStore(persistence);
        var path = PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}000002{StorageFileExtensions.Journal}");
        var a = await BuildPutEnvelopeAsync(5UL, "a", "x");
        var b = await BuildPutEnvelopeAsync(6UL, "b", "y");
        await WriteSegmentWithFramesAsync(path, [a, b]);
        await using (var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
        {
            fs.SetLength(fs.Length - 1);
        }

        await manifestStore.WriteAsync(
            new Manifest
            {
                Format = 1,
                CurrentJournal = 2,
                NextSequence = 5,
                LastSnapshot = null,
            },
            DefaultCancellationToken);

        await using var journal = await JournalWriter.CreateAsync(persistence, await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken), manifestStore, new JournalStartupGate(), DefaultCancellationToken);
        Assert.Equal(6UL, journal.NextSequence);
    }

    private static async Task<JournalEnvelope> BuildPutEnvelopeAsync(ulong seq, string key, string value)
    {
        var body = await DiscriminatedEntryJsonWriter.BuildEntryJsonAsync(value, null, null, 1, null);
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
        JournalBackend = JournalBackend.JsonFramed,
        JournalMaxSegmentMb = 16,
        FlushIntervalMs = 5,
        ManifestRetentionCount = 1,
    };

    private static Task WriteJournalSegmentAsync(string dir, int index, IReadOnlyList<JournalEnvelope> envelopes)
    {
        var path = PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}{index.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Journal}");
        return WriteSegmentWithFramesAsync(path, envelopes);
    }

    private static async Task WriteSegmentWithFramesAsync(string path, IReadOnlyList<JournalEnvelope> envelopes)
    {
        await using var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read);
        JournalFraming.WriteFileHeader(stream);
        foreach (var envelope in envelopes)
        {
            var payload = RecordCodec.Serialize(envelope);
            JournalFraming.WriteFrame(stream, payload);
        }

        await stream.FlushAsync(DefaultCancellationToken);
    }
}
