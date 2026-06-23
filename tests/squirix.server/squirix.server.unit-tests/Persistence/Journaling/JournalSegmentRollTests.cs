using System;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.Storage.Journaling.Framing;
using Squirix.Server.Storage.Journaling.Observability;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Verifies journal segment roll happens before the frame that would overflow the active segment.</summary>
public sealed class JournalSegmentRollTests : UnitTestBase
{
    /// <summary>When the next manifest file cannot be created, the roll fails before the overflow frame is appended.</summary>
    [Fact]
    public async Task BlockedNextManifestFilePreventsOverflowFrameFromBeingAppended()
    {
        using var dir = new TempDirectory("squirix-journal-roll-manifest-blocked");
        var options = CreateOptions(dir);
        using var manifestStore = new ManifestStore(options);
        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);
        var pipelined = Assert.IsType<JournalCoordinator>(journal);

        var overflowPayload = BuildLargePutPayload();
        var overflowKey = CacheKey.Default("overflow-key");
        var overflowFrameLen = FrameLength(overflowPayload, overflowKey);
        await FillSegmentOneForOverflowAsync(pipelined, overflowFrameLen, DefaultCancellationToken);

        var segmentOnePath = SegmentPath(dir, 1);
        var bytesBefore = new FileInfo(segmentOnePath).Length;

        await BlockNextManifestWriteAsync(manifestStore, dir);
        var manifestFileCountAfterBlock = CountManifestDataFiles(dir);
        await journal.AppendPutAsync(overflowKey, overflowPayload, null, DefaultCancellationToken);

        var deadline = Environment.TickCount64 + 5_000;
        while (!journal.HasFlushLoopFailure && Environment.TickCount64 < deadline)
            await Task.Delay(10, DefaultCancellationToken);

        Assert.True(journal.HasFlushLoopFailure);

        Assert.Equal(bytesBefore, new FileInfo(segmentOnePath).Length);
        Assert.Equal(manifestFileCountAfterBlock, CountManifestDataFiles(dir));
        Assert.False(ContainsPutKey(dir, 1, "overflow-key"));
        if (File.Exists(SegmentPath(dir, 2)))
            Assert.False(ContainsPutKey(dir, 2, "overflow-key"));
    }

    /// <summary>An overflow frame is written only after a successful roll, on the new journal segment file.</summary>
    [Fact]
    public async Task OverflowingAppendLandsOnNextSegmentAfterManifestRoll()
    {
        using var dir = new TempDirectory("squirix-journal-roll-overflow");
        var options = CreateOptions(dir);
        using var manifestStore = new ManifestStore(options);
        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);
        var pipelined = Assert.IsType<JournalCoordinator>(journal);

        var overflowPayload = BuildLargePutPayload();
        var overflowKey = CacheKey.Default("overflow-key");
        var overflowFrameLen = FrameLength(overflowPayload, overflowKey);
        await FillSegmentOneForOverflowAsync(pipelined, overflowFrameLen, DefaultCancellationToken);

        await journal.AppendPutAsync(overflowKey, overflowPayload, null, DefaultCancellationToken);
        await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);

        await ManifestStoreTestSupport.WaitUntilAsync(
            manifestStore,
            static s => s.ReadCurrentOrDefaultBlocking().CurrentJournal is 2,
            TimeSpan.FromSeconds(5),
            DefaultCancellationToken);

        Assert.Equal(2, (await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken)).CurrentJournal);
        Assert.False(ContainsPutKey(dir, 1, "overflow-key"));
        Assert.True(ContainsPutKey(dir, 2, "overflow-key"));
    }

    private static async Task BlockNextManifestWriteAsync(ManifestStore manifestStore, string dataDir)
    {
        manifestStore.PublishRollBlocking(1, 1);
        await File.WriteAllTextAsync(PathKit.Combine(dataDir, ManifestStoreTestSupport.ManifestDataFileName(2)), string.Empty, DefaultCancellationToken);
    }

    private static byte[] BuildLargePutPayload()
    {
        var payload = new byte[16_000];
        Array.Fill(payload, Convert.ToByte('y'));
        return payload;
    }

    private static bool ContainsPutKey(string dataDir, int segmentIndex, string key)
    {
        var path = SegmentPath(dataDir, segmentIndex);
        if (!File.Exists(path))
            return false;

        var reader = new BinaryJournalSegmentReader(path, true, CancellationToken.None);
        foreach (var record in reader)
        {
            if (record.Operation is JournalOperationKind.Put && string.Equals(record.Key.Key, key, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    private static int CountManifestDataFiles(string dataDir) =>
        Directory.Exists(dataDir) ? Directory.GetFiles(dataDir, $"{StorageFilePrefixes.Manifest}*{StorageFileExtensions.Manifest}").Length : 0;

    private static PersistenceOptions CreateOptions(string dataDir) => new()
    {
        DataDir = dataDir,
        JournalMaxSegmentMb = 1,
        FlushIntervalMs = 600_000,
        ManifestRetentionCount = 3,
    };

    private static async Task FillSegmentOneForOverflowAsync(JournalCoordinator journal, int overflowFrameLen, CancellationToken cancellationToken)
    {
        var fillPayload = new byte[8_192];
        Array.Fill(fillPayload, Convert.ToByte('x'));
        var fillKey = CacheKey.Default("fill");
        var fillFrameLen = FrameLength(fillPayload, fillKey);
        const long maxBytes = 1024L * 1024L;

        for (var i = 0; i < 16_384 && journal.CurrentSegmentIndex is 1; i++)
        {
            if (journal.ActiveSegmentWrittenBytes + overflowFrameLen > maxBytes)
                break;

            if (journal.ActiveSegmentWrittenBytes + fillFrameLen > maxBytes)
                break;

            await journal.AppendPutAsync(fillKey, fillPayload, null, cancellationToken);
            await journal.AwaitDurabilityCommitAsync(cancellationToken);
        }

        Assert.Equal(1, journal.CurrentSegmentIndex);
        Assert.True(journal.ActiveSegmentWrittenBytes + overflowFrameLen > maxBytes);
    }

    private static int FrameLength(byte[] payload, CacheKey key)
    {
        var record = new JournalRecord
        {
            Sequence = 1,
            UnixMs = 1,
            Operation = JournalOperationKind.Put,
            Key = key,
            PutEntryBytes = payload,
        };
        return JournalFraming.FrameTotalLength(BinaryJournalCodec.ComputeFrameBodyLength(record));
    }

    private static string SegmentPath(string dataDir, int segmentIndex) => PathKit.Combine(
        dataDir,
        $"{StorageFilePrefixes.Journal}{segmentIndex.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Journal}");
}
