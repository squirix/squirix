using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.PipelinedWal;
using Squirix.Server.Storage.JournalProto;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Verifies journal segment roll happens before the frame that would overflow the active segment.</summary>
public sealed class JournalWriterSegmentRollTests : UnitTestBase
{
    /// <summary>When the next manifest file cannot be created, the roll fails before the overflow frame is appended.</summary>
    [Fact]
    public async Task BlockedNextManifestFilePreventsOverflowFrameFromBeingAppended()
    {
        using var dir = new TempDirectory("squirix-journal-roll-manifest-blocked");
        var options = CreateOptions(dir);
        using var manifestStore = new ManifestStore(options);
        await using var journal = await JournalWriter.CreateAsync(options, await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken), manifestStore, new JournalStartupGate(), DefaultCancellationToken);

        var overflowPayload = await BuildLargePutPayloadAsync();
        var overflowFrameLen = FrameLength(overflowPayload);
        await FillSegmentOneForOverflowAsync(journal, overflowFrameLen, DefaultCancellationToken);

        var segmentOnePath = SegmentPath(dir, 1);
        var bytesBefore = new FileInfo(segmentOnePath).Length;

        await BlockNextManifestWriteAsync(dir);
        var manifestFileCountAfterBlock = CountManifestDataFiles(dir);
        _ = await Assert.ThrowsAnyAsync<IOException>(() => journal.AppendPutAsync(CacheKey.Default("overflow-key"), overflowPayload, null, DefaultCancellationToken).AsTask());

        Assert.Equal(bytesBefore, new FileInfo(segmentOnePath).Length);
        Assert.Equal(manifestFileCountAfterBlock, CountManifestDataFiles(dir));
        Assert.False(ContainsPutKey(ReadSingleSegment(dir, 1), "overflow-key"));
        if (File.Exists(SegmentPath(dir, 2)))
            Assert.False(ContainsPutKey(ReadSingleSegment(dir, 2), "overflow-key"));
    }

    /// <summary>An overflow frame is written only after a successful roll, on the new journal segment file.</summary>
    [Fact]
    public async Task OverflowingAppendLandsOnNextSegmentAfterManifestRoll()
    {
        using var dir = new TempDirectory("squirix-journal-roll-overflow");
        var options = CreateOptions(dir);
        using var manifestStore = new ManifestStore(options);
        await using var journal = await JournalWriter.CreateAsync(options, await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken), manifestStore, new JournalStartupGate(), DefaultCancellationToken);

        var overflowPayload = await BuildLargePutPayloadAsync();
        var overflowFrameLen = FrameLength(overflowPayload);
        await FillSegmentOneForOverflowAsync(journal, overflowFrameLen, DefaultCancellationToken);

        await journal.AppendPutAsync(CacheKey.Default("overflow-key"), overflowPayload, null, DefaultCancellationToken);
        await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);

        Assert.Equal(2, (await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken)).CurrentJournal);
        Assert.False(ContainsPutKey(ReadSingleSegment(dir, 1), "overflow-key"));
        Assert.True(ContainsPutKey(ReadSingleSegment(dir, 2), "overflow-key"));
    }

    /// <summary>
    /// Forces the next <see cref="ManifestStore.WriteAsync" /> onto a path that already exists (<see cref="FileMode.CreateNew" /> conflict).
    /// </summary>
    private static async Task BlockNextManifestWriteAsync(string dataDir)
    {
        var currentPath = PathKit.Combine(dataDir, $"{StorageFilePrefixes.Manifest}current");
        const string baselineName = $"{StorageFilePrefixes.Manifest}000001{StorageFileExtensions.Manifest}";
        await File.WriteAllTextAsync(currentPath, baselineName, DefaultCancellationToken);

        const string blockedName = $"{StorageFilePrefixes.Manifest}000002{StorageFileExtensions.Manifest}";
        await File.WriteAllTextAsync(PathKit.Combine(dataDir, blockedName), string.Empty, DefaultCancellationToken);
    }

    private static Task<byte[]> BuildLargePutPayloadAsync() =>
        DiscriminatedEntryJsonWriter.BuildEntryJsonAsync(new string('y', 16_000), null, null, 1, null);

    private static bool ContainsPutKey(IEnumerable<JournalEnvelope> envelopes, string key)
    {
        foreach (var env in envelopes)
        {
            if (env.OpCase is JournalEnvelope.OpOneofCase.Put && string.Equals(env.Put.Item.Key, key, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    private static int CountManifestDataFiles(string dataDir) =>
        Directory.Exists(dataDir) ? Directory.GetFiles(dataDir, $"{StorageFilePrefixes.Manifest}*{StorageFileExtensions.Manifest}").Length : 0;

    private static PersistenceOptions CreateOptions(string dataDir) => new()
    {
        DataDir = dataDir,
        JournalBackend = JournalBackend.JsonFramed,
        JournalMaxSegmentMb = 1,
        FlushIntervalMs = 600_000,
        ManifestRetentionCount = 3,
    };

    private static async Task FillSegmentOneForOverflowAsync(JournalWriter journal, int overflowFrameLen, CancellationToken cancellationToken)
    {
        var fillPayload = await DiscriminatedEntryJsonWriter.BuildEntryJsonAsync(new string('x', 128), null, null, 1, null);
        var fillFrameLen = FrameLength(fillPayload);
        const long maxBytes = 1024L * 1024L;

        for (var i = 0; i < 16_384 && journal.CurrentSegmentIndex is 1 && journal.ActiveSegmentWrittenBytes + fillFrameLen <= maxBytes; i++)
        {
            await journal.AppendPutAsync(CacheKey.Default("fill"), fillPayload, null, cancellationToken);
        }

        Assert.Equal(1, journal.CurrentSegmentIndex);
        Assert.True(journal.ActiveSegmentWrittenBytes + overflowFrameLen > maxBytes);
    }

    private static int FrameLength(byte[] payload) => JournalFraming.FrameHeaderSize + payload.Length + JournalFraming.FrameFooterSize;

    private static List<JournalEnvelope> ReadSingleSegment(string dataDir, int segmentIndex)
    {
        var path = SegmentPath(dataDir, segmentIndex);
        var envelopes = new List<JournalEnvelope>();
        using var reader = new MappedJournalSegmentReader(path, true, CancellationToken.None).GetEnumerator();
        while (reader.MoveNext())
            envelopes.Add(reader.Current);

        return envelopes;
    }

    private static string SegmentPath(string dataDir, int segmentIndex) => PathKit.Combine(
        dataDir,
        $"{StorageFilePrefixes.Journal}{segmentIndex.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Journal}");
}
