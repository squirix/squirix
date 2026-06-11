using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.JournalProto;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Storage.Journaling;

/// <summary>
/// Verifies journal segment roll happens before the frame that would overflow the active segment.
/// </summary>
public sealed class JournalWriterSegmentRollTests : ServerUnitTestBase
{
    /// <summary>
    /// When the next manifest file cannot be created, the roll fails before the overflow frame is appended.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task BlockedNextManifestFilePreventsOverflowFrameFromBeingAppended()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-journal-roll-manifest-blocked");
        try
        {
            var options = CreateOptions(dir);
            var manifestStore = new ManifestStore(options);
            await using var journal = new JournalWriter(options, manifestStore.ReadCurrentOrDefault(), manifestStore, new JournalStartupGate());

            var overflowPayload = BuildLargePutPayload();
            var overflowFrameLen = FrameLength(overflowPayload);
            await FillSegmentOneForOverflowAsync(journal, overflowFrameLen, DefaultCancellationToken);

            var segmentOnePath = SegmentPath(dir, 1);
            var bytesBefore = new FileInfo(segmentOnePath).Length;

            BlockNextManifestWrite(dir);
            var manifestFileCountAfterBlock = CountManifestDataFiles(dir);
            _ = await Assert.ThrowsAnyAsync<IOException>(() => journal.AppendPutAsync(CacheKey.Default("overflow-key"), overflowPayload, null, DefaultCancellationToken).AsTask());

            Assert.Equal(bytesBefore, new FileInfo(segmentOnePath).Length);
            Assert.Equal(manifestFileCountAfterBlock, CountManifestDataFiles(dir));
            Assert.False(ContainsPutKey(ReadSingleSegment(dir, 1), "overflow-key"));
            if (File.Exists(SegmentPath(dir, 2)))
                Assert.False(ContainsPutKey(ReadSingleSegment(dir, 2), "overflow-key"));
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// An overflow frame is written only after a successful roll, on the new journal segment file.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task OverflowingAppendLandsOnNextSegmentAfterManifestRoll()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-journal-roll-overflow");
        try
        {
            var options = CreateOptions(dir);
            var manifestStore = new ManifestStore(options);
            await using var journal = new JournalWriter(options, manifestStore.ReadCurrentOrDefault(), manifestStore, new JournalStartupGate());

            var overflowPayload = BuildLargePutPayload();
            var overflowFrameLen = FrameLength(overflowPayload);
            await FillSegmentOneForOverflowAsync(journal, overflowFrameLen, DefaultCancellationToken);

            await journal.AppendPutAsync(CacheKey.Default("overflow-key"), overflowPayload, null, DefaultCancellationToken);
            await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);

            Assert.Equal(2, manifestStore.ReadCurrentOrDefault().CurrentJournal);
            Assert.False(ContainsPutKey(ReadSingleSegment(dir, 1), "overflow-key"));
            Assert.True(ContainsPutKey(ReadSingleSegment(dir, 2), "overflow-key"));
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// Forces the next <see cref="ManifestStore.Write" /> onto a path that already exists (<see cref="FileMode.CreateNew" /> conflict).
    /// </summary>
    private static void BlockNextManifestWrite(string dataDir)
    {
        var currentPath = PathKit.Combine(dataDir, $"{StorageFilePrefixes.Manifest}current");
        const string baselineName = $"{StorageFilePrefixes.Manifest}000001{StorageFileExtensions.Manifest}";
        File.WriteAllText(currentPath, baselineName);

        const string blockedName = $"{StorageFilePrefixes.Manifest}000002{StorageFileExtensions.Manifest}";
        File.WriteAllText(PathKit.Combine(dataDir, blockedName), string.Empty);
    }

    private static byte[] BuildLargePutPayload() => DiscriminatedEntryJsonWriter.BuildEntryJson(new string('y', 16_000), null, null, 1, null);

    private static bool ContainsPutKey(IEnumerable<JournalEnvelope> envelopes, string key)
    {
        foreach (var env in envelopes)
        {
            if (env.OpCase == JournalEnvelope.OpOneofCase.Put && env.Put.Item.Key == key)
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

    private static async Task FillSegmentOneForOverflowAsync(JournalWriter journal, int overflowFrameLen, CancellationToken cancellationToken)
    {
        var fillPayload = DiscriminatedEntryJsonWriter.BuildEntryJson(new string('x', 128), null, null, 1, null);
        var fillFrameLen = FrameLength(fillPayload);
        const long maxBytes = 1024L * 1024L;

        for (var i = 0; i < 16_384 && journal.CurrentSegmentIndex == 1 && journal.ActiveSegmentWrittenBytes + fillFrameLen <= maxBytes; i++)
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

    private static string SegmentPath(string dataDir, int segmentIndex) => PathKit.Combine(dataDir, $"{StorageFilePrefixes.Journal}{segmentIndex:000000}{StorageFileExtensions.Journal}");
}
