using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading;
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
/// Verifies a failed in-flight journal frame write does not strand later durable frames behind a torn tail (SQU-35).
/// </summary>
public sealed class JournalWriterFailedAppendTailTests : ServerUnitTestBase
{
    /// <summary>
    /// After a canceled payload write, the torn partial frame is truncated and a later append is replayable.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task CanceledPayloadWriteTruncatesTailBeforeLaterReplayableFrames()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-journal-failed-append-tail");
        try
        {
            var options = CreateOptions(dir);
            var manifestStore = new ManifestStore(options);
            await using var journal = new JournalWriter(options, manifestStore.ReadCurrentOrDefault(), manifestStore, new JournalStartupGate());

            var anchorPayload = BuildEntryJson("anchor");
            await journal.AppendPutAsync(CacheKey.Default("anchor-key"), anchorPayload, null, DefaultCancellationToken);
            await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);

            var segmentPath = SegmentPath(dir, 1);
            var lengthBeforeFailed = new FileInfo(segmentPath).Length;

            var journalEnvelope = new JournalEnvelope
            {
                Seq = 2,
                UnixMs = 2,
                Put = new Put
                {
                    Item = new EntryPair
                    {
                        Key = "stranded-key",
                        EntryJson = ByteString.CopyFrom(BuildEntryJson("stranded")),
                    },
                },
            };
            var strandedPayload = RecordCodec.Serialize(journalEnvelope);
            using var canceled = new CancellationTokenSource();
            await canceled.CancelAsync();

            var appendFrame = typeof(JournalWriter).GetMethod("AppendFrameAsync", BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(appendFrame);

            _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => _ = await (Task<int>)appendFrame.Invoke(journal, [strandedPayload, canceled.Token])!);

            Assert.Equal(lengthBeforeFailed, new FileInfo(segmentPath).Length);
            Assert.Equal(lengthBeforeFailed, journal.ActiveSegmentWrittenBytes);

            var afterPayload = BuildEntryJson("after");
            await journal.AppendPutAsync(CacheKey.Default("after-key"), afterPayload, null, DefaultCancellationToken);
            await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);

            Assert.False(ContainsPutKey(ReadSegment(segmentPath), "stranded-key"));
            Assert.True(ContainsPutKey(ReadSegment(segmentPath), "anchor-key"));
            Assert.True(ContainsPutKey(ReadSegment(segmentPath), "after-key"));
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    private static byte[] BuildEntryJson(string value) => DiscriminatedEntryJsonWriter.BuildEntryJson(value, null, null, 1, null);

    private static bool ContainsPutKey(IEnumerable<JournalEnvelope> envelopes, string key)
    {
        foreach (var env in envelopes)
        {
            if (env.OpCase == JournalEnvelope.OpOneofCase.Put && env.Put.Item.Key == key)
                return true;
        }

        return false;
    }

    private static PersistenceOptions CreateOptions(string dataDir) => new()
    {
        DataDir = dataDir,
        JournalMaxSegmentMb = 16,
        FlushIntervalMs = 600_000,
        ManifestRetentionCount = 3,
        StrictFsync = true,
    };

    private static List<JournalEnvelope> ReadSegment(string segmentPath)
    {
        var envelopes = new List<JournalEnvelope>();
        using var reader = new MappedJournalSegmentReader(segmentPath, true, CancellationToken.None).GetEnumerator();
        while (reader.MoveNext())
            envelopes.Add(reader.Current);

        return envelopes;
    }

    private static string SegmentPath(string dataDir, int segmentIndex) => PathKit.Combine(dataDir, $"{StorageFilePrefixes.Journal}{segmentIndex:000000}{StorageFileExtensions.Journal}");
}
