using System;
using System.Collections.Generic;
using System.Globalization;
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
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Verifies a failed in-flight journal frame write does not strand later durable frames behind a torn tail (SQU-35).</summary>
public sealed class JournalWriterFailedAppendTailTests : UnitTestBase
{
    /// <summary>After a canceled payload write, the torn partial frame is truncated and a later append is replayable.</summary>
    [Fact]
    public async Task CanceledPayloadWriteTruncatesTailBeforeLaterReplayableFrames()
    {
        using var dir = new TempDirectory("squirix-journal-failed-append-tail");
        var options = CreateOptions(dir);
        using var manifestStore = new ManifestStore(options);
        await using var journal = await JournalWriter.CreateAsync(options, await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken), manifestStore, new JournalStartupGate(), DefaultCancellationToken);

        var anchorPayload = await BuildEntryJsonAsync("anchor");
        await journal.AppendPutAsync(CacheKey.Default("anchor-key"), anchorPayload, null, DefaultCancellationToken);
        await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);

        var segmentPath = SegmentPath(dir, 1);
        var lengthBeforeFailed = new FileInfo(segmentPath).Length;

        var strandedPayloadBytes = await BuildEntryJsonAsync("stranded");
        var journalEnvelope = new JournalEnvelope
        {
            Seq = 2,
            UnixMs = 2,
            Put = new Put
            {
                Item = new EntryPair
                {
                    Key = "stranded-key",
                    EntryJson = ByteString.CopyFrom(strandedPayloadBytes),
                },
            },
        };
        var strandedPayload = RecordCodec.Serialize(journalEnvelope);
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();

        var appendFrame = typeof(JournalWriter).GetMethod("AppendFrameAsync", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(appendFrame);

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            if (appendFrame.Invoke(journal, [strandedPayload, canceled.Token]) is not Task<int> appendTask)
                throw new InvalidOperationException("AppendFrameAsync did not return Task<int>.");

            _ = await appendTask;
        });

        Assert.Equal(lengthBeforeFailed, new FileInfo(segmentPath).Length);
        Assert.Equal(lengthBeforeFailed, journal.ActiveSegmentWrittenBytes);

        var afterPayload = await BuildEntryJsonAsync("after");
        await journal.AppendPutAsync(CacheKey.Default("after-key"), afterPayload, null, DefaultCancellationToken);
        await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);

        Assert.False(ContainsPutKey(ReadSegment(segmentPath), "stranded-key"));
        Assert.True(ContainsPutKey(ReadSegment(segmentPath), "anchor-key"));
        Assert.True(ContainsPutKey(ReadSegment(segmentPath), "after-key"));
    }

    private static Task<byte[]> BuildEntryJsonAsync(string value) =>
        DiscriminatedEntryJsonWriter.BuildEntryJsonAsync(value, null, null, 1, null);

    private static bool ContainsPutKey(IEnumerable<JournalEnvelope> envelopes, string key)
    {
        foreach (var env in envelopes)
        {
            if (env.OpCase is JournalEnvelope.OpOneofCase.Put && string.Equals(env.Put.Item.Key, key, StringComparison.OrdinalIgnoreCase))
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
    };

    private static List<JournalEnvelope> ReadSegment(string segmentPath)
    {
        var envelopes = new List<JournalEnvelope>();
        using var reader = new MappedJournalSegmentReader(segmentPath, true, CancellationToken.None).GetEnumerator();
        while (reader.MoveNext())
            envelopes.Add(reader.Current);

        return envelopes;
    }

    private static string SegmentPath(string dataDir, int segmentIndex) => PathKit.Combine(
        dataDir,
        $"{StorageFilePrefixes.Journal}{segmentIndex.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Journal}");
}
