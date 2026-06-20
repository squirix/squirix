using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using Google.Protobuf;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.JsonFramed;
using Squirix.Server.Storage.Journaling.JsonFramed.Json;
using Squirix.Server.Storage.JournalProto;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Journal-only recovery must replay from the first on-disk segment, not manifest CurrentJournal.</summary>
public sealed class RecoveryServiceJournalOnlyReplayTests : UnitTestBase
{
    /// <summary>After a segment roll, keys in the closed segment are still required for cache rebuild when no snapshot exists.</summary>
    [Fact]
    public async Task JournalOnlyRecoveryReplaysClosedSegmentBelowManifestCurrentJournal()
    {
        await using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-journal-only-roll");
        var seg1A = await BuildPutEnvelopeAsync(1UL, "seg1-a", "a");
        var seg1B = await BuildPutEnvelopeAsync(2UL, "seg1-b", "b");
        var seg2C = await BuildPutEnvelopeAsync(3UL, "seg2-c", "c");
        await WriteJournalSegmentAsync(scenario.DataDir, 1, [seg1A, seg1B]);
        await WriteJournalSegmentAsync(scenario.DataDir, 2, [seg2C]);
        await scenario.ManifestStore.WriteAsync(
            new Manifest
            {
                Format = 1,
                CurrentJournal = 2,
                NextSequence = 4,
                LastSnapshot = null,
            },
            DefaultCancellationToken);

        var gate = new JournalStartupGate(false);
        var recovery = new RecoveryService<object?>(
            new PersistenceOptions { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16, FlushIntervalMs = 5 },
            scenario.ManifestStore,
            scenario.Cache,
            new RecoveryOptions { BlockOnStart = true },
            gate,
            NullLogger<RecoveryService<object?>>.Instance);
        await recovery.StartAsync(DefaultCancellationToken);

        Assert.True((await scenario.Cache.GetValueAsync(CacheKey.Default("seg1-a"), DefaultCancellationToken)).Found);
        Assert.True((await scenario.Cache.GetValueAsync(CacheKey.Default("seg1-b"), DefaultCancellationToken)).Found);
        Assert.True((await scenario.Cache.GetValueAsync(CacheKey.Default("seg2-c"), DefaultCancellationToken)).Found);
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
