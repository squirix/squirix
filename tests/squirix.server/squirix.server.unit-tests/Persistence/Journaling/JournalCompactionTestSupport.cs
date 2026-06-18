using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Shared helpers for journal compaction gap and completeness tests.</summary>
internal static class JournalCompactionTestSupport
{
    internal const string VolumeNamespace = "journal-volume";

    internal static Task<JournalRecord> BuildVolumePutAsync(ulong seq, int keyIndex, int payloadBytes = 64)
    {
        var payload = CreatePayload(keyIndex, payloadBytes);
        var body = JournalEntryPayloadKit.EncodePut(payload);
        return Task.FromResult(
            new JournalRecord
            {
                Sequence = seq,
                UnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Operation = JournalOperationKind.Put,
                Key = new CacheKey(VolumeNamespace, FormatKey(keyIndex)),
                PutEntryBytes = body,
            });
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "ZeroAlloc",
        "ZA0302:Large array allocation in method scope",
        Justification = "Test payload is the owned result returned to callers; pooling is not applicable.")]
    internal static byte[] CreatePayload(int keyIndex, int payloadBytes)
    {
        var payload = new byte[payloadBytes];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(payload, keyIndex);
        for (var i = 4; i < payload.Length; i++)
            payload[i] = Convert.ToByte((keyIndex + i) % 256);

        return payload;
    }

    internal static string FormatKey(int index) => $"vol:{index.ToString(CultureInfo.InvariantCulture)}";

    internal static Task WriteJournalSegmentAsync(string dir, int index, IReadOnlyList<JournalRecord> records) =>
        BinaryJournalTestSegmentWriter.WriteJournalSegmentAsync(dir, index, records);

    internal static Task<string> WriteSnapshotAsync(string dataDir, int snapshotIndex, IReadOnlyList<int> keyIndices, int payloadBytes = 64)
    {
        var writer = new SnapshotWriter(dataDir);
        var items = new List<(CacheKey Key, NodeCacheEntry<object?> Entry)>(keyIndices.Count);
        foreach (var index in keyIndices)
            items.Add((new CacheKey(VolumeNamespace, FormatKey(index)), new NodeCacheEntry<object?> { Value = CreatePayload(index, payloadBytes), Version = 1 }));

        return writer.WriteAsync(snapshotIndex, items, [], CancellationToken.None);
    }

    internal static async Task WriteKeyBatchAsync(
        PersistenceOptions persistence,
        ManifestStore manifestStore,
        int startIndex,
        int count,
        int payloadBytes,
        CancellationToken cancellationToken)
    {
        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new JournalStartupGate(),
            cancellationToken);

        for (var i = 0; i < count; i++)
        {
            var index = startIndex + i;
            var body = JournalEntryPayloadKit.EncodePut(CreatePayload(index, payloadBytes));
            await journal.AppendPutAsync(new CacheKey(VolumeNamespace, FormatKey(index)), body, cancellationToken);
        }

        await journal.AwaitDurabilityCommitAsync(cancellationToken);
    }

    internal static async Task TakeSnapshotAsync(
        PersistenceOptions persistence,
        ManifestStore manifestStore,
        SnapshotWriter snapWriter,
        int snapshotIndex,
        CancellationToken cancellationToken)
    {
        await using var cache = new PhysicalCache<object?>();
        var gate = new JournalStartupGate(false);
        var recovery = new RecoveryService<object?>(
            new RecoveryOptions { BlockOnStart = true },
            NullLogger<RecoveryService<object?>>.Instance,
            new RecoveryDependencies<object?>(
                persistence,
                manifestStore,
                cache,
                gate,
                new RpcMutationIdempotencyStore(),
                StoreFactory.CreateReader(persistence)));
        await recovery.StartAsync(cancellationToken);

        var items = new List<(CacheKey Key, NodeCacheEntry<object?> Entry)>();
        await foreach (var (key, entry) in cache.EnumerateLiveAsync(cancellationToken))
            items.Add((key, entry));

        var manifest = await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken);
        var seqAtFlush = manifest.NextSequence > 0 ? manifest.NextSequence - 1UL : 0UL;
        var replayFrom = manifest.CurrentJournal > 0 ? manifest.CurrentJournal : 1;
        var path = await snapWriter.WriteAsync(snapshotIndex, items, [], cancellationToken);

        await manifestStore.WriteAsync(
            new State
            {
                Format = manifest.Format is 0 ? 1 : manifest.Format,
                CurrentJournal = manifest.CurrentJournal,
                NextSequence = manifest.NextSequence,
                LastSnapshot = new SnapshotRef
                {
                    Index = snapshotIndex,
                    Path = path,
                    CreatedUtc = DateTime.UtcNow,
                    LastAppliedSequence = seqAtFlush,
                    ReplayFromJournalSegment = replayFrom,
                },
            },
            cancellationToken);
    }

    internal static PersistenceOptions NewPersistence(string dataDir, int journalMaxSegmentMb = 1, int groupCommitMs = 0) => new()
    {
        DataDir = dataDir,
        JournalMaxSegmentMb = journalMaxSegmentMb,
        FlushIntervalMs = 5,
        ManifestRetentionCount = 3,
        SnapshotRetentionCount = 3,
        JournalGroupCommitMaxWait = TimeSpan.FromMilliseconds(groupCommitMs),
    };
}
