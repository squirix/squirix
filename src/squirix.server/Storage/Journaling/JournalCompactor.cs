using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Json;
using Squirix.Server.Storage.JournalProto;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// Compacts the current state (snapshot + journal tail) into a single journal segment,
/// then atomically replaces old journal files and updates the manifest.
/// Invariants after completion:
/// - All used file handles are closed
/// - At least one valid journal segment exists
/// - Manifest reflects the new journal start index and the next sequence.
/// </summary>
internal static class JournalCompactor
{
    public static async Task CompactAsync(PersistenceOptions options, ManifestStore manifestStore, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);

        _ = DirectoryEx.CreateDirectory(options.DataDir);
        var oldManifest = manifestStore.ReadCurrentOrDefault();
        var snapshotRef = oldManifest.LastSnapshot;
        var replayFromSegment = snapshotRef?.ReplayFromJournalSegment > 0 ? snapshotRef.ReplayFromJournalSegment : 1;
        var (state, lastSeq) = await BuildCompactionStateAsync(options, snapshotRef, replayFromSegment, cancellationToken).ConfigureAwait(false);

        var newFirstIdx = GetNextJournalSegmentIndex(CollectJournalSegments(options.DataDir));
        var tmpPath = PathEx.Combine(options.DataDir, $"{StorageFilePrefixes.Journal}{newFirstIdx:000000}.tmp");
        _ = FileEx.TryDeleteFile(tmpPath);
        await WriteCompactedJournalAsync(tmpPath, state, lastSeq, cancellationToken).ConfigureAwait(false);
        FinalizeCompaction(options, manifestStore, oldManifest, newFirstIdx, lastSeq);
    }

    private static void Apply(JournalEnvelope env, Dictionary<CacheKey, CacheEntry<object?>> state)
    {
        switch (env.OpCase)
        {
            case JournalEnvelope.OpOneofCase.Put:
                ApplyPut(env, state);
                break;
            case JournalEnvelope.OpOneofCase.Remove:
                ApplyRemove(env, state);
                break;
            case JournalEnvelope.OpOneofCase.RemoveExpiration:
                ApplyRemoveExpiration(env, state);
                break;
            case JournalEnvelope.OpOneofCase.TouchExpiration:
                ApplyTouchExpiration(env, state);
                break;
            case JournalEnvelope.OpOneofCase.None:
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(env));
        }
    }

    private static void ApplyPut(JournalEnvelope env, Dictionary<CacheKey, CacheEntry<object?>> state)
    {
        var put = env.Put ?? throw new InvalidOperationException("journal envelope op case is Put but payload is missing.");
        var key = new CacheKey(put.Item.Namespace, put.Item.Key);

        if (!DiscriminatedEntryJsonReader.TryUtf8ToEntry<object?>(put.Item.EntryJson.Memory, out var entry))
            throw CreateCompactionDecodeFailure("put", key.Key);

        if (IsExpired(entry))
            _ = state.Remove(key);
        else
            state[key] = entry;
    }

    private static void ApplyRemove(JournalEnvelope env, Dictionary<CacheKey, CacheEntry<object?>> state)
    {
        var remove = env.Remove ?? throw new InvalidOperationException("journal envelope op case is Remove but payload is missing.");
        _ = state.Remove(new CacheKey(remove.Namespace, remove.Key));
    }

    private static void ApplyRemoveExpiration(JournalEnvelope env, Dictionary<CacheKey, CacheEntry<object?>> state)
    {
        var removeExpiration = env.RemoveExpiration ?? throw new InvalidOperationException("journal envelope op case is RemoveExpiration but payload is missing.");
        var key = new CacheKey(removeExpiration.Namespace, removeExpiration.Key);
        if (!state.TryGetValue(key, out var entry))
            return;

        state[key] = new CacheEntry<object?>
        {
            Value = entry.Value,
            Tags = entry.Tags,
            Version = entry.Version,
        };
    }

    private static void ApplyTouchExpiration(JournalEnvelope env, Dictionary<CacheKey, CacheEntry<object?>> state)
    {
        var touchExpiration = env.TouchExpiration ?? throw new InvalidOperationException("journal envelope op case is TouchExpiration but payload is missing.");
        var key = new CacheKey(touchExpiration.Namespace, touchExpiration.Key);
        if (!state.TryGetValue(key, out var entry))
            return;

        state[key] = new CacheEntry<object?>
        {
            Value = entry.Value,
            ExpiresUtc = DateTimeOffset.FromUnixTimeMilliseconds(touchExpiration.ExpiresUnixMs).UtcDateTime,
            Tags = entry.Tags,
            Version = entry.Version,
        };
    }

    private static async Task<(Dictionary<CacheKey, CacheEntry<object?>> State, ulong LastSeq)> BuildCompactionStateAsync(
        PersistenceOptions options,
        Manifest.SnapshotRef? snapshotRef,
        int replayFromSegment,
        CancellationToken cancellationToken)
    {
        var state = new Dictionary<CacheKey, CacheEntry<object?>>();
        if (!string.IsNullOrWhiteSpace(snapshotRef?.Path) && File.Exists(snapshotRef.Path))
        {
            var snapshot = await SnapshotReader.LoadStrictAsync<object?>(snapshotRef.Path, cancellationToken: cancellationToken).ConfigureAwait(false);
            foreach (var (key, entry) in snapshot.Entries)
                state[key] = entry;
        }

        ulong lastSeq = 0;
        var fromSeg = Math.Max(1, replayFromSegment);
        foreach (var env in JournalReader.ReadAll(options.DataDir, fromSeg, cancellationToken))
        {
            lastSeq = Math.Max(lastSeq, env.Seq);
            Apply(env, state);
        }

        return (state, lastSeq);
    }

    private static JournalSegment[] CollectJournalSegments(string dataDir)
    {
        var result = new List<JournalSegment>();
        foreach (var segment in JournalReader.EnumerateSegments(dataDir, 1))
            result.Add(segment);

        return [.. result];
    }

    private static InvalidOperationException CreateCompactionDecodeFailure(string operation, string key) =>
        new($"journal compaction failed: undecodable entry payload for operation '{operation}' on key '{key}'.");

    private static void FinalizeCompaction(PersistenceOptions options, ManifestStore manifestStore, Manifest oldManifest, int newFirstIdx, ulong lastSeq)
    {
        // Install the compacted journal before deleting any old segments.
        // Crash safety relies on each intermediate state remaining recoverable.
        var finalJournalPath = PathEx.Combine(options.DataDir, $"{StorageFilePrefixes.Journal}{newFirstIdx:000000}{StorageFileExtensions.Journal}");
        var backupJournalPath = PathEx.Combine(options.DataDir, $"{StorageFilePrefixes.Journal}{newFirstIdx:000000}.bak");
        var tmpPath = PathEx.Combine(options.DataDir, $"{StorageFilePrefixes.Journal}{newFirstIdx:000000}.tmp");
        _ = FileEx.TryDeleteFile(backupJournalPath);
        FileEx.PublishFile(tmpPath, finalJournalPath, backupJournalPath);

        var newManifest = new Manifest
        {
            Format = oldManifest.Format == 0 ? 1 : oldManifest.Format,
            CurrentJournal = newFirstIdx,
            NextSequence = lastSeq == 0UL ? 1UL : lastSeq + 1UL,
            LastSnapshot = null,
        };
        manifestStore.Write(newManifest);

        foreach (var segment in CollectJournalSegments(options.DataDir))
        {
            if (segment.Index == newFirstIdx)
                continue;

            _ = FileEx.TryDeleteFile(segment.Path);
        }

        _ = FileEx.TryDeleteFile(backupJournalPath);
    }

    private static int GetNextJournalSegmentIndex(JournalSegment[] segments)
    {
        if (segments.Length == 0)
            return 1;

        var max = segments[0].Index;
        for (var i = 1; i < segments.Length; i++)
        {
            if (segments[i].Index > max)
                max = segments[i].Index;
        }

        return max + 1;
    }

    private static bool IsExpired(CacheEntry<object?> e) => e.ExpiresUtc is { } utc && utc <= DateTime.UtcNow;

    private static async Task WriteCompactedJournalAsync(string tmpPath, Dictionary<CacheKey, CacheEntry<object?>> state, ulong lastSeq, CancellationToken cancellationToken)
    {
        var fs = new FileStream(tmpPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read, 64 * 1024, FileOptions.Asynchronous);
        await using (fs.ConfigureAwait(false))
        {
            JournalFraming.WriteFileHeader(fs);
            var seq = lastSeq == 0UL ? 1UL : lastSeq + 1UL;

            var i = 0;
            foreach (var (k, e) in state)
            {
                if ((i++ & 0x3FF) == 0) // every 1024 items
                    cancellationToken.ThrowIfCancellationRequested();

                if (IsExpired(e))
                    continue;

                var body = DiscriminatedEntryJsonWriter.BuildEntryJson(e.Value, e.ExpiresUtc, e.Expiration, e.Version, e.Tags);

                var env = new JournalEnvelope
                {
                    Seq = seq++,
                    UnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    Put = new Put
                    {
                        Item = new EntryPair
                        {
                            Key = k.Key,
                            Namespace = k.Namespace,
                            EntryJson = ByteString.CopyFrom(body),
                        },
                    },
                };

                var payload = RecordCodec.Serialize(env);
                JournalFraming.WriteFrame(fs, payload);
            }

            await fs.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
