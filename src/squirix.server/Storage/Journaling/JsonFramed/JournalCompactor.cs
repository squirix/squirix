using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.JsonFramed.Json;
using Squirix.Server.Storage.Journaling.Pipelined.Codec;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.JsonFramed;

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

        _ = await DirectoryEx.CreateDirectoryAsync(options.DataDir, cancellationToken: cancellationToken).ConfigureAwait(false);
        var oldManifest = await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        var snapshotRef = oldManifest.LastSnapshot;
        var replayFromSegment = snapshotRef?.ReplayFromJournalSegment > 0 ? snapshotRef.ReplayFromJournalSegment : 1;
        var (state, lastSeq) = await BuildCompactionStateAsync(options, snapshotRef, replayFromSegment, cancellationToken).ConfigureAwait(false);

        var newFirstIdx = GetNextJournalSegmentIndex(CollectJournalSegments(options.DataDir));
        var tmpPath = PathEx.Combine(options.DataDir, $"{StorageFilePrefixes.Journal}{newFirstIdx.ToString("000000", CultureInfo.InvariantCulture)}.tmp");
        _ = FileEx.TryDeleteFile(tmpPath);
        await WriteCompactedJournalAsync(options, tmpPath, state, lastSeq, cancellationToken).ConfigureAwait(false);
        await FinalizeCompactionAsync(options, manifestStore, oldManifest, newFirstIdx, lastSeq, cancellationToken).ConfigureAwait(false);
    }

    private static void Apply(JournalRecord record, Dictionary<CacheKey, CacheEntry<object?>> state)
    {
        switch (record.Operation)
        {
            case JournalOperationKind.Put:
                ApplyPut(record, state);
                break;
            case JournalOperationKind.Remove:
                ApplyRemove(record, state);
                break;
            case JournalOperationKind.RemoveExpiration:
                ApplyRemoveExpiration(record, state);
                break;
            case JournalOperationKind.TouchExpiration:
                ApplyTouchExpiration(record, state);
                break;
            case JournalOperationKind.AwaitDurabilityCommit:
            case JournalOperationKind.WaitForStartup:
            case JournalOperationKind.MaintenanceExclusive:
            case JournalOperationKind.SnapshotCut:
            case JournalOperationKind.UnderSnapshotBarrier:
            default:
                throw new ArgumentOutOfRangeException(nameof(record), record.Operation, "Unsupported journal op.");
        }
    }

    private static void ApplyPut(JournalRecord record, Dictionary<CacheKey, CacheEntry<object?>> state)
    {
        var key = new CacheKey(record.Key.Namespace, record.Key.Key);
        var payload = record.PutDiscriminatedEntryJson ?? [];
        if (!DiscriminatedEntryJsonReader.TryUtf8ToEntry<object?>(payload, out var entry))
            throw CreateCompactionDecodeFailure("put", key.Key);

        if (IsExpired(entry))
            _ = state.Remove(key);
        else
            state[key] = entry!;
    }

    private static void ApplyRemove(JournalRecord record, Dictionary<CacheKey, CacheEntry<object?>> state) => _ = state.Remove(new CacheKey(record.Key.Namespace, record.Key.Key));

    private static void ApplyRemoveExpiration(JournalRecord record, Dictionary<CacheKey, CacheEntry<object?>> state)
    {
        var key = new CacheKey(record.Key.Namespace, record.Key.Key);
        if (!state.TryGetValue(key, out var entry))
            return;

        state[key] = new CacheEntry<object?>
        {
            Value = entry.Value,
            Tags = entry.Tags,
            Version = entry.Version,
        };
    }

    private static void ApplyTouchExpiration(JournalRecord record, Dictionary<CacheKey, CacheEntry<object?>> state)
    {
        var key = new CacheKey(record.Key.Namespace, record.Key.Key);
        if (!state.TryGetValue(key, out var entry))
            return;

        state[key] = new CacheEntry<object?>
        {
            Value = entry.Value,
            ExpiresUtc = record.TouchExpirationUtc,
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
        foreach (var record in JournalReadPath.ReadAll(options.DataDir, fromSeg, cancellationToken))
        {
            lastSeq = Math.Max(lastSeq, record.Sequence);
            Apply(record, state);
        }

        return (state, lastSeq);
    }

    private static JournalSegment[] CollectJournalSegments(string dataDir)
    {
        var result = new List<JournalSegment>();
        foreach (var segment in JournalReadPath.EnumerateSegments(dataDir, 1))
            result.Add(segment);

        return [.. result];
    }

    private static InvalidOperationException CreateCompactionDecodeFailure(string operation, string key) =>
        new($"journal compaction failed: undecodable entry payload for operation '{operation}' on key '{key}'.");

    private static async Task FinalizeCompactionAsync(
        PersistenceOptions options,
        ManifestStore manifestStore,
        Manifest oldManifest,
        int newFirstIdx,
        ulong lastSeq,
        CancellationToken cancellationToken)
    {
        // Install the compacted journal before deleting any old segments.
        // Crash safety relies on each intermediate state remaining recoverable.
        var finalJournalPath = PathEx.Combine(
            options.DataDir,
            $"{StorageFilePrefixes.Journal}{newFirstIdx.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Journal}");
        var backupJournalPath = PathEx.Combine(options.DataDir, $"{StorageFilePrefixes.Journal}{newFirstIdx.ToString("000000", CultureInfo.InvariantCulture)}.bak");
        var tmpPath = PathEx.Combine(options.DataDir, $"{StorageFilePrefixes.Journal}{newFirstIdx.ToString("000000", CultureInfo.InvariantCulture)}.tmp");
        _ = FileEx.TryDeleteFile(backupJournalPath);
        FileEx.PublishFile(tmpPath, finalJournalPath, backupJournalPath);

        var newManifest = new Manifest
        {
            Format = oldManifest.Format is 0 ? 1 : oldManifest.Format,
            CurrentJournal = newFirstIdx,
            NextSequence = lastSeq is 0UL ? 1UL : lastSeq + 1UL,
            LastSnapshot = null,
        };
        await manifestStore.WriteAsync(newManifest, cancellationToken).ConfigureAwait(false);

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
        if (segments.Length is 0)
            return 1;

        var max = segments[0].Index;
        for (var i = 1; i < segments.Length; i++)
        {
            if (segments[i].Index > max)
                max = segments[i].Index;
        }

        return max + 1;
    }

    private static bool IsExpired(CacheEntry<object?>? e) => e is { ExpiresUtc: { } utc } && utc <= DateTime.UtcNow;

    private static async Task WriteCompactedJournalAsync(
        PersistenceOptions options,
        string tmpPath,
        Dictionary<CacheKey, CacheEntry<object?>> state,
        ulong lastSeq,
        CancellationToken cancellationToken)
    {
        var fs = new FileStream(tmpPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read, 64 * 1024, FileOptions.Asynchronous);
        await using (fs.ConfigureAwait(false))
        {
            await WriteCompactedJournalHeaderAsync(fs, options, cancellationToken).ConfigureAwait(false);

            var seq = lastSeq is 0UL ? 1UL : lastSeq + 1UL;
            var i = 0;
            foreach (var (k, e) in state)
            {
                if ((i++ & 0x3FF) is 0)
                    cancellationToken.ThrowIfCancellationRequested();

                if (IsExpired(e))
                    continue;

                var body = await DiscriminatedEntryJsonWriter.BuildEntryJsonAsync(e.Value, e.ExpiresUtc, e.Expiration, e.Version, e.Tags).ConfigureAwait(false);
                seq = await WriteCompactedPutEntryAsync(fs, options, k, body, seq, cancellationToken).ConfigureAwait(false);
            }

            await fs.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private static async Task WriteCompactedJournalHeaderAsync(FileStream fs, PersistenceOptions options, CancellationToken cancellationToken)
    {
        if (options.JournalBackend is JournalBackend.Pipelined)
        {
            var header = new byte[JournalBinaryFraming.FileHeaderSize];
            JournalBinaryFraming.WriteFileHeader(header);
            await fs.WriteAsync(header, cancellationToken).ConfigureAwait(false);
            return;
        }

        JournalFraming.WriteFileHeader(fs);
    }

    private static async Task<ulong> WriteCompactedPutEntryAsync(
        FileStream fs,
        PersistenceOptions options,
        CacheKey key,
        byte[] body,
        ulong sequence,
        CancellationToken cancellationToken)
    {
        var record = new JournalRecord
        {
            Sequence = sequence,
            UnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Operation = JournalOperationKind.Put,
            Key = key,
            PutDiscriminatedEntryJson = body,
            PutOperationId = string.Empty,
        };

        if (options.JournalBackend is JournalBackend.Pipelined)
        {
            var codec = JournalFrameCodecFactory.Binary;
            var bodyLen = BinaryJournalCodec.ComputeFrameBodyLength(record);
            var frameLen = JournalBinaryFraming.FrameTotalLength(bodyLen);
            var frame = new byte[frameLen];
            var bodySpan = frame.AsSpan(JournalBinaryFraming.FrameHeaderSize, bodyLen);
            var encodedLength = codec.Encode(record, bodySpan);
            if (encodedLength != bodyLen)
                throw new InvalidOperationException("unexpected journal frame length after encode.");

            JournalBinaryFraming.WriteFrame(frame, bodySpan);
            await fs.WriteAsync(frame, cancellationToken).ConfigureAwait(false);
            return sequence + 1UL;
        }

        var env = JsonFramedJournalCodec.ToEnvelope(record);
        var payload = RecordCodec.Serialize(env);
        JournalFraming.WriteFrame(fs, payload);
        return sequence + 1UL;
    }
}
