using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.Storage.Journaling.Entries;
using Squirix.Server.Storage.Journaling.Framing;
using Squirix.Server.Storage.Journaling.Observability;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.Compaction;

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
    public static async Task CompactAsync(PersistenceOptions options, ManifestStore manifestStore, ISnapshotReader snapshotReader, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(snapshotReader);

        _ = await DirectoryEx.CreateDirectoryAsync(options.DataDir, cancellationToken: cancellationToken).ConfigureAwait(false);
        var oldManifest = await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        var snapshotRef = oldManifest.LastSnapshot;
        var replayFromSegment = snapshotRef?.ReplayFromJournalSegment > 0 ? snapshotRef.ReplayFromJournalSegment : 1;
        var (state, lastSeq) = await BuildCompactionStateAsync(options, snapshotRef, replayFromSegment, snapshotReader, cancellationToken).ConfigureAwait(false);

        var journalSegments = JournalReadPath.EnumerateSegments(options.DataDir, 1);
        var newFirstIdx = GetNextJournalSegmentIndex(journalSegments);
        var tmpPath = PathEx.Combine(options.DataDir, $"{StorageFilePrefixes.Journal}{newFirstIdx.ToString("000000", CultureInfo.InvariantCulture)}.tmp");
        _ = FileEx.TryDeleteFile(tmpPath);
        await WriteCompactedJournalAsync(tmpPath, state, lastSeq, cancellationToken).ConfigureAwait(false);
        await FinalizeCompactionAsync(options, manifestStore, oldManifest, newFirstIdx, lastSeq, journalSegments, cancellationToken).ConfigureAwait(false);
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
                throw new ArgumentOutOfRangeException(nameof(record), "Unsupported journal op.");
        }
    }

    private static void ApplyPut(JournalRecord record, Dictionary<CacheKey, CacheEntry<object?>> state)
    {
        var key = new CacheKey(record.Key.Namespace, record.Key.Key);
        if (!JournalEntryPayload.TryDecode<object?>(record.PutEntryBytes ?? [], out var entry))
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
        ManifestState.SnapshotRef? snapshotRef,
        int replayFromSegment,
        ISnapshotReader snapshotReader,
        CancellationToken cancellationToken)
    {
        var state = new Dictionary<CacheKey, CacheEntry<object?>>();
        if (!string.IsNullOrWhiteSpace(snapshotRef?.Path) && File.Exists(snapshotRef.Path))
        {
            var snapshot = await snapshotReader.LoadStrictAsync<object?>(snapshotRef.Path, cancellationToken: cancellationToken).ConfigureAwait(false);
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

    private static InvalidOperationException CreateCompactionDecodeFailure(string operation, string key) =>
        new($"journal compaction failed: undecodable entry payload for operation '{operation}' on key '{key}'.");

    private static async Task FinalizeCompactionAsync(
        PersistenceOptions options,
        ManifestStore manifestStore,
        ManifestState oldManifest,
        int newFirstIdx,
        ulong lastSeq,
        JournalSegment[] journalSegments,
        CancellationToken cancellationToken)
    {
        // Install the compacted journal before deleting any old segments.
        // Crash safety relies on each intermediate state remaining recoverable.
        var path = PathEx.Combine(options.DataDir, $"{StorageFilePrefixes.Journal}{newFirstIdx.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Journal}");
        var backupJournalPath = PathEx.Combine(options.DataDir, $"{StorageFilePrefixes.Journal}{newFirstIdx.ToString("000000", CultureInfo.InvariantCulture)}.bak");
        var tmpPath = PathEx.Combine(options.DataDir, $"{StorageFilePrefixes.Journal}{newFirstIdx.ToString("000000", CultureInfo.InvariantCulture)}.tmp");
        _ = FileEx.TryDeleteFile(backupJournalPath);
        FileEx.PublishFile(tmpPath, path, backupJournalPath);

        var newManifest = new ManifestState
        {
            Format = oldManifest.Format is 0 ? 1 : oldManifest.Format,
            CurrentJournal = newFirstIdx,
            NextSequence = lastSeq is 0UL ? 1UL : lastSeq + 1UL,
            LastSnapshot = null,
        };
        await manifestStore.WriteAsync(newManifest, cancellationToken).ConfigureAwait(false);

        foreach (var segment in journalSegments)
        {
            if (segment.Index == newFirstIdx)
                continue;

            _ = FileEx.TryDeleteFile(segment.Path);
        }

        _ = FileEx.TryDeleteFile(backupJournalPath);
    }

    private static int GetNextJournalSegmentIndex(JournalSegment[] segments) =>
        segments.Length is 0 ? 1 : segments[^1].Index + 1;

    private static bool IsExpired(CacheEntry<object?>? e) => e is { ExpiresUtc: { } utc } && utc <= DateTime.UtcNow;

    private static async Task WriteCompactedJournalAsync(string tmpPath, Dictionary<CacheKey, CacheEntry<object?>> state, ulong lastSeq, CancellationToken cancellationToken)
    {
        var fs = new FileStream(tmpPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read, 64 * 1024, FileOptions.Asynchronous);
        await using (fs.ConfigureAwait(false))
        {
            await WriteCompactedJournalHeaderAsync(fs, cancellationToken).ConfigureAwait(false);

            var seq = lastSeq is 0UL ? 1UL : lastSeq + 1UL;
            var i = 0;
            foreach (var (k, e) in state)
            {
                if ((i++ & 0x3FF) is 0)
                    cancellationToken.ThrowIfCancellationRequested();

                if (IsExpired(e))
                    continue;

                seq = await WriteCompactedPutEntryAsync(fs, k, JournalEntryPayload.Encode(e), seq, cancellationToken).ConfigureAwait(false);
            }

            await fs.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private static async Task WriteCompactedJournalHeaderAsync(FileStream fs, CancellationToken cancellationToken)
    {
        var header = new byte[JournalFraming.FileHeaderSize];
        JournalFraming.WriteFileHeader(header);
        await fs.WriteAsync(header, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<ulong> WriteCompactedPutEntryAsync(FileStream fs, CacheKey key, byte[] body, ulong sequence, CancellationToken cancellationToken)
    {
        var record = new JournalRecord
        {
            Sequence = sequence,
            UnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Operation = JournalOperationKind.Put,
            Key = key,
            PutEntryBytes = body,
            PutOperationId = string.Empty,
        };

        var bodyLen = BinaryJournalCodec.ComputeFrameBodyLength(record);
        var frameLen = JournalFraming.FrameTotalLength(bodyLen);
        var frame = new byte[frameLen];
        var bodySpan = frame.AsSpan(JournalFraming.FrameHeaderSize, bodyLen);
        var encodedLength = BinaryJournalCodec.Encode(record, bodySpan);
        if (encodedLength != bodyLen)
            throw new InvalidOperationException("unexpected journal frame length after encode.");

        JournalFraming.WriteFrame(frame, bodySpan);
        await fs.WriteAsync(frame, cancellationToken).ConfigureAwait(false);
        return sequence + 1UL;
    }
}
