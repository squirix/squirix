using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.Compaction;

/// <summary>
/// Compacts the current state (snapshot plus journal tail) into a single journal segment,
/// then atomically replaces old journal files, and updates the manifest.
/// Invariants after completion:
/// - All used file handles are closed
/// - At least one valid journal segment exists
/// - Manifest reflects the new journal start index and the next sequence.
/// </summary>
internal static class JournalCompactor
{
    internal static async Task CompactAsync(PersistenceOptions options, Ledger manifestStore, ISnapshotReader snapshotReader, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(snapshotReader);

        _ = await DirectoryEx.CreateDirectoryAsync(options.DataDir, cancellationToken: cancellationToken).ConfigureAwait(false);
        var oldManifest = await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        var snapshotRef = oldManifest.LastSnapshot;
        var replayFromSegment = snapshotRef?.ReplayFromJournalSegment > 0 ? snapshotRef.ReplayFromJournalSegment : 1;
        var (state, idempotencyState, lastSeq) = await BuildCompactionStateAsync(options, snapshotRef, replayFromSegment, snapshotReader, cancellationToken).ConfigureAwait(false);

        var journalSegments = JournalReadPath.EnumerateSegments(options.DataDir, 1);
        var newFirstIdx = GetNextJournalSegmentIndex(journalSegments);
        var tmpPath = PathEx.Combine(options.DataDir, $"{FilePrefixes.Journal}{InvariantDigitStrings.FormatD6(newFirstIdx)}.tmp");
        _ = FileEx.TryDeleteFile(tmpPath);
        var writtenLastSeq = await WriteCompactedJournalAsync(tmpPath, state, idempotencyState, lastSeq, cancellationToken).ConfigureAwait(false);
        await FinalizeCompactionAsync(options, manifestStore, oldManifest, newFirstIdx, writtenLastSeq, journalSegments, cancellationToken).ConfigureAwait(false);
    }

    private static void Apply(JournalRecord record, Dictionary<CacheKey, NodeCacheEntry<object?>> state, Dictionary<string, CompactedIdempotencyRecord> idempotencyState)
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
            case JournalOperationKind.IdempotencyOutcome:
                ApplyIdempotencyOutcome(record, idempotencyState);
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

    private static void ApplyIdempotencyOutcome(JournalRecord record, Dictionary<string, CompactedIdempotencyRecord> idempotencyState)
    {
        var operationId = record.IdempotencyOperationId ?? ThrowHelper.Throw<string>(CreateCompactionDecodeFailure());
        var fingerprint = record.IdempotencyFingerprint ?? ThrowHelper.Throw<string>(CreateCompactionDecodeFailure());
        var responseBytes = record.IdempotencyResponseBytes;

        var copy = BufferEx.CopyToOwned(responseBytes.Span);
        idempotencyState[operationId] = new CompactedIdempotencyRecord(operationId, fingerprint, copy, record.UnixMs);
    }

    private static void ApplyPut(JournalRecord record, Dictionary<CacheKey, NodeCacheEntry<object?>> state)
    {
        var key = new CacheKey(record.Key.Namespace, record.Key.Key);
        if (!JournalEntryPayload.TryDecode<object?>(record.PutEntryBytes.Span, out var entry))
            throw CreateCompactionDecodeFailure();

        if (IsExpired(entry))
            _ = state.Remove(key);
        else
            state[key] = entry!;
    }

    private static void ApplyRemove(JournalRecord record, Dictionary<CacheKey, NodeCacheEntry<object?>> state) =>
        _ = state.Remove(new CacheKey(record.Key.Namespace, record.Key.Key));

    private static void ApplyRemoveExpiration(JournalRecord record, Dictionary<CacheKey, NodeCacheEntry<object?>> state)
    {
        var key = new CacheKey(record.Key.Namespace, record.Key.Key);
        if (!state.TryGetValue(key, out var entry))
            return;

        state[key] = new NodeCacheEntry<object?>(entry.Value, entry.Version, tags: entry.Tags);
    }

    private static void ApplyTouchExpiration(JournalRecord record, Dictionary<CacheKey, NodeCacheEntry<object?>> state)
    {
        var key = new CacheKey(record.Key.Namespace, record.Key.Key);
        if (!state.TryGetValue(key, out var entry))
            return;

        state[key] = new NodeCacheEntry<object?>(entry.Value, entry.Version, record.TouchExpirationUtc, tags: entry.Tags);
    }

    private static async Task<(Dictionary<CacheKey, NodeCacheEntry<object?>> State, Dictionary<string, CompactedIdempotencyRecord> IdempotencyState, ulong LastSeq)>
        BuildCompactionStateAsync(PersistenceOptions options, SnapshotRef? snapshotRef, int replayFromSegment, ISnapshotReader snapshotReader, CancellationToken cancellationToken)
    {
        var state = new Dictionary<CacheKey, NodeCacheEntry<object?>>();
        var idempotencyState = new Dictionary<string, CompactedIdempotencyRecord>(StringComparer.Ordinal);
        if (!string.IsNullOrWhiteSpace(snapshotRef?.Path) && File.Exists(snapshotRef.Path))
        {
            var snapshot = await snapshotReader.LoadStrictAsync<object?>(snapshotRef.Path, cancellationToken: cancellationToken).ConfigureAwait(false);
            for (var i = 0; i < snapshot.Entries.Count; i++)
            {
                var (key, entry) = snapshot.Entries[i];
                state[key] = entry;
            }

            for (var i = 0; i < snapshot.IdempotencyRecords.Count; i++)
            {
                var record = ThrowHelper.Required(snapshot.IdempotencyRecords[i], "Idempotency record must not be null.");
                var unixMs = new DateTimeOffset(DateTime.SpecifyKind(record.CreatedUtc, DateTimeKind.Utc)).ToUnixTimeMilliseconds();
                idempotencyState[record.OperationId] = new CompactedIdempotencyRecord(record.OperationId, record.Fingerprint, record.ResponseBytes, unixMs);
            }
        }

        ulong lastSeq = 0;
        var fromSeg = Math.Max(1, replayFromSegment);
        using var records = JournalReadPath.ReadAll(options.DataDir, fromSeg, cancellationToken);
        while (records.MoveNext())
        {
            var record = records.Current;
            lastSeq = Math.Max(lastSeq, record.Sequence);
            Apply(record, state, idempotencyState);
        }

        return (state, idempotencyState, lastSeq);
    }

    private static InvalidOperationException CreateCompactionDecodeFailure() => new("journal compaction failed: undecodable entry payload.");

    private static async Task FinalizeCompactionAsync(
        PersistenceOptions options,
        Ledger manifestStore,
        State oldManifest,
        int newFirstIdx,
        ulong lastSeq,
        JournalSegment[] journalSegments,
        CancellationToken cancellationToken)
    {
        // Install the compacted journal before deleting any old segments.
        // Crash safety relies on each intermediate state remaining recoverable.
        var path = PathEx.Combine(options.DataDir, $"{FilePrefixes.Journal}{InvariantDigitStrings.FormatD6(newFirstIdx)}{FileExtensions.Journal}");
        var backupJournalPath = PathEx.Combine(options.DataDir, $"{FilePrefixes.Journal}{InvariantDigitStrings.FormatD6(newFirstIdx)}.bak");
        var tmpPath = PathEx.Combine(options.DataDir, $"{FilePrefixes.Journal}{InvariantDigitStrings.FormatD6(newFirstIdx)}.tmp");
        _ = FileEx.TryDeleteFile(backupJournalPath);
        _ = FileEx.PublishFile(tmpPath, path, backupJournalPath);

        var newManifest = new State
        {
            Format = oldManifest.Format == 0 ? 1 : oldManifest.Format,
            CurrentJournal = newFirstIdx,
            NextSequence = lastSeq == 0UL ? 1UL : lastSeq + 1UL,
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

    private static int GetNextJournalSegmentIndex(JournalSegment[] segments) => segments.Length == 0 ? 1 : segments[^1].Index + 1;

    private static bool IsExpired(NodeCacheEntry<object?>? e) => e is { ExpiresUtc: { } utc } && utc <= DateTime.UtcNow;

    private static async Task<(ulong Sequence, long Offset)> WriteCompactedIdempotencyOutcomeAsync(
        SafeFileHandle handle,
        CompactedIdempotencyRecord record,
        ulong sequence,
        long offset,
        CancellationToken cancellationToken)
    {
        var journalRecord = new JournalRecord
        {
            Sequence = sequence,
            UnixMs = record.UnixMs,
            Operation = JournalOperationKind.IdempotencyOutcome,
            Key = new CacheKey(string.Empty, string.Empty),
            IdempotencyOperationId = record.OperationId,
            IdempotencyFingerprint = record.Fingerprint,
            IdempotencyResponseBytes = record.ResponseBytes,
        };

        var encode = BinaryJournalCodec.PrepareEncode(journalRecord);
        var bodyLen = encode.BodyLength;
        var frameLen = JournalFraming.FrameTotalLength(bodyLen);
        var frame = ArrayPool<byte>.Shared.Rent(frameLen);
        try
        {
            const int bodyOffset = JournalFraming.FrameHeaderSize;
            var encodedLength = BinaryJournalCodec.Encode(journalRecord, frame.AsSpan(bodyOffset, bodyLen), in encode);
            if (encodedLength != bodyLen)
                throw new InvalidOperationException("unexpected journal frame length after encode.");

            JournalFraming.WriteFrame(frame.AsSpan(0, frameLen), frame.AsSpan(bodyOffset, bodyLen));
            await RandomAccess.WriteAsync(handle, frame.AsMemory(0, frameLen), offset, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.ReturnCleared(frame);
        }

        return (sequence + 1UL, offset + frameLen);
    }

    private static async Task<ulong> WriteCompactedJournalAsync(
        string tmpPath,
        Dictionary<CacheKey, NodeCacheEntry<object?>> state,
        Dictionary<string, CompactedIdempotencyRecord> idempotencyState,
        ulong lastSeq,
        CancellationToken cancellationToken)
    {
        var handle = File.OpenHandle(tmpPath, FileMode.CreateNew, FileAccess.Write, FileShare.Read, FileOptions.Asynchronous | FileOptions.SequentialScan);
        try
        {
            await WriteCompactedJournalHeaderAsync(handle, cancellationToken).ConfigureAwait(false);

            var seq = lastSeq == 0UL ? 1UL : lastSeq + 1UL;
            var wroteAny = false;
            var i = 0;
            long offset = JournalFraming.FileHeaderSize;
            foreach (var (k, e) in state)
            {
                if ((i++ & 0x3FF) == 0)
                    cancellationToken.ThrowIfCancellationRequested();

                if (IsExpired(e))
                    continue;

                var encode = JournalEntryPayload.PrepareEncode(e);
                using var payloadBuffer = JournalEntryPayload.Encode(in encode);
                (seq, offset) = await WriteCompactedPutEntryAsync(handle, k, payloadBuffer.Memory, seq, offset, cancellationToken).ConfigureAwait(false);
                wroteAny = true;
            }

            foreach (var pair in idempotencyState)
            {
                (seq, offset) = await WriteCompactedIdempotencyOutcomeAsync(handle, pair.Value, seq, offset, cancellationToken).ConfigureAwait(false);
                wroteAny = true;
            }

            return wroteAny ? seq - 1UL : lastSeq;
        }
        finally
        {
            handle.Dispose();
        }
    }

    private static Task WriteCompactedJournalHeaderAsync(SafeFileHandle handle, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
        JournalFraming.WriteFileHeader(header);
        RandomAccess.Write(handle, header, 0);
        return Task.CompletedTask;
    }

    private static async Task<(ulong Sequence, long Offset)> WriteCompactedPutEntryAsync(
        SafeFileHandle handle,
        CacheKey key,
        ReadOnlyMemory<byte> body,
        ulong sequence,
        long offset,
        CancellationToken cancellationToken)
    {
        var record = new JournalRecord
        {
            Sequence = sequence,
            UnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Operation = JournalOperationKind.Put,
            Key = key,
            PutEntryBytes = body,
        };

        var encode = BinaryJournalCodec.PrepareEncode(record);
        var bodyLen = encode.BodyLength;
        var frameLen = JournalFraming.FrameTotalLength(bodyLen);
        var frame = ArrayPool<byte>.Shared.Rent(frameLen);
        try
        {
            const int bodyOffset = JournalFraming.FrameHeaderSize;
            var encodedLength = BinaryJournalCodec.Encode(record, frame.AsSpan(bodyOffset, bodyLen), in encode);
            if (encodedLength != bodyLen)
                throw new InvalidOperationException("unexpected journal frame length after encode.");

            JournalFraming.WriteFrame(frame.AsSpan(0, frameLen), frame.AsSpan(bodyOffset, bodyLen));
            await RandomAccess.WriteAsync(handle, frame.AsMemory(0, frameLen), offset, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.ReturnCleared(frame);
        }

        return (sequence + 1UL, offset + frameLen);
    }

    [Immutable]
    private sealed class CompactedIdempotencyRecord
    {
        internal CompactedIdempotencyRecord(string operationId, string fingerprint, byte[] responseBytes, long unixMs)
        {
            OperationId = operationId;
            Fingerprint = fingerprint;
            ResponseBytes = responseBytes;
            UnixMs = unixMs;
        }

        internal string Fingerprint { get; }

        internal string OperationId { get; }

        internal byte[] ResponseBytes { get; }

        internal long UnixMs { get; }
    }
}
