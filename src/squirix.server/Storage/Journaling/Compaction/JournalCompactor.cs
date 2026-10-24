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
/// Compacts the current state (snapshot plus journal tail) into a single journal segment staged under a temp name.
/// The manifest write that names the new segment is the commit point; the segment is published under its final name after it,
/// and then the old journal files are deleted. Startup finishes the publish when a crash separates the two.
/// A throw after the manifest write began leaves the commit outcome unknown: the exclusive maintenance executor fails the journal pipeline, and startup
/// either publishes the staged segment (the manifest names it) or deletes it.
/// Invariants after completion:
/// - All used file handles are closed
/// - At least one valid journal segment exists
/// - Manifest reflects the new journal start index and the next sequence.
/// </summary>
internal static class JournalCompactor
{
    private static readonly CompactedSegmentDurability DefaultSegmentDurability = new();

    internal static Task CompactAsync(
        PersistenceOptions options,
        Ledger manifestStore,
        ISnapshotReader snapshotReader,
        DateTime utcNow,
        DateTime? expiredAsOf,
        CancellationToken cancellationToken) =>
        CompactAsync(options, manifestStore, snapshotReader, utcNow, expiredAsOf, DefaultSegmentDurability, cancellationToken);

    internal static async Task CompactAsync(
        PersistenceOptions options,
        Ledger manifestStore,
        ISnapshotReader snapshotReader,
        DateTime utcNow,
        DateTime? expiredAsOf,
        ICompactedSegmentDurability segmentDurability,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(snapshotReader);
        ArgumentNullException.ThrowIfNull(segmentDurability);

        _ = await DirectoryEx.CreateDirectoryAsync(options.DataDir, cancellationToken: cancellationToken).ConfigureAwait(false);
        var oldManifest = await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        var snapshotRef = oldManifest.LastSnapshot;
        var replayFromSegment = snapshotRef?.ReplayFromJournalSegment > 0 ? snapshotRef.ReplayFromJournalSegment : 1;
        var compaction =
            await BuildCompactionStateAsync(options, snapshotRef, replayFromSegment, snapshotReader, expiredAsOf, cancellationToken).ConfigureAwait(false);

        var journalSegments = JournalReadPath.EnumerateSegments(options.DataDir, 1);
        var newFirstIdx = journalSegments.Length == 0 ? 1 : journalSegments[^1].Index + 1;
        var tmpPath = PathEx.Combine(options.DataDir, $"{FilePrefixes.Journal}{InvariantDigitStrings.FormatD6(newFirstIdx)}.tmp");
        _ = FileEx.TryDeleteFile(tmpPath);
        var writtenLastSeq = await WriteCompactedJournalAsync(tmpPath, compaction, utcNow, expiredAsOf, segmentDurability, cancellationToken).ConfigureAwait(false);
        await FinalizeCompactionAsync(options, manifestStore, newFirstIdx, writtenLastSeq, journalSegments, cancellationToken).ConfigureAwait(false);
    }

    private static void Apply(JournalRecord record, Dictionary<CacheKey, NodeCacheEntry<object?>> state, Dictionary<string, CompactedIdempotencyRecord> idempotencyState)
    {
        if (TryApplyMutation(record, state, idempotencyState))
            return;

        ApplyIdempotencyRecord(record, idempotencyState);
    }

    private static void ApplyIdempotencyOutcome(JournalRecord record, Dictionary<string, CompactedIdempotencyRecord> idempotencyState)
    {
        var operationId = record.IdempotencyOperationId ?? ThrowHelper.Throw<string>(CreateCompactionDecodeFailure());
        var fingerprint = record.IdempotencyFingerprint ?? ThrowHelper.Throw<string>(CreateCompactionDecodeFailure());
        var responseBytes = record.IdempotencyResponseBytes;

        var copy = BufferEx.CopyToOwned(responseBytes.Span);
        idempotencyState[operationId] = new CompactedIdempotencyRecord(operationId, fingerprint, copy, false, record.UnixMs);
    }

    private static void ApplyIdempotencyRecord(JournalRecord record, Dictionary<string, CompactedIdempotencyRecord> idempotencyState)
    {
        switch (record.Operation)
        {
            case JournalOperationKind.IdempotencyOutcome:
                ApplyIdempotencyOutcome(record, idempotencyState);
                break;
            case JournalOperationKind.IdempotencyStarted:
                ApplyIdempotencyStarted(record, idempotencyState);
                break;
            case JournalOperationKind.Remove:
            case JournalOperationKind.Put:
            case JournalOperationKind.AwaitDurabilityCommit:
            case JournalOperationKind.WaitForStartup:
            case JournalOperationKind.MaintenanceExclusive:
            case JournalOperationKind.SnapshotCut:
            case JournalOperationKind.UnderSnapshotBarrier:
            default:
                throw new ArgumentOutOfRangeException(nameof(record), "Unsupported journal op.");
        }
    }

    private static void ApplyIdempotencyStarted(JournalRecord record, Dictionary<string, CompactedIdempotencyRecord> idempotencyState)
    {
        var operationId = record.IdempotencyOperationId ?? ThrowHelper.Throw<string>(CreateCompactionDecodeFailure());
        ApplyIdempotencyStartedMarker(operationId, record.IdempotencyFingerprint, record.UnixMs, idempotencyState);
    }

    private static void ApplyIdempotencyStartedMarker(
        string? mutationOperationId,
        string? fingerprint,
        long unixMs,
        Dictionary<string, CompactedIdempotencyRecord> idempotencyState)
    {
        if (mutationOperationId == null)
            return;

        if (string.IsNullOrEmpty(fingerprint))
            fingerprint = null;

        if (idempotencyState.TryGetValue(mutationOperationId, out var existing))
        {
            if (!existing.IsStarted)
                return;

            // A mutation marker may carry no fingerprint: preserve the fingerprint already recorded for
            // the started intent so a post-compaction retry still rejects operation-id reuse with a
            // mismatched fingerprint instead of surfacing a wildcard COMMIT_OUTCOME_UNKNOWN.
            fingerprint ??= existing.Fingerprint;
            if (string.Equals(fingerprint, existing.Fingerprint, StringComparison.Ordinal) && existing.UnixMs == unixMs)
                return;
        }

        idempotencyState[mutationOperationId] = new CompactedIdempotencyRecord(mutationOperationId, fingerprint, [], true, unixMs);
    }

    private static void ApplyPut(JournalRecord record, Dictionary<CacheKey, NodeCacheEntry<object?>> state)
    {
        var key = new CacheKey(record.Key.Namespace, record.Key.Key);
        if (!JournalEntryPayload.TryDecode<object?>(record.PutEntryBytes.Span, out var entry))
            throw CreateCompactionDecodeFailure();

        // A relative expiration is measured from the frame write time, as recovery does; whether the entry is expired is decided once,
        // on the server clock, when the compacted journal is written, so an expired put still supersedes the earlier value of the key.
        state[key] = JournalEntryExpirationMaterializer.ForRecoveryInsert(entry!, record.UnixMs);
    }

    private static void ApplyRemove(JournalRecord record, Dictionary<CacheKey, NodeCacheEntry<object?>> state) =>
        _ = state.Remove(new CacheKey(record.Key.Namespace, record.Key.Key));

    private static async Task<(Dictionary<CacheKey, NodeCacheEntry<object?>> State, Dictionary<string, CompactedIdempotencyRecord> IdempotencyState, ulong LastSeq)>
        BuildCompactionStateAsync(
            PersistenceOptions options,
            SnapshotRef? snapshotRef,
            int replayFromSegment,
            ISnapshotReader snapshotReader,
            DateTime? expiredAsOf,
            CancellationToken cancellationToken)
    {
        var state = new Dictionary<CacheKey, NodeCacheEntry<object?>>();
        var idempotencyState = new Dictionary<string, CompactedIdempotencyRecord>(StringComparer.Ordinal);
        var snapshotPath = snapshotRef == null ? null : SnapshotFileName.BuildPath(options.DataDir, snapshotRef.Index);
        if (snapshotPath != null && File.Exists(snapshotPath))
        {
            var snapshot = await snapshotReader.LoadStrictAsync<object?>(snapshotPath, expiredAsOf, cancellationToken).ConfigureAwait(false);
            for (var i = 0; i < snapshot.Entries.Count; i++)
            {
                var (key, entry) = snapshot.Entries[i];
                state[key] = entry;
            }

            for (var i = 0; i < snapshot.IdempotencyRecords.Count; i++)
            {
                var record = ThrowHelper.Required(snapshot.IdempotencyRecords[i], "Idempotency record must not be null.");
                var unixMs = new DateTimeOffset(DateTime.SpecifyKind(record.CreatedUtc, DateTimeKind.Utc)).ToUnixTimeMilliseconds();
                idempotencyState[record.OperationId] = new CompactedIdempotencyRecord(
                    record.OperationId,
                    record.Fingerprint,
                    record.ResponseBytes,
                    record.State == IdempotencyRecordState.Started,
                    unixMs);
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
        int newFirstIdx,
        ulong lastSeq,
        JournalSegment[] journalSegments,
        CancellationToken cancellationToken)
    {
        // The manifest write is the commit point: until it lands, recovery sees exactly the pre-compaction layout and no segment above the
        // manifest holds frames. After it, startup publishes the staged segment if the move below did not complete. A throw from the write
        // leaves the outcome unknown; safety comes from the executor latch plus that startup roll-forward or delete.
        var path = PathEx.Combine(options.DataDir, $"{FilePrefixes.Journal}{InvariantDigitStrings.FormatD6(newFirstIdx)}{FileExtensions.Journal}");
        var tmpPath = PathEx.Combine(options.DataDir, $"{FilePrefixes.Journal}{InvariantDigitStrings.FormatD6(newFirstIdx)}.tmp");
        FileEx.FlushDirectoryEntry(tmpPath);

        var newManifest = new State
        {
            CurrentJournal = newFirstIdx,
            NextSequence = lastSeq == 0UL ? 1UL : lastSeq + 1UL,
            LastSnapshot = null,
        };
        await manifestStore.WriteAsync(newManifest, cancellationToken).ConfigureAwait(false);
        _ = FileEx.PublishFile(tmpPath, path, TimeProvider.System);

        foreach (var segment in journalSegments)
        {
            if (segment.Index == newFirstIdx)
                continue;

            _ = FileEx.TryDeleteFile(segment.Path);
        }
    }

    private static bool IsExpired(NodeCacheEntry<object?>? e, DateTime? expiredAsOf) => expiredAsOf is { } asOf && e is { ExpiresUtc: { } utc } && utc <= asOf;

    private static bool TryApplyCacheMutation(JournalRecord record, Dictionary<CacheKey, NodeCacheEntry<object?>> state)
    {
        switch (record.Operation)
        {
            case JournalOperationKind.Put:
                ApplyPut(record, state);
                break;
            case JournalOperationKind.Remove:
                ApplyRemove(record, state);
                break;
            case JournalOperationKind.AwaitDurabilityCommit:
            case JournalOperationKind.WaitForStartup:
            case JournalOperationKind.MaintenanceExclusive:
            case JournalOperationKind.SnapshotCut:
            case JournalOperationKind.UnderSnapshotBarrier:
            case JournalOperationKind.IdempotencyOutcome:
            case JournalOperationKind.IdempotencyStarted:
            default:
                return false;
        }

        return true;
    }

    /// <summary>Applies a cache mutation to the compaction state and records its write-ahead idempotency intent.</summary>
    /// <param name="record">The journal record to apply.</param>
    /// <param name="state">The compacted cache entries.</param>
    /// <param name="idempotencyState">The compacted idempotency records.</param>
    /// <returns><see langword="true" /> when the record was a cache mutation; otherwise <see langword="false" />.</returns>
    private static bool TryApplyMutation(JournalRecord record, Dictionary<CacheKey, NodeCacheEntry<object?>> state, Dictionary<string, CompactedIdempotencyRecord> idempotencyState)
    {
        if (!TryApplyCacheMutation(record, state))
            return false;

        ApplyIdempotencyStartedMarker(record.MutationOperationId, record.MutationFingerprint, record.UnixMs, idempotencyState);
        return true;
    }

    private static async Task<(ulong Sequence, long Offset)> WriteCompactedIdempotencyAsync(
        SafeFileHandle handle,
        CompactedIdempotencyRecord record,
        bool isStarted,
        ulong sequence,
        long offset,
        CancellationToken cancellationToken)
    {
        var journalRecord = new JournalRecord
        {
            Sequence = sequence,
            UnixMs = record.UnixMs,
            Operation = isStarted ? JournalOperationKind.IdempotencyStarted : JournalOperationKind.IdempotencyOutcome,
            Key = new CacheKey(string.Empty, string.Empty),
            IdempotencyOperationId = record.OperationId,
            IdempotencyFingerprint = record.Fingerprint,
            IdempotencyResponseBytes = isStarted ? null : record.ResponseBytes,
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
        (Dictionary<CacheKey, NodeCacheEntry<object?>> State, Dictionary<string, CompactedIdempotencyRecord> IdempotencyState, ulong LastSeq) compaction,
        DateTime utcNow,
        DateTime? expiredAsOf,
        ICompactedSegmentDurability segmentDurability,
        CancellationToken cancellationToken)
    {
        var (state, idempotencyState, lastSeq) = compaction;
        var handle = FileEx.OpenPrivate(tmpPath, FileMode.CreateNew, FileAccess.Write, FileShare.Read, FileOptions.Asynchronous | FileOptions.SequentialScan);
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

                // A frame without a write time keeps its relative expiration; it is anchored at the compaction instant. A non-positive one is
                // expired, as recovery treats it. Without an expiry time, expiry is left to committed records and nothing is dropped.
                if (expiredAsOf != null && e.Expiration is { } relative && relative <= TimeSpan.Zero)
                    continue;

                var durable = JournalEntryExpirationMaterializer.ForDurableWrite(e, utcNow);
                if (IsExpired(durable, expiredAsOf))
                    continue;

                var encode = JournalEntryPayload.PrepareEncode(durable);
                using var payloadBuffer = JournalEntryPayload.Encode(in encode);
                (seq, offset) = await WriteCompactedPutEntryAsync(handle, k, payloadBuffer.Memory, seq, offset, utcNow, cancellationToken).ConfigureAwait(false);
                wroteAny = true;
            }

            foreach (var pair in idempotencyState)
            {
                (seq, offset) = await WriteCompactedIdempotencyAsync(handle, pair.Value, pair.Value.IsStarted, seq, offset, cancellationToken).ConfigureAwait(false);
                wroteAny = true;
            }

            segmentDurability.Flush(handle);
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
        DateTime utcNow,
        CancellationToken cancellationToken)
    {
        var record = new JournalRecord
        {
            Sequence = sequence,
            UnixMs = new DateTimeOffset(utcNow.Ticks, TimeSpan.Zero).ToUnixTimeMilliseconds(),
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
        internal CompactedIdempotencyRecord(string operationId, string? fingerprint, byte[] responseBytes, bool isStarted, long unixMs)
        {
            OperationId = operationId;
            Fingerprint = fingerprint;
            ResponseBytes = responseBytes;
            IsStarted = isStarted;
            UnixMs = unixMs;
        }

        internal string? Fingerprint { get; }

        internal bool IsStarted { get; }

        internal string OperationId { get; }

        internal byte[] ResponseBytes { get; }

        internal long UnixMs { get; }
    }
}
