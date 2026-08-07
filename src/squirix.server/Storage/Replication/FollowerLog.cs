using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Replication;

/// <summary>Durable, ordered follower log for a single replica group.</summary>
/// <remarks>
///     <para>
///     The log stores canonical entry bytes in an append-only file with per-frame CRC32C and publishes
///     per-group metadata (term, commit index, applied index, voted-for) through an atomic temp-file replacement.
///     Only the committed prefix that is not yet applied is exposed through the storage contract; the uncommitted
///     tail is retained on disk for pending-operation rebuild but is never applied to memory.
///     </para>
///     <para>
///     Advancing the applied index persists the watermark first and then releases the applied entry payloads
///     from memory; the frame offsets and terms of applied entries stay available so a divergent tail at or
///     above the committed index can still be truncated durably and applied-region term conflicts (a Leader
///     Completeness violation) can still be detected and fail readiness.
///     </para>
///     <para>
///     Append follows the following half of the consensus AppendEntries rule: previous
///     <c>(term, log_index)</c> consistency, consecutive append without gaps, idempotent duplicate
///     acknowledgement, higher-term persistence before response, and committed-prefix conflicts fail readiness.
///     An uncommitted entry that conflicts with the leader's batch truncates the divergent tail, which is then
///     rewritten with the leader's entries before the appending is acknowledged.
///     </para>
/// </remarks>
internal sealed class FollowerLog : IFollowerLog
{
    private static readonly IFollowerLogFaultHooks DefaultFaults = new NoOpFaultHooks();

    private readonly GroupComposition _composition;
    private readonly GroupLogDurability _durability = new();

    /// <summary>
    /// Applied entries are pruned from memory once their watermark is durable, so this working set is bounded
    /// by the applied watermark during a single process lifetime. Payloads are intentionally retained until the
    /// group is closed or the process restarts: groups are expected to be opened/closed on membership
    /// events in a later milestone, and enforced retention limits will be introduced together with that
    /// lifecycle. See the durable ordered follower log specification (M8-05) for the retention decision.
    /// </summary>
    private readonly SortedDictionary<ulong, FollowerLogEntry> _entries = [];

    private readonly SortedDictionary<ulong, (long Offset, ulong Term)> _entryOffsets = [];
    private readonly IFollowerLogFaultHooks _faults;

    [SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposing the semaphore could throw ObjectDisposedException in synchronous readers blocked on _gate.Wait(); idempotent disposal is handled via _disposed.")]
    private readonly SemaphoreSlim _gate = new(1, 1);

    private readonly string _groupDir;
    private readonly string _logPath;
    private readonly string _metaPath;
    private readonly string _metaTempPath;

    private int _disposed;
    private ulong _lastLogIndex;
    private long _logLength;
    private GroupLogMetadata _meta;

    internal FollowerLog(string persistenceRoot, string groupId, GroupComposition composition, IFollowerLogFaultHooks? faultHooks = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        _composition = composition ?? throw new ArgumentNullException(nameof(composition));
        _faults = faultHooks ?? DefaultFaults;
        GroupId = groupId;
        _groupDir = GroupStoragePaths.GetGroupDirectory(persistenceRoot, groupId);
        _metaPath = GroupStoragePaths.GetMetadataPath(persistenceRoot, groupId);
        _metaTempPath = GroupStoragePaths.GetMetadataTempPath(persistenceRoot, groupId);
        _logPath = GroupStoragePaths.GetLogPath(persistenceRoot, groupId);
    }

    /// <inheritdoc />
    public string GroupId { get; }

    /// <inheritdoc />
    public FollowerLogReadiness Readiness { get; private set; } = FollowerLogReadiness.Unknown;

    /// <summary>Gets a value indicating whether the log has been disposed.</summary>
    private bool IsDisposed => Volatile.Read(ref _disposed) is not 0;

    /// <inheritdoc />
    public async Task<FollowerLogAppliedResult> AdvanceAppliedAsync(ulong appliedIndex, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (IsDisposed || Readiness is not FollowerLogReadiness.Ready)
                return new FollowerLogAppliedResult(false, FollowerLogRefusal.NotReady, _meta.LastAppliedIndex);

            // Applied index moves only monotonically.
            if (appliedIndex <= _meta.LastAppliedIndex)
                return new FollowerLogAppliedResult(true, string.Empty, _meta.LastAppliedIndex);

            // Never applied beyond the committed index.
            if (appliedIndex > _meta.CommitIndex)
                return new FollowerLogAppliedResult(false, FollowerLogRefusal.NotReady, _meta.LastAppliedIndex);

            // The watermark is persisted before the payloads are released; on a crash between the two, restart
            // reloads the frames, but the durable watermark still suppresses re-application of the applied prefix.
            var candidate = _meta with { LastAppliedIndex = appliedIndex };
            await FollowerLogAppend.PersistMetaOrFailReadinessAsync(this, candidate, cancellationToken).ConfigureAwait(false);
            SetMeta(candidate);
            FollowerLogRecovery.PruneAppliedEntries(this);
            return new FollowerLogAppliedResult(true, string.Empty, appliedIndex);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <inheritdoc />
    public async Task<FollowerLogCommitResult> AdvanceCommitAsync(ulong commitIndex, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (IsDisposed || Readiness is not FollowerLogReadiness.Ready)
                return new FollowerLogCommitResult(false, FollowerLogRefusal.NotReady, _meta.CommitIndex);

            // Commit index moves only monotonically.
            if (commitIndex <= _meta.CommitIndex)
                return new FollowerLogCommitResult(true, string.Empty, _meta.CommitIndex);

            // Never beyond the locally durable last index.
            if (commitIndex > _lastLogIndex)
                return new FollowerLogCommitResult(false, FollowerLogRefusal.NotReady, _meta.CommitIndex);

            var candidate = _meta with { CommitIndex = commitIndex };
            await FollowerLogAppend.PersistMetaOrFailReadinessAsync(this, candidate, cancellationToken).ConfigureAwait(false);
            SetMeta(candidate);
            _faults.OnCommitAdvanced();
            return new FollowerLogCommitResult(true, string.Empty, commitIndex);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <inheritdoc />
    public async Task<FollowerLogAppendResult> AppendAsync(FollowerLogAppendRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request.LeaderNodeId);
        request = FollowerLogAppend.SnapshotRequestEntries(request);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (IsDisposed || Readiness is not FollowerLogReadiness.Ready)
                return new FollowerLogAppendResult(false, FollowerLogRefusal.NotReady, _meta.CurrentTerm, _lastLogIndex);

            var termError = await FollowerLogAppend.AdvanceTermIfHigherAsync(this, request, cancellationToken).ConfigureAwait(false);
            if (termError is not null)
                return termError.Value;

            var consistencyError = FollowerLogAppend.VerifyPreviousLogConsistency(this, request);
            return consistencyError ?? await FollowerLogAppend.AppendVerifiedBatchAsync(this, request, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) is not 0)
            return;

        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            _durability.Dispose();
            _ = FileEx.TryDeleteFile(_metaTempPath);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<FollowerLogEntry>> GetCommittedEntriesAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _faults.OnBeforeMemoryApply();
            var result = new List<FollowerLogEntry>();
            foreach (var pair in _entries)
            {
                if (pair.Key > _meta.CommitIndex)
                    break;

                // Applied entries were released from memory; their keys can still be present right after a
                // restart, so the working set is bounded below by the durable applied watermark.
                if (pair.Key <= _meta.LastAppliedIndex)
                    continue;

                result.Add(pair.Value);
            }

            return result;
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask<FollowerLogStatus> GetStatusAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return new FollowerLogStatus(
                GroupId,
                _meta.TopologyFingerprint,
                _meta.ConfigurationGeneration,
                _meta.CurrentTerm,
                _meta.VotedFor,
                _lastLogIndex,
                _meta.CommitIndex,
                _meta.LastAppliedIndex,
                Readiness);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<FollowerLogEntry>> GetUncommittedTailAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var result = new List<FollowerLogEntry>();
            foreach (var pair in _entries)
            {
                if (pair.Key <= _meta.CommitIndex)
                    continue;

                result.Add(pair.Value);
            }

            return result;
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <summary>Opens the group log, running startup validation and recovering only the committed prefix.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the group log is open and ready.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the group is not part of the local static composition. No storage directory is created.</exception>
    /// <exception cref="InvalidDataException">
    /// Thrown when metadata or a committed log frame is corrupt; readiness is set to <see cref="FollowerLogReadiness.Failed" />.
    /// </exception>
    internal async Task OpenAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!_composition.Contains(GroupId))
                throw new InvalidOperationException($"Group '{GroupId}' is not part of the local static composition.");

            _ = await DirectoryEx.CreateDirectoryAsync(_groupDir, cancellationToken: cancellationToken).ConfigureAwait(false);
            _ = FileEx.TryDeleteFile(_metaTempPath);

            var metaExists = File.Exists(_metaPath);
            var logExists = File.Exists(_logPath);

            if (!metaExists && !logExists)
            {
                var fresh = new GroupLogMetadata(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 0UL, string.Empty, 0UL, 0UL, 0UL);
                await FollowerLogAppend.PersistMetaOrFailReadinessAsync(this, fresh, cancellationToken).ConfigureAwait(false);
                SetMeta(fresh);
                SetLastLogIndex(0);
                SetLogLength(0);
                _durability.Open(_logPath, _logLength);
                Readiness = FollowerLogReadiness.Ready;
                return;
            }

            if (metaExists)
            {
                var metaBytes = await File.ReadAllBytesAsync(_metaPath, cancellationToken).ConfigureAwait(false);
                if (!GroupLogCodec.TryDecodeMeta(metaBytes, out var decoded) || !string.Equals(decoded.GroupId, GroupId, StringComparison.Ordinal))
                {
                    Readiness = FollowerLogReadiness.Failed;
                    throw new InvalidDataException($"Replica group '{GroupId}' metadata is corrupt.");
                }

                SetMeta(decoded);
            }
            else
            {
                // The log file exists without its atomically published metadata, so the committed boundary is unknown.
                // Assuming CommitIndex = 0 would treat every durable frame as an uncommitted tail and truncate it,
                // destroying possibly-committed data. Fail readiness instead; the group requires explicit repair.
                Readiness = FollowerLogReadiness.Failed;
                throw new InvalidDataException($"Replica group '{GroupId}' metadata is missing while the log file exists; the group requires recovery or repair.");
            }

            await FollowerLogRecovery.RecoverLogFileAsync(this, cancellationToken).ConfigureAwait(false);
            _durability.Open(_logPath, _logLength);
            Readiness = FollowerLogReadiness.Ready;
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    private void SetMeta(GroupLogMetadata meta) => _meta = meta;

    private void SetLastLogIndex(ulong logIndex) => _lastLogIndex = logIndex;

    private void SetLogLength(long logLength) => _logLength = logLength;

    /// <summary>Append-protocol operations for a follower log.</summary>
    private static class FollowerLogAppend
    {
        internal static FollowerLogAppendRequest SnapshotRequestEntries(FollowerLogAppendRequest request)
        {
            var entries = request.Entries;
            var owned = new FollowerLogEntry[entries.Length];
            for (var i = 0; i < entries.Length; i++)
            {
                var entry = entries.Span[i];
                owned[i] = new FollowerLogEntry(entry.LogIndex, entry.Term, entry.Payload.ToArray());
            }

            return new FollowerLogAppendRequest(
                request.LeaderNodeId,
                request.CurrentTerm,
                request.PrevLogIndex,
                request.PrevLogTerm,
                request.LeaderCommitIndex,
                owned);
        }

        internal static async Task<FollowerLogAppendResult?> AdvanceTermIfHigherAsync(FollowerLog owner, FollowerLogAppendRequest request, CancellationToken cancellationToken)
        {
            // Higher term is persisted durably before any further response; the old leader stops being authoritative.
            if (request.CurrentTerm > owner._meta.CurrentTerm)
            {
                var candidate = owner._meta with { CurrentTerm = request.CurrentTerm, VotedFor = string.Empty };
                await PersistMetaOrFailReadinessAsync(owner, candidate, cancellationToken).ConfigureAwait(false);
                owner.SetMeta(candidate);
                return null;
            }

            if (request.CurrentTerm < owner._meta.CurrentTerm)
                return new FollowerLogAppendResult(false, FollowerLogRefusal.StaleTerm, owner._meta.CurrentTerm, owner._lastLogIndex);

            return null;
        }

        internal static async Task<FollowerLogAppendResult> AppendVerifiedBatchAsync(FollowerLog owner, FollowerLogAppendRequest request, CancellationToken cancellationToken)
        {
            // Validate the whole batch for contiguity and conflicts before writing anything.
            var entries = request.Entries;
            var lastVerifiedIndex = entries.Length is 0 ? request.PrevLogIndex : entries.Span[entries.Length - 1].LogIndex;
            if (entries.Length is 0)
                return await CompleteAppendAsync(owner, request.LeaderCommitIndex, lastVerifiedIndex, false, cancellationToken).ConfigureAwait(false);

            var error = PrepareAppendBatch(owner, request, out var toAppend, out var truncateAtIndex);
            if (error is not null)
                return error.Value;

            if (truncateAtIndex is not null)
                await FollowerLogDurable.TruncateFromAsync(owner, truncateAtIndex.Value, cancellationToken).ConfigureAwait(false);

            if (toAppend is { Count: > 0 })
                await FollowerLogDurable.AppendFramesDurableAsync(owner, toAppend, cancellationToken).ConfigureAwait(false);

            return await CompleteAppendAsync(owner, request.LeaderCommitIndex, lastVerifiedIndex, toAppend is { Count: > 0 } || truncateAtIndex is not null, cancellationToken)
               .ConfigureAwait(false);
        }

        internal static async Task PersistMetaOrFailReadinessAsync(FollowerLog owner, GroupLogMetadata candidate, CancellationToken cancellationToken)
        {
            try
            {
                await FollowerLogDurable.PersistMetaAsync(owner, candidate, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Cancellation is not a publication failure; preserve readiness so the caller can retry.
                throw;
            }
            catch
            {
                owner.Readiness = FollowerLogReadiness.Failed;
                throw;
            }
        }

        internal static FollowerLogAppendResult? VerifyPreviousLogConsistency(FollowerLog owner, FollowerLogAppendRequest request)
        {
            // Previous-log consistency; the term at an applied index was released from memory, so the check
            // covers only the retained region above the applied watermark. The term of an applied entry is read
            // back from the retained frame metadata; a leader claiming a conflicting term there violates the
            // Leader Completeness property.
            if (request.PrevLogIndex > owner._lastLogIndex)
                return new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, owner._meta.CurrentTerm, owner._lastLogIndex);

            if (request.PrevLogIndex <= 0)
                return null;

            if (request.PrevLogIndex <= owner._meta.LastAppliedIndex)
                return TermAtApplied(owner, request.PrevLogIndex) == request.PrevLogTerm ? null : FailReadiness(owner);

            if (TermAt(owner, request.PrevLogIndex) == request.PrevLogTerm)
                return null;

            // A term conflict at or below the committed index violates the Leader Completeness property; fail readiness.
            if (request.PrevLogIndex <= owner._meta.CommitIndex)
                return FailReadiness(owner);

            return new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, owner._meta.CurrentTerm, owner._lastLogIndex);

            static ulong TermAt(FollowerLog owner, ulong logIndex)
            {
                return owner._entries.TryGetValue(logIndex, out var entry) ? entry.Term : 0UL;
            }

            static ulong TermAtApplied(FollowerLog owner, ulong logIndex)
            {
                return owner._entryOffsets.TryGetValue(logIndex, out var location) ? location.Term : 0UL;
            }
        }

        private static async Task<FollowerLogAppendResult> CompleteAppendAsync(FollowerLog owner, ulong leaderCommitIndex, ulong lastVerifiedIndex, bool metaDirty, CancellationToken cancellationToken)
        {
            var commitAdvanced = false;
            GroupLogMetadata? commitLogical = null;
            if (leaderCommitIndex > owner._meta.CommitIndex)
            {
                var target = Math.Min(leaderCommitIndex, lastVerifiedIndex);
                if (target > owner._meta.CommitIndex)
                {
                    commitLogical = owner._meta with { CommitIndex = target };
                    commitAdvanced = true;
                }
            }

            if (commitAdvanced || metaDirty)
            {
                await PersistMetaOrFailReadinessAsync(owner, commitLogical ?? owner._meta, cancellationToken).ConfigureAwait(false);
                if (commitLogical is { } candidate)
                    owner.SetMeta(candidate);
            }

            if (commitAdvanced)
                owner._faults.OnCommitAdvanced();

            return new FollowerLogAppendResult(true, string.Empty, owner._meta.CurrentTerm, owner._lastLogIndex);
        }

        private static FollowerLogAppendResult FailReadiness(FollowerLog owner)
        {
            owner.Readiness = FollowerLogReadiness.Failed;
            return new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, owner._meta.CurrentTerm, owner._lastLogIndex);
        }

        private static FollowerLogAppendResult? PrepareAppendBatch(FollowerLog owner, FollowerLogAppendRequest request, out List<FollowerLogEntry>? toAppend, out ulong? truncateAtIndex)
        {
            toAppend = null;
            truncateAtIndex = null;
            var nextExpected = request.PrevLogIndex + 1;
            var entries = request.Entries.Span;

            for (var i = 0; i < entries.Length; i++)
            {
                var entry = entries[i];
                if (entry.LogIndex != nextExpected)
                    return new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, owner._meta.CurrentTerm, owner._lastLogIndex);

                nextExpected++;

                // Once the divergent tail is being rewritten, every subsequent entry must be re-appended durably.
                if (truncateAtIndex is not null)
                {
                    toAppend!.Add(entry);
                    continue;
                }

                // Entries already satisfied by local state need no durable writing: duplicates already present with
                // identical content, and applied entries whose payloads were released after application (Leader
                // Completeness guarantees a current-term leader cannot create a conflict at an applied index).
                if (IsSatisfiedByLocalState(owner, in entry))
                    continue;

                if (entry.LogIndex <= owner._meta.CommitIndex)
                    return FailReadiness(owner);

                if (entry.LogIndex <= owner._lastLogIndex)
                    truncateAtIndex = entry.LogIndex;

                toAppend ??= [];
                toAppend.Add(entry);
            }

            return null;

            static bool IsSatisfiedByLocalState(FollowerLog owner, in FollowerLogEntry candidate)
            {
                if (candidate.LogIndex <= owner._lastLogIndex && owner._entries.TryGetValue(candidate.LogIndex, out var existing) && existing.Term == candidate.Term &&
                    existing.PayloadSpan.SequenceEqual(candidate.PayloadSpan))
                    return true;

                // The term of an applied entry was released with its payload, so it is read back from the retained
                // frame metadata; a batch re-appending at an applied index is a committed conflict and is rejected below.
                return candidate.LogIndex <= owner._meta.LastAppliedIndex && owner._entryOffsets.TryGetValue(candidate.LogIndex, out var location) && location.Term != candidate.Term;
            }
        }
    }

    /// <summary>Durable write coordination for log frames and metadata.</summary>
    private static class FollowerLogDurable
    {
        private static readonly Action<object?> AppendDurableCallback = static state =>
        {
            if (state is AppendDurableWork work)
                work.Execute();
        };

        private static readonly Action<object?> MetaDurableCallback = static state =>
        {
            if (state is MetaDurableWork work)
                work.Execute();
        };

        private static readonly Action<object?> TruncateDurableCallback = static state =>
        {
            if (state is TruncateDurableWork work)
                work.Execute();
        };

        internal static async Task AppendFramesDurableAsync(FollowerLog owner, List<FollowerLogEntry> toAppend, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // The file header precedes the first frame and is written exactly once
            // when the log file is still empty.
            var writeHeader = owner._logLength == 0;

            // The whole batch is encoded into a single contiguous buffer, so the OS
            // performs one sequential writing instead of many small ones.
            var totalLength = writeHeader ? GroupLogCodec.LogFileHeader.Length : 0;
            for (var i = 0; i < toAppend.Count; i++)
                totalLength += GroupLogCodec.ComputeFrameEncodedLength(toAppend[i].Payload.Length);

            var buffer = ArrayPool<byte>.Shared.Rent(totalLength);
            var position = writeHeader ? GroupLogCodec.LogFileHeader.Length : 0;
            var startOffset = writeHeader ? 0 : owner._logLength;

            if (writeHeader)
                GroupLogCodec.LogFileHeader.CopyTo(buffer);

            // Each frame is encoded at its final on-disk offset, and that offset is remembered,
            // so the in-memory index can serve reads without re-walking the file.
            var offsets = new List<KeyValuePair<ulong, long>>(toAppend.Count);
            for (var i = 0; i < toAppend.Count; i++)
            {
                var entry = toAppend[i];
                var encodedLength = GroupLogCodec.ComputeFrameEncodedLength(entry.Payload.Length);
                var frameOffset = startOffset + position;
                GroupLogCodec.EncodeFrame(buffer.AsSpan(position, encodedLength), entry);
                offsets.Add(new KeyValuePair<ulong, long>(entry.LogIndex, frameOffset));
                position += encodedLength;
            }

            // The durability worker flushes the exact-size buffer and propagates any I/O faults. The buffer is
            // owned by the work item and returned only inside its Execute; scheduling with a non-cancelable token
            // after the explicit check guarantees the callback always runs and the buffer is always returned.
            var work = new AppendDurableWork(owner._durability, buffer, startOffset, totalLength, owner._faults);
            await Task.Factory.StartNew(AppendDurableCallback, work, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).ConfigureAwait(false);

            // Only after the durable writing succeeds do the in-memory indexes gain the entries,
            // so a crash mid-appending can never leave the index ahead of the file.
            for (var i = 0; i < offsets.Count; i++)
            {
                var entry = toAppend[i];
                owner._entryOffsets[offsets[i].Key] = (offsets[i].Value, entry.Term);
                owner._entries[offsets[i].Key] = entry with { Payload = BufferEx.CopyToOwned(entry.PayloadSpan) };
            }

            owner.SetLastLogIndex(offsets[^1].Key);
            owner.SetLogLength(startOffset + totalLength);
            owner.SetMeta(owner._meta with { LastLogIndex = owner._lastLogIndex });
        }

        internal static Task PersistMetaAsync(FollowerLog owner, GroupLogMetadata meta, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var encodedLength = GroupLogCodec.ComputeMetaEncodedLength(meta);
            var buffer = ArrayPool<byte>.Shared.Rent(encodedLength);
            GroupLogCodec.EncodeMeta(meta, buffer.AsSpan(0, encodedLength));
            var work = new MetaDurableWork(owner._metaTempPath, owner._metaPath, buffer, encodedLength);

            // The buffer is returned only inside MetaDurableWork.Execute; a non-cancelable scheduling token
            // after the explicit check guarantees the worker always runs and the buffer is always returned.
            return Task.Factory.StartNew(MetaDurableCallback, work, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
        }

        internal static async Task TruncateFromAsync(FollowerLog owner, ulong logIndex, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!owner._entryOffsets.TryGetValue(logIndex, out var location))
                throw new InvalidOperationException($"Replica group '{owner.GroupId}' cannot truncate from a missing index '{logIndex}'.");

            var truncated = new List<ulong>();
            foreach (var index in owner._entries.Keys)
            {
                if (index >= logIndex)
                    truncated.Add(index);
            }

            try
            {
                var work = new TruncateDurableWork(owner._durability, location.Offset, owner._faults);
                await Task.Factory.StartNew(TruncateDurableCallback, work, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).ConfigureAwait(false);
            }
            finally
            {
                // Reconcile the in-memory indexes even when the durable truncate throws (SetLength may already
                // have been applied), so the ready log never validates against a suffix that may no longer be
                // durable. The exception still propagates so the caller can retry; at worst the in-memory state
                // trails the durable file (an uncommitted tail the leader will rewrite).
                for (var i = 0; i < truncated.Count; i++)
                {
                    _ = owner._entries.Remove(truncated[i]);
                    _ = owner._entryOffsets.Remove(truncated[i]);
                }

                owner.SetLastLogIndex(logIndex - 1);
                owner.SetLogLength(location.Offset);
                owner.SetMeta(owner._meta with { LastLogIndex = owner._lastLogIndex });
            }
        }

        /// <summary>Background work that durably writes appended frames and flushes them.</summary>
        private sealed class AppendDurableWork
        {
            private readonly byte[] _buffer;
            private readonly GroupLogDurability _durability;
            private readonly IFollowerLogFaultHooks _faults;
            private readonly int _length;
            private readonly long _startOffset;

            internal AppendDurableWork(GroupLogDurability durability, byte[] buffer, long startOffset, int length, IFollowerLogFaultHooks faults)
            {
                _durability = durability;
                _buffer = buffer;
                _startOffset = startOffset;
                _length = length;
                _faults = faults;
            }

            internal void Execute()
            {
                try
                {
                    _durability.Write(_buffer.AsMemory(0, _length), _startOffset);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(_buffer);
                }

                _faults.OnFrameWritten();
                _durability.Flush();
                _faults.OnFlushed();
            }
        }

        /// <summary>Background work that writes and atomically publishes metadata.</summary>
        private sealed class MetaDurableWork
        {
            private readonly byte[] _buffer;
            private readonly int _length;
            private readonly string _metaPath;
            private readonly string _metaTempPath;

            internal MetaDurableWork(string metaTempPath, string metaPath, byte[] buffer, int length)
            {
                _metaTempPath = metaTempPath;
                _metaPath = metaPath;
                _buffer = buffer;
                _length = length;
            }

            internal void Execute()
            {
                try
                {
                    const FileOptions options = FileOptions.WriteThrough;
                    using (var handle = File.OpenHandle(_metaTempPath, FileMode.Create, FileAccess.Write, FileShare.None, options))
                    {
                        RandomAccess.Write(handle, _buffer.AsSpan(0, _length), 0);
                        if (!OperatingSystem.IsWindows())
                            RandomAccess.FlushToDisk(handle);
                    }

                    FileEx.PublishFile(_metaTempPath, _metaPath);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(_buffer);
                }
            }
        }

        /// <summary>Background work that durably truncates the log before a conflicting tail is rewritten.</summary>
        private sealed class TruncateDurableWork
        {
            private readonly GroupLogDurability _durability;
            private readonly IFollowerLogFaultHooks _faults;
            private readonly long _length;

            internal TruncateDurableWork(GroupLogDurability durability, long length, IFollowerLogFaultHooks faults)
            {
                _durability = durability;
                _length = length;
                _faults = faults;
            }

            internal void Execute()
            {
                _durability.Truncate(_length);
                _durability.Flush();
                _faults.OnFlushed();
            }
        }
    }

    /// <summary>Startup recovery: rebuilds the in-memory log from the durable frame file.</summary>
    private static class FollowerLogRecovery
    {
        private const int FrameHeaderByteCount = 9;

        /// <summary>Page-aligned read buffer for the recovery file stream; frame parsing uses its own header size.</summary>
        private const int LogFileReadBufferSize = 64 * 1024;

        private static readonly Action<object?> RecoveryTruncateCallback = static state =>
        {
            if (state is RecoveryTruncateWork work)
                work.Execute();
        };

        internal static void PruneAppliedEntries(FollowerLog owner)
        {
            var applied = new List<ulong>();
            foreach (var index in owner._entries.Keys)
            {
                if (index <= owner._meta.LastAppliedIndex)
                    applied.Add(index);
            }

            for (var i = 0; i < applied.Count; i++)
                _ = owner._entries.Remove(applied[i]);
        }

        internal static async Task RecoverLogFileAsync(FollowerLog owner, CancellationToken cancellationToken)
        {
            // A missing log file simply means the group has never persisted anything.
            if (!File.Exists(owner._logPath))
            {
                ResetLogState(owner);
                EnsureCommittedPrefixCovered(owner);
                return;
            }

            var stream = new FileStream(
                owner._logPath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite | FileShare.Delete,
                LogFileReadBufferSize,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
            await using (stream.ConfigureAwait(false))
            {
                // An empty file is treated the same as no file at all.
                if (stream.Length == 0)
                {
                    ResetLogState(owner);
                    EnsureCommittedPrefixCovered(owner);
                    return;
                }

                // Anything preceding the expected header marks the file as unusable.
                if (stream.Length < GroupLogCodec.LogFileHeader.Length)
                {
                    owner.Readiness = FollowerLogReadiness.Failed;
                    throw new InvalidDataException($"Replica group '{owner.GroupId}' log header is corrupt.");
                }

                await ReadAndValidateLogFileHeaderAsync(stream, owner, cancellationToken).ConfigureAwait(false);

                owner._entries.Clear();
                owner._entryOffsets.Clear();
                owner.SetLastLogIndex(0);

                var result = await WalkFramesAsync(owner, stream, cancellationToken).ConfigureAwait(false);

                // A torn trailing frame is truncated back to the last valid boundary on disk,
                // since a CRC mismatch alone cannot tell an appended tail from corruption.
                if (result.Truncated && result.LastValidEnd < stream.Length)
                {
                    var work = new RecoveryTruncateWork(owner._logPath, result.LastValidEnd);
                    await Task.Factory.StartNew(RecoveryTruncateCallback, work, cancellationToken, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default)
                              .ConfigureAwait(false);
                }

                owner.SetLogLength(result.LastValidEnd);
                owner.SetMeta(owner._meta with { LastLogIndex = owner._lastLogIndex });
                PruneAppliedEntries(owner);
                EnsureCommittedPrefixCovered(owner);
            }
        }

        /// <summary>Fails recovery for a gap within the committed region.</summary>
        /// <param name="owner">The log being recovered.</param>
        /// <param name="lastValidEnd">The byte offset after the last valid frame.</param>
        /// <param name="nextLogIndex">The index of the missing frame.</param>
        /// <exception cref="InvalidDataException">The gap lies within the committed region.</exception>
        private static WalkResult CommittedGap(FollowerLog owner, long lastValidEnd, ulong nextLogIndex)
        {
            if (nextLogIndex > owner._meta.CommitIndex)
                return new WalkResult(lastValidEnd, true);
            owner.Readiness = FollowerLogReadiness.Failed;
            throw new InvalidDataException($"Replica group '{owner.GroupId}' committed log has a gap at index '{nextLogIndex}'.");
        }

        private static void EnsureCommittedPrefixCovered(FollowerLog owner)
        {
            if (owner._meta.CommitIndex <= owner._lastLogIndex)
                return;
            owner.Readiness = FollowerLogReadiness.Failed;
            throw new InvalidDataException($"Replica group '{owner.GroupId}' commit index exceeds the durable log.");
        }

        /// <summary>Reads and verifies the log file header, failing recovery when it is corrupt.</summary>
        /// <param name="stream">The open log file positioned at its start.</param>
        /// <param name="owner">The log being recovered.</param>
        /// <param name="cancellationToken">A token to observe while reading.</param>
        /// <exception cref="InvalidDataException">The log file header is corrupt.</exception>
        private static async Task ReadAndValidateLogFileHeaderAsync(FileStream stream, FollowerLog owner, CancellationToken cancellationToken)
        {
            var header = ArrayPool<byte>.Shared.Rent(GroupLogCodec.LogFileHeader.Length);
            try
            {
                await stream.ReadExactlyAsync(header.AsMemory(0, GroupLogCodec.LogFileHeader.Length), cancellationToken).ConfigureAwait(false);
                if (!header.AsSpan(0, GroupLogCodec.LogFileHeader.Length).SequenceEqual(GroupLogCodec.LogFileHeader))
                {
                    owner.Readiness = FollowerLogReadiness.Failed;
                    throw new InvalidDataException($"Replica group '{owner.GroupId}' log header is corrupt.");
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(header);
            }
        }

        /// <summary>Records a CRC-validated frame, keeping the payload only above the applied watermark.</summary>
        /// <param name="owner">The log being recovered.</param>
        /// <param name="nextLogIndex">The index the frame must carry.</param>
        /// <param name="lastValidEnd">The byte offset after the last valid frame.</param>
        /// <param name="frame">The complete frame at its start offset.</param>
        /// <returns>The recovery result when the frame must be truncated; otherwise <see langword="null" />.</returns>
        private static WalkResult? RecordFrame(FollowerLog owner, ulong nextLogIndex, long lastValidEnd, ReadOnlySpan<byte> frame)
        {
            // Frames at or below the applied watermark need only their index and term, not their payload.
            if (nextLogIndex <= owner._meta.LastAppliedIndex)
            {
                if (!GroupLogCodec.TryReadFrameFields(frame, out var logIndex, out var term))
                    return UncommittedTail(owner, lastValidEnd, nextLogIndex);

                if (logIndex != nextLogIndex)
                    return CommittedGap(owner, lastValidEnd, nextLogIndex);

                owner._entryOffsets[logIndex] = (lastValidEnd, term);
                owner.SetLastLogIndex(logIndex);
                return null;
            }

            if (!GroupLogCodec.TryReadFrame(frame, out var entry))
                return UncommittedTail(owner, lastValidEnd, nextLogIndex);

            if (entry.LogIndex != nextLogIndex)
                return CommittedGap(owner, lastValidEnd, nextLogIndex);

            owner._entryOffsets[entry.LogIndex] = (lastValidEnd, entry.Term);
            owner._entries[entry.LogIndex] = entry;
            owner.SetLastLogIndex(entry.LogIndex);
            return null;
        }

        private static void ResetLogState(FollowerLog owner)
        {
            owner.SetLogLength(0);
            owner.SetLastLogIndex(0);
            owner.SetMeta(owner._meta with { LastLogIndex = 0 });
        }

        /// <summary>Fails recovery for an invalid frame within the committed region.</summary>
        /// <param name="owner">The log being recovered.</param>
        /// <param name="lastValidEnd">The byte offset after the last valid frame.</param>
        /// <param name="nextLogIndex">The index of the invalid frame.</param>
        /// <exception cref="InvalidDataException">The invalid frame lies within the committed region.</exception>
        private static WalkResult UncommittedTail(FollowerLog owner, long lastValidEnd, ulong nextLogIndex)
        {
            if (nextLogIndex > owner._meta.CommitIndex)
                return new WalkResult(lastValidEnd, true);
            owner.Readiness = FollowerLogReadiness.Failed;
            throw new InvalidDataException($"Replica group '{owner.GroupId}' committed log frame at index '{nextLogIndex}' is corrupt.");
        }

        private static async Task<WalkResult> WalkFramesAsync(FollowerLog owner, FileStream stream, CancellationToken cancellationToken)
        {
            long lastValidEnd = GroupLogCodec.LogFileHeader.Length;

            while (stream.Position < stream.Length)
            {
                var nextLogIndex = owner._lastLogIndex + 1;

                // A partial header is a torn trailing frame.
                if (stream.Length - stream.Position < FrameHeaderByteCount)
                    return UncommittedTail(owner, lastValidEnd, nextLogIndex);

                var frameHeader = ArrayPool<byte>.Shared.Rent(FrameHeaderByteCount);
                try
                {
                    await stream.ReadExactlyAsync(frameHeader.AsMemory(0, FrameHeaderByteCount), cancellationToken).ConfigureAwait(false);

                    // A header that fails structural validation is either a torn tail or a corrupt committed frame.
                    if (!GroupLogCodec.TryReadFrameHeaderLength(frameHeader, out var frameLength))
                        return UncommittedTail(owner, lastValidEnd, nextLogIndex);

                    // A frame that ends past EOF is a torn trailing frame.
                    if (stream.Length - stream.Position < frameLength - FrameHeaderByteCount)
                        return UncommittedTail(owner, lastValidEnd, nextLogIndex);

                    var frame = ArrayPool<byte>.Shared.Rent(frameLength);
                    try
                    {
                        frameHeader.AsSpan(0, FrameHeaderByteCount).CopyTo(frame);
                        await stream.ReadExactlyAsync(frame.AsMemory(FrameHeaderByteCount, frameLength - FrameHeaderByteCount), cancellationToken).ConfigureAwait(false);

                        var tail = RecordFrame(owner, nextLogIndex, lastValidEnd, frame.AsSpan(0, frameLength));
                        if (tail is not null)
                            return tail.Value;

                        lastValidEnd += frameLength;
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(frame);
                    }
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(frameHeader);
                }
            }

            return new WalkResult(lastValidEnd, false);
        }

        /// <summary>Result of walking the log frames during startup recovery.</summary>
        /// <param name="LastValidEnd">The byte offset after the last valid frame.</param>
        /// <param name="Truncated">Determines whether a divergent tail was truncated.</param>
        private readonly record struct WalkResult(long LastValidEnd, bool Truncated);

        /// <summary>Background work that durably truncates the log during startup recovery.</summary>
        private sealed class RecoveryTruncateWork
        {
            private readonly long _length;
            private readonly string _path;

            internal RecoveryTruncateWork(string path, long length)
            {
                _path = path;
                _length = length;
            }

            internal void Execute()
            {
                const FileOptions options = FileOptions.WriteThrough;
                using var handle = File.OpenHandle(_path, FileMode.Open, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete, options);
                RandomAccess.SetLength(handle, _length);
                if (!OperatingSystem.IsWindows())
                    RandomAccess.FlushToDisk(handle);
            }
        }
    }

    /// <summary>No-op fault hooks used when none are supplied.</summary>
    private sealed class NoOpFaultHooks : IFollowerLogFaultHooks
    {
        public void OnBeforeMemoryApply()
        {
        }

        public void OnCommitAdvanced()
        {
        }

        public void OnFlushed()
        {
        }

        public void OnFrameWritten()
        {
        }
    }
}
