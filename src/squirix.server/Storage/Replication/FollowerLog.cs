using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
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
///     from memory; the frame offsets of applied entries stay available so a divergent tail at or above the
///     committed index can still be truncated durably.
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
    private static readonly IFollowerLogFaultHooks NoOpFaults = new NoOpFaultHooks();

    private readonly GroupComposition _composition;
    private readonly GroupLogDurability _durability = new();
    private readonly SortedDictionary<ulong, FollowerLogEntry> _entries = [];
    private readonly SortedDictionary<ulong, long> _entryOffsets = [];
    private readonly IFollowerLogFaultHooks _faults;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly string _groupDir;
    private readonly string _logPath;
    private readonly string _metaPath;
    private readonly string _metaTempPath;

    private bool _disposed;
    private ulong _lastLogIndex;
    private long _logLength;
    private GroupLogMetadata _meta;

    internal FollowerLog(string persistenceRoot, string groupId, GroupComposition composition, IFollowerLogFaultHooks? faultHooks = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        _composition = composition ?? throw new ArgumentNullException(nameof(composition));
        _faults = faultHooks ?? NoOpFaults;
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

    /// <inheritdoc />
    public async Task<FollowerLogAppliedResult> AdvanceAppliedAsync(ulong appliedIndex, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_disposed || Readiness is not FollowerLogReadiness.Ready)
                return new FollowerLogAppliedResult(false, FollowerLogRefusal.NotReady, _meta.LastAppliedIndex);

            // Applied index moves only monotonically.
            if (appliedIndex <= _meta.LastAppliedIndex)
                return new FollowerLogAppliedResult(true, string.Empty, _meta.LastAppliedIndex);

            // Never applied beyond the committed index.
            if (appliedIndex > _meta.CommitIndex)
                return new FollowerLogAppliedResult(false, FollowerLogRefusal.NotReady, _meta.LastAppliedIndex);

            // The watermark is persisted before the payloads are released; on a crash between the two, restart
            // reloads the frames, but the durable watermark still suppresses re-application of the applied prefix.
            SetMeta(_meta with { LastAppliedIndex = appliedIndex });
            await FollowerLogDurable.PersistMetaAsync(this, cancellationToken).ConfigureAwait(false);
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
            if (_disposed || Readiness is not FollowerLogReadiness.Ready)
                return new FollowerLogCommitResult(false, FollowerLogRefusal.NotReady, _meta.CommitIndex);

            // Commit index moves only monotonically.
            if (commitIndex <= _meta.CommitIndex)
                return new FollowerLogCommitResult(true, string.Empty, _meta.CommitIndex);

            // Never beyond the locally durable last index.
            if (commitIndex > _lastLogIndex)
                return new FollowerLogCommitResult(false, FollowerLogRefusal.NotReady, _meta.CommitIndex);

            SetMeta(_meta with { CommitIndex = commitIndex });
            await FollowerLogDurable.PersistMetaAsync(this, cancellationToken).ConfigureAwait(false);
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
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_disposed || Readiness is not FollowerLogReadiness.Ready)
                return new FollowerLogAppendResult(false, FollowerLogRefusal.NotReady, _meta.CurrentTerm, _lastLogIndex);

            var termError = await AdvanceTermIfHigherAsync(request, cancellationToken).ConfigureAwait(false);
            if (termError is not null)
                return termError.Value;

            var consistencyError = VerifyPreviousLogConsistency(request);
            return consistencyError ?? await AppendVerifiedBatchAsync(request, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, true))
            return;

        try
        {
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
        finally
        {
            _gate.Dispose();
        }
    }

    /// <inheritdoc />
    public IReadOnlyList<FollowerLogEntry> GetCommittedEntries()
    {
        _gate.Wait();
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
    public FollowerLogStatus GetStatus()
    {
        _gate.Wait();
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
    public IReadOnlyList<FollowerLogEntry> GetUncommittedTail()
    {
        _gate.Wait();
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
                SetMeta(new GroupLogMetadata(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 0UL, string.Empty, 0UL, 0UL, 0UL));
                SetLastLogIndex(0);
                SetLogLength(0);
                await FollowerLogDurable.PersistMetaAsync(this, cancellationToken).ConfigureAwait(false);
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

    private async Task<FollowerLogAppendResult?> AdvanceTermIfHigherAsync(FollowerLogAppendRequest request, CancellationToken cancellationToken)
    {
        // Higher term is persisted durably before any further response; the old leader stops being authoritative.
        if (request.CurrentTerm > _meta.CurrentTerm)
        {
            SetMeta(_meta with { CurrentTerm = request.CurrentTerm, VotedFor = string.Empty });
            await FollowerLogDurable.PersistMetaAsync(this, cancellationToken).ConfigureAwait(false);
            return null;
        }

        if (request.CurrentTerm < _meta.CurrentTerm)
            return new FollowerLogAppendResult(false, FollowerLogRefusal.StaleTerm, _meta.CurrentTerm, _lastLogIndex);

        return null;
    }

    private async Task<FollowerLogAppendResult> AppendVerifiedBatchAsync(FollowerLogAppendRequest request, CancellationToken cancellationToken)
    {
        // Validate the whole batch for contiguity and conflicts before writing anything.
        var entries = request.Entries;
        var lastVerifiedIndex = entries.Length is 0 ? request.PrevLogIndex : entries.Span[entries.Length - 1].LogIndex;
        if (entries.Length is 0)
            return await CompleteAppendAsync(request.LeaderCommitIndex, lastVerifiedIndex, false, cancellationToken).ConfigureAwait(false);

        var error = PrepareAppendBatch(request, out var toAppend, out var truncateAtIndex);
        if (error is not null)
            return error.Value;

        if (truncateAtIndex is not null)
            await FollowerLogDurable.TruncateFromAsync(this, truncateAtIndex.Value, cancellationToken).ConfigureAwait(false);

        if (toAppend is { Count: > 0 })
            await FollowerLogDurable.AppendFramesDurableAsync(this, toAppend, cancellationToken).ConfigureAwait(false);

        return await CompleteAppendAsync(request.LeaderCommitIndex, lastVerifiedIndex, toAppend is { Count: > 0 } || truncateAtIndex is not null, cancellationToken)
           .ConfigureAwait(false);
    }

    private async Task<FollowerLogAppendResult> CompleteAppendAsync(ulong leaderCommitIndex, ulong lastVerifiedIndex, bool metaDirty, CancellationToken cancellationToken)
    {
        var commitAdvanced = false;
        if (leaderCommitIndex > _meta.CommitIndex)
        {
            var target = Math.Min(leaderCommitIndex, lastVerifiedIndex);
            if (target > _meta.CommitIndex)
            {
                SetMeta(_meta with { CommitIndex = target });
                commitAdvanced = true;
            }
        }

        if (commitAdvanced || metaDirty)
            await FollowerLogDurable.PersistMetaAsync(this, cancellationToken).ConfigureAwait(false);
        if (commitAdvanced)
            _faults.OnCommitAdvanced();

        return new FollowerLogAppendResult(true, string.Empty, _meta.CurrentTerm, _lastLogIndex);
    }

    private FollowerLogAppendResult FailReadiness()
    {
        Readiness = FollowerLogReadiness.Failed;
        return new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, _meta.CurrentTerm, _lastLogIndex);
    }

    private FollowerLogAppendResult? PrepareAppendBatch(FollowerLogAppendRequest request, out List<FollowerLogEntry>? toAppend, out ulong? truncateAtIndex)
    {
        toAppend = null;
        truncateAtIndex = null;
        var nextExpected = request.PrevLogIndex + 1;
        var entries = request.Entries.Span;

        for (var i = 0; i < entries.Length; i++)
        {
            var entry = entries[i];
            if (entry.LogIndex != nextExpected)
                return new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, _meta.CurrentTerm, _lastLogIndex);

            nextExpected++;

            // Once the divergent tail is being rewritten, every subsequent entry must be re-appended durably.
            if (truncateAtIndex is not null)
            {
                toAppend!.Add(entry);
                continue;
            }

            // Entries already satisfied by local state need no durable writing: duplicates already present with
            // identical content, and applied entries whose payloads were released after application (Leader
            // Completeness guarantees a current-term leader cannot conflict at an applied index).
            if (IsSatisfiedByLocalState(in entry))
                continue;

            if (entry.LogIndex <= _meta.CommitIndex)
                return FailReadiness();

            if (entry.LogIndex <= _lastLogIndex)
                truncateAtIndex = entry.LogIndex;

            toAppend ??= [];
            toAppend.Add(entry);
        }

        return null;

        bool IsSatisfiedByLocalState(in FollowerLogEntry candidate)
        {
            if (candidate.LogIndex <= _lastLogIndex && _entries.TryGetValue(candidate.LogIndex, out var existing) && existing.Term == candidate.Term &&
                existing.PayloadSpan.SequenceEqual(candidate.PayloadSpan))
                return true;

            return candidate.LogIndex <= _meta.LastAppliedIndex;
        }
    }

    private FollowerLogAppendResult? VerifyPreviousLogConsistency(FollowerLogAppendRequest request)
    {
        // Previous-log consistency; the term at an applied index was released from memory, so the check
        // covers only the retained region above the applied watermark.
        if (request.PrevLogIndex > _lastLogIndex)
            return new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, _meta.CurrentTerm, _lastLogIndex);

        if (request.PrevLogIndex <= 0 || request.PrevLogIndex <= _meta.LastAppliedIndex || TermAt(request.PrevLogIndex) == request.PrevLogTerm)
            return null;

        // A term conflict at or below the committed index violates Leader Completeness; fail readiness.
        if (request.PrevLogIndex <= _meta.CommitIndex)
            return FailReadiness();

        return new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, _meta.CurrentTerm, _lastLogIndex);

        ulong TermAt(ulong logIndex)
        {
            return _entries.TryGetValue(logIndex, out var entry) ? entry.Term : 0UL;
        }
    }

    private void SetLastLogIndex(ulong value) => _lastLogIndex = value;

    private void SetLogLength(long value) => _logLength = value;

    private void SetMeta(GroupLogMetadata meta) => _meta = meta;

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

            // The durability worker flushes the exact-size buffer and propagates any I/O faults.
            var work = new AppendDurableWork(owner._durability, buffer, startOffset, totalLength, owner._faults);
            await Task.Factory.StartNew(AppendDurableCallback, work, cancellationToken, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).ConfigureAwait(false);

            // Only after the durable writing succeeds do the in-memory indexes gain the entries,
            // so a crash mid-appending can never leave the index ahead of the file.
            for (var i = 0; i < offsets.Count; i++)
            {
                var entry = toAppend[i];
                owner._entryOffsets[offsets[i].Key] = offsets[i].Value;
                owner._entries[offsets[i].Key] = entry with { Payload = BufferEx.CopyToOwned(entry.PayloadSpan) };
            }

            owner.SetLastLogIndex(offsets[^1].Key);
            owner.SetLogLength(startOffset + totalLength);
            owner.SetMeta(owner._meta with { LastLogIndex = owner._lastLogIndex });
        }

        internal static Task PersistMetaAsync(FollowerLog owner, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var encodedLength = GroupLogCodec.ComputeMetaEncodedLength(owner._meta);
            var buffer = ArrayPool<byte>.Shared.Rent(encodedLength);
            GroupLogCodec.EncodeMeta(owner._meta, buffer.AsSpan(0, encodedLength));
            var work = new MetaDurableWork(owner._metaTempPath, owner._metaPath, buffer, encodedLength);
            return Task.Factory.StartNew(MetaDurableCallback, work, cancellationToken, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
        }

        internal static async Task TruncateFromAsync(FollowerLog owner, ulong logIndex, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!owner._entryOffsets.TryGetValue(logIndex, out var byteOffset))
                throw new InvalidOperationException($"Replica group '{owner.GroupId}' cannot truncate from a missing index '{logIndex}'.");

            var work = new TruncateDurableWork(owner._durability, byteOffset, owner._faults);
            await Task.Factory.StartNew(TruncateDurableCallback, work, cancellationToken, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).ConfigureAwait(false);

            var truncated = new List<ulong>();
            foreach (var index in owner._entries.Keys)
            {
                if (index >= logIndex)
                    truncated.Add(index);
            }

            for (var i = 0; i < truncated.Count; i++)
            {
                _ = owner._entries.Remove(truncated[i]);
                _ = owner._entryOffsets.Remove(truncated[i]);
            }

            owner.SetLastLogIndex(logIndex - 1);
            owner.SetLogLength(byteOffset);
            owner.SetMeta(owner._meta with { LastLogIndex = owner._lastLogIndex });
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

            var bytes = await File.ReadAllBytesAsync(owner._logPath, cancellationToken).ConfigureAwait(false);

            // An empty file is treated the same as no file at all.
            if (bytes.Length is 0)
            {
                ResetLogState(owner);
                EnsureCommittedPrefixCovered(owner);
                return;
            }

            // Anything preceding the expected header marks the file as unusable.
            if (bytes.Length < GroupLogCodec.LogFileHeader.Length || !bytes.AsSpan(0, GroupLogCodec.LogFileHeader.Length).SequenceEqual(GroupLogCodec.LogFileHeader))
            {
                owner.Readiness = FollowerLogReadiness.Failed;
                throw new InvalidDataException($"Replica group '{owner.GroupId}' log header is corrupt.");
            }

            owner._entries.Clear();
            owner._entryOffsets.Clear();
            owner.SetLastLogIndex(0);

            var result = WalkFrames(owner, bytes);

            // A torn trailing frame is truncated back to the last valid boundary on disk,
            // since a CRC mismatch alone cannot tell an appended tail from corruption.
            if (result.Truncated && result.LastValidEnd < bytes.Length)
            {
                var work = new RecoveryTruncateWork(owner._logPath, result.LastValidEnd);
                await Task.Factory.StartNew(RecoveryTruncateCallback, work, cancellationToken, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).ConfigureAwait(false);
            }

            owner.SetLogLength(result.LastValidEnd);
            owner.SetMeta(owner._meta with { LastLogIndex = owner._lastLogIndex });
            PruneAppliedEntries(owner);
            EnsureCommittedPrefixCovered(owner);
        }

        private static void EnsureCommittedPrefixCovered(FollowerLog owner)
        {
            if (owner._meta.CommitIndex <= owner._lastLogIndex)
                return;
            owner.Readiness = FollowerLogReadiness.Failed;
            throw new InvalidDataException($"Replica group '{owner.GroupId}' commit index exceeds the durable log.");
        }

        private static void ResetLogState(FollowerLog owner)
        {
            owner.SetLogLength(0);
            owner.SetLastLogIndex(0);
            owner.SetMeta(owner._meta with { LastLogIndex = 0 });
        }

        private static bool TryReadFrameAt(ReadOnlySpan<byte> buffer, int offset, out FollowerLogEntry entry, out int consumed)
        {
            entry = default;
            consumed = 0;
            if (buffer.Length - offset < 9)
                return false;

            var bodyLength = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(offset + 5, 4));
            if (bodyLength < 20)
                return false;

            if (offset + 9 + bodyLength + 4 > buffer.Length)
                return false;

            if (!GroupLogCodec.TryReadFrame(buffer[offset..], out entry, out consumed))
                return false;

            return true;
        }

        private static WalkResult WalkFrames(FollowerLog owner, byte[] bytes)
        {
            var buffer = bytes.AsSpan();
            var offset = GroupLogCodec.LogFileHeader.Length;
            var lastValidEnd = offset;

            while (offset < bytes.Length)
            {
                var nextLogIndex = owner._lastLogIndex + 1;
                if (!TryReadFrameAt(buffer, offset, out var entry, out var consumed))
                {
                    if (nextLogIndex > owner._meta.CommitIndex)
                        return new WalkResult(lastValidEnd, true);
                    owner.Readiness = FollowerLogReadiness.Failed;
                    throw new InvalidDataException($"Replica group '{owner.GroupId}' committed log frame at index '{nextLogIndex}' is corrupt.");
                }

                if (entry.LogIndex != nextLogIndex)
                {
                    if (nextLogIndex > owner._meta.CommitIndex)
                        return new WalkResult(lastValidEnd, true);
                    owner.Readiness = FollowerLogReadiness.Failed;
                    throw new InvalidDataException($"Replica group '{owner.GroupId}' committed log has a gap at index '{nextLogIndex}'.");
                }

                owner._entryOffsets[entry.LogIndex] = offset;
                owner._entries[entry.LogIndex] = entry;
                owner.SetLastLogIndex(entry.LogIndex);
                offset += consumed;
                lastValidEnd = offset;
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
