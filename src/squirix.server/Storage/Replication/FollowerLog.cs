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
///     Only the committed prefix is exposed through the storage contract; the uncommitted tail is retained on
///     disk for pending-operation rebuild but is never applied to memory.
///     </para>
///     <para>
///     Append follows the following half of the consensus AppendEntries rule: previous
///     <c>(term, log_index)</c> consistency, consecutive append without gaps, idempotent duplicate
///     acknowledgement, higher-term persistence before response, and committed-prefix conflicts fail readiness.
///     An uncommitted entry that conflicts with the leader's batch truncates the divergent tail, which is then
///     rewritten with the leader's entries before the append is acknowledged.
///     </para>
/// </remarks>
internal sealed class FollowerLog : IFollowerLog
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

    private static readonly IFollowerLogFaultHooks NoOpFaults = new NoOpFaultHooks();

    private static readonly Action<object?> RecoveryTruncateCallback = static state =>
    {
        if (state is RecoveryTruncateWork work)
            work.Execute();
    };

    private static readonly Action<object?> TruncateDurableCallback = static state =>
    {
        if (state is TruncateDurableWork work)
            work.Execute();
    };

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
        _metaPath = Path.Join(_groupDir, "group.meta");
        _metaTempPath = Path.Join(_groupDir, "group.meta.tmp");
        _logPath = Path.Join(_groupDir, "group.log");
    }

    /// <inheritdoc />
    public string GroupId { get; }

    /// <inheritdoc />
    public FollowerLogReadiness Readiness { get; private set; } = FollowerLogReadiness.Unknown;

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

            _meta = _meta with { CommitIndex = commitIndex };
            await PersistMetaAsync(cancellationToken).ConfigureAwait(false);
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

            // Higher term is persisted durably before any further response; the old leader stops being authoritative.
            if (request.CurrentTerm > _meta.CurrentTerm)
            {
                _meta = _meta with { CurrentTerm = request.CurrentTerm, VotedFor = string.Empty };
                await PersistMetaAsync(cancellationToken).ConfigureAwait(false);
            }
            else if (request.CurrentTerm < _meta.CurrentTerm)
            {
                return new FollowerLogAppendResult(false, FollowerLogRefusal.StaleTerm, _meta.CurrentTerm, _lastLogIndex);
            }

            // Previous-log consistency.
            if (request.PrevLogIndex > _lastLogIndex)
                return new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, _meta.CurrentTerm, _lastLogIndex);

            if (request.PrevLogIndex > 0 && TermAt(request.PrevLogIndex) != request.PrevLogTerm)
            {
                // A term conflict at or below the committed index is irreconcilable: the leader disagrees with a
                // committed entry, which violates Leader Completeness, so the follower fails readiness.
                if (request.PrevLogIndex <= _meta.CommitIndex)
                    return FailReadiness();

                return new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, _meta.CurrentTerm, _lastLogIndex);
            }

            // Validate the whole batch for contiguity and conflicts before writing anything.
            var entries = request.Entries;
            if (entries.Length is 0)
                return await CompleteAppendAsync(request.LeaderCommitIndex, cancellationToken).ConfigureAwait(false);

            var error = PrepareAppendBatch(request, out var toAppend, out var truncateAtIndex);
            if (error is not null)
                return error.Value;

            if (truncateAtIndex is not null)
                await TruncateFromAsync(truncateAtIndex.Value, cancellationToken).ConfigureAwait(false);

            if (toAppend is { Count: > 0 })
                await AppendFramesDurableAsync(toAppend, cancellationToken).ConfigureAwait(false);

            return await CompleteAppendAsync(request.LeaderCommitIndex, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            _disposed = true;
            _durability.Dispose();
            _ = FileEx.TryDeleteFile(_metaTempPath);
        }
        finally
        {
            _ = _gate.Release();
        }

        _gate.Dispose();
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
                _meta = new GroupLogMetadata(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 0UL, string.Empty, 0UL, 0UL, 0UL);
                _lastLogIndex = 0;
                _logLength = 0;
                await PersistMetaAsync(cancellationToken).ConfigureAwait(false);
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

                _meta = decoded;
            }
            else
            {
                _meta = new GroupLogMetadata(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 0UL, string.Empty, 0UL, 0UL, 0UL);
            }

            await RecoverLogFileAsync(cancellationToken).ConfigureAwait(false);
            _durability.Open(_logPath, _logLength);
            Readiness = FollowerLogReadiness.Ready;
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    private static FollowerLogEntry FindEntry(List<FollowerLogEntry> toAppend, ulong logIndex)
    {
        for (var i = 0; i < toAppend.Count; i++)
        {
            if (toAppend[i].LogIndex == logIndex)
                return toAppend[i];
        }

        throw new InvalidOperationException("Appended entry not found.");
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

    private async Task AppendFramesDurableAsync(List<FollowerLogEntry> toAppend, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var writeHeader = _logLength == 0;
        var totalLength = writeHeader ? GroupLogCodec.LogFileHeader.Length : 0;
        for (var i = 0; i < toAppend.Count; i++)
            totalLength += GroupLogCodec.ComputeFrameEncodedLength(toAppend[i].Payload.Length);

        var buffer = ArrayPool<byte>.Shared.Rent(totalLength);
        var position = writeHeader ? GroupLogCodec.LogFileHeader.Length : 0;
        var startOffset = writeHeader ? 0 : _logLength;

        if (writeHeader)
            GroupLogCodec.LogFileHeader.CopyTo(buffer);

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

        var work = new AppendDurableWork(_durability, buffer, startOffset, totalLength, _faults);
        await Task.Factory.StartNew(AppendDurableCallback, work, cancellationToken, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).ConfigureAwait(false);
        for (var i = 0; i < offsets.Count; i++)
        {
            var entry = FindEntry(toAppend, offsets[i].Key);
            _entryOffsets[offsets[i].Key] = offsets[i].Value;
            _entries[offsets[i].Key] = entry with { Payload = BufferEx.CopyToOwned(entry.PayloadSpan) };
        }

        _lastLogIndex = offsets[^1].Key;
        _logLength = startOffset + totalLength;
    }

    private async Task<FollowerLogAppendResult> CompleteAppendAsync(ulong leaderCommitIndex, CancellationToken cancellationToken)
    {
        var commitAdvanced = false;
        if (leaderCommitIndex > _meta.CommitIndex)
        {
            var target = Math.Min(leaderCommitIndex, _lastLogIndex);
            if (target > _meta.CommitIndex)
            {
                _meta = _meta with { CommitIndex = target };
                commitAdvanced = true;
            }
        }

        await PersistMetaAsync(cancellationToken).ConfigureAwait(false);
        if (commitAdvanced)
            _faults.OnCommitAdvanced();

        return new FollowerLogAppendResult(true, string.Empty, _meta.CurrentTerm, _lastLogIndex);
    }

    private FollowerLogAppendResult FailReadiness()
    {
        Readiness = FollowerLogReadiness.Failed;
        return new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, _meta.CurrentTerm, _lastLogIndex);
    }

    private Task PersistMetaAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var encodedLength = GroupLogCodec.ComputeMetaEncodedLength(_meta);
        var buffer = ArrayPool<byte>.Shared.Rent(encodedLength);
        GroupLogCodec.EncodeMeta(_meta, buffer.AsSpan(0, encodedLength));
        var work = new MetaDurableWork(_metaTempPath, _metaPath, buffer, encodedLength);
        return Task.Factory.StartNew(MetaDurableCallback, work, cancellationToken, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
    }

    private FollowerLogAppendResult? PrepareAppendBatch(FollowerLogAppendRequest request, out List<FollowerLogEntry>? toAppend, out ulong? truncateAtIndex)
    {
        toAppend = null;
        truncateAtIndex = null;
        var nextExpected = request.PrevLogIndex + 1;

        for (var i = 0; i < request.Entries.Length; i++)
        {
            var entry = request.Entries.Span[i];
            if (entry.LogIndex != nextExpected)
                return new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, _meta.CurrentTerm, _lastLogIndex);

            nextExpected++;

            // Once the divergent tail is being rewritten, every subsequent entry must be re-appended durably.
            if (truncateAtIndex is not null)
            {
                toAppend ??= [];
                toAppend.Add(entry);
                continue;
            }

            // Already present locally with identical content: no durable write is required.
            if (entry.LogIndex <= _lastLogIndex && TryGetEntry(entry.LogIndex, out var existing) &&
                existing.Term == entry.Term && existing.PayloadSpan.SequenceEqual(entry.PayloadSpan))
                continue;

            if (entry.LogIndex <= _meta.CommitIndex)
                return FailReadiness();

            if (entry.LogIndex <= _lastLogIndex)
                truncateAtIndex = entry.LogIndex;

            toAppend ??= [];
            toAppend.Add(entry);
        }

        return null;
    }

    private async Task TruncateFromAsync(ulong logIndex, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!_entryOffsets.TryGetValue(logIndex, out var byteOffset))
            throw new InvalidOperationException($"Replica group '{GroupId}' cannot truncate from a missing index '{logIndex}'.");

        var work = new TruncateDurableWork(_durability, byteOffset, _faults);
        await Task.Factory.StartNew(TruncateDurableCallback, work, cancellationToken, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).ConfigureAwait(false);

        var truncated = new List<ulong>();
        foreach (var index in _entries.Keys)
        {
            if (index >= logIndex)
                truncated.Add(index);
        }

        for (var i = 0; i < truncated.Count; i++)
        {
            _ = _entries.Remove(truncated[i]);
            _ = _entryOffsets.Remove(truncated[i]);
        }

        _lastLogIndex = logIndex - 1;
        _logLength = byteOffset;
        _meta = _meta with { LastLogIndex = _lastLogIndex };
    }

    private async Task RecoverLogFileAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(_logPath))
        {
            ResetLogState();
            EnsureCommittedPrefixCovered();
            return;
        }

        var bytes = await File.ReadAllBytesAsync(_logPath, cancellationToken).ConfigureAwait(false);
        if (bytes.Length is 0)
        {
            ResetLogState();
            EnsureCommittedPrefixCovered();
            return;
        }

        if (bytes.Length < GroupLogCodec.LogFileHeader.Length || !bytes.AsSpan(0, GroupLogCodec.LogFileHeader.Length).SequenceEqual(GroupLogCodec.LogFileHeader))
        {
            Readiness = FollowerLogReadiness.Failed;
            throw new InvalidDataException($"Replica group '{GroupId}' log header is corrupt.");
        }

        _entries.Clear();
        _entryOffsets.Clear();
        _lastLogIndex = 0;

        var result = WalkFrames(bytes);
        if (result.Truncated && result.LastValidEnd < bytes.Length)
        {
            var work = new RecoveryTruncateWork(_logPath, result.LastValidEnd);
            await Task.Factory.StartNew(RecoveryTruncateCallback, work, cancellationToken, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).ConfigureAwait(false);
        }

        _logLength = result.LastValidEnd;
        _meta = _meta with { LastLogIndex = _lastLogIndex };
        EnsureCommittedPrefixCovered();
        return;

        void EnsureCommittedPrefixCovered()
        {
            if (_meta.CommitIndex <= _lastLogIndex)
                return;
            Readiness = FollowerLogReadiness.Failed;
            throw new InvalidDataException($"Replica group '{GroupId}' commit index exceeds the durable log.");
        }
    }

    private void ResetLogState()
    {
        _logLength = 0;
        _lastLogIndex = 0;
        _meta = _meta with { LastLogIndex = 0 };
    }

    private ulong TermAt(ulong logIndex) => _entries.TryGetValue(logIndex, out var entry) ? entry.Term : 0UL;

    private bool TryGetEntry(ulong logIndex, out FollowerLogEntry entry) => _entries.TryGetValue(logIndex, out entry);

    private WalkResult WalkFrames(byte[] bytes)
    {
        var buffer = bytes.AsSpan();
        var offset = GroupLogCodec.LogFileHeader.Length;
        var lastValidEnd = offset;

        while (offset < bytes.Length)
        {
            var nextLogIndex = _lastLogIndex + 1;
            if (!TryReadFrameAt(buffer, offset, out var entry, out var consumed))
            {
                if (nextLogIndex > _meta.CommitIndex)
                    return new WalkResult(lastValidEnd, true);
                Readiness = FollowerLogReadiness.Failed;
                throw new InvalidDataException($"Replica group '{GroupId}' committed log frame at index '{nextLogIndex}' is corrupt.");
            }

            if (entry.LogIndex != nextLogIndex)
            {
                if (nextLogIndex > _meta.CommitIndex)
                    return new WalkResult(lastValidEnd, true);
                Readiness = FollowerLogReadiness.Failed;
                throw new InvalidDataException($"Replica group '{GroupId}' committed log has a gap at index '{nextLogIndex}'.");
            }

            _entryOffsets[entry.LogIndex] = offset;
            _entries[entry.LogIndex] = entry;
            _lastLogIndex = entry.LogIndex;
            offset += consumed;
            lastValidEnd = offset;
        }

        return new WalkResult(lastValidEnd, false);
    }

    /// <summary>Result of walking the log frames during startup recovery.</summary>
    /// <param name="LastValidEnd">The byte offset after the last valid frame.</param>
    /// <param name="Truncated">Determines whether a divergent tail was truncated.</param>
    private readonly record struct WalkResult(long LastValidEnd, bool Truncated);

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
