using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;
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
///     <c language="csharp">(term, log_index)</c> consistency, consecutive append without gaps, idempotent duplicate
///     acknowledgement, higher-term persistence before response, and committed-prefix conflicts fail readiness.
///     An uncommitted entry that conflicts with the leader's batch truncates the divergent tail, which is then
///     rewritten with the leader's entries before the appending is acknowledged.
///     </para>
/// </remarks>
[SuppressMessage(
    "Maintainability",
    "SQR003",
    Justification = "Snapshot and idempotency state belong to the durable follower-log lifecycle; extracting them would split its gate-owned invariants.")]
[SuppressMessage(
    "Maintainability",
    "MA0051",
    Justification = "Recovery intentionally keeps the file-header, snapshot-baseline, and torn-tail reconciliation in one gated transaction.")]
internal sealed class FollowerLog : IFollowerLog, IFollowerLogContext
{
    private static readonly IFollowerLogFaultHooks DefaultFaults = new NoOpFaultHooks();

    private readonly GroupComposition _composition;
    private readonly GroupLogDurability _durability = new();
    private readonly IFollowerLogFaultHooks _faults;

    [SuppressMessage(
        "Reliability",
        "CA2213:Disposable fields should be disposed",
        Justification = "Disposing _gate may throw ObjectDisposedException in synchronous readers blocked on Wait(); idempotent disposal guarded by _disposed.")]
    private readonly AsyncLock _gate = new();

    private readonly GroupIdempotencyState _idempotency;

    private readonly FollowerLogJournal _journal;

    private int _disposed;
    private ulong _lastLogIndex;

    private long _logLength;
    private GroupLogMetadata _meta;

    internal FollowerLog(string persistenceRoot, string groupId, GroupComposition composition, FollowerLogOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        ArgumentNullException.ThrowIfNull(composition);
        _composition = composition;
        var settings = options ?? new FollowerLogOptions();
        _faults = settings.FaultHooks ?? DefaultFaults;
        GroupId = groupId;
        var paths = FollowerLogPaths.Create(persistenceRoot, groupId);
        var snapshot = new GroupSnapshotStore(persistenceRoot, groupId, settings.MaxSnapshotBytes);
        _journal = new FollowerLogJournal(paths, snapshot);
        _idempotency = new GroupIdempotencyState(
            settings.IdempotencyCapacity,
            settings.IdempotencyRetention ?? GroupIdempotencyState.DefaultRetention,
            settings.TimeProvider ?? TimeProvider.System);
        Idempotency = _idempotency;
    }

    internal FollowerLog(string persistenceRoot, string groupId, GroupComposition composition, IFollowerLogFaultHooks faultHooks)
        : this(persistenceRoot, groupId, composition, new FollowerLogOptions { FaultHooks = faultHooks })
    {
    }

    /// <inheritdoc />
    GroupLogDurability IFollowerLogDurability.Durability => _durability;

    /// <inheritdoc />
    IFollowerLogFaultHooks IFollowerLogDurability.Faults => _faults;

    public string GroupId { get; }

    /// <inheritdoc />
    GroupIdempotencyState IFollowerLogState.Idempotency => _idempotency;

    /// <inheritdoc />
    ulong IFollowerLogState.LastLogIndex => _lastLogIndex;

    /// <inheritdoc />
    long IFollowerLogDurability.LogLength => _logLength;

    /// <inheritdoc />
    GroupLogMetadata IFollowerLogState.Meta => _meta;

    public FollowerLogReadiness Readiness { get; private set; } = FollowerLogReadiness.Unknown;

    /// <summary>Gets the durable idempotency state of the replica group.</summary>
    internal GroupIdempotencyState Idempotency { get; }

    /// <summary>Gets the published snapshot file path, or <see langword="null" /> when none is published.</summary>
    internal string? SnapshotPath => _journal.Snapshot.SnapshotExists ? _journal.Snapshot.SnapshotPath : null;

    /// <summary>Gets a value indicating whether the log has been disposed.</summary>
    private bool IsDisposed => Volatile.Read(ref _disposed) != 0;

    /// <inheritdoc />
    Task<GroupSnapshotInstallResult> IFollowerLog.InstallSnapshotAsync(GroupSnapshot snapshot, CancellationToken cancellationToken) =>
        InstallSnapshotAsync(snapshot, cancellationToken);

    /// <inheritdoc />
    public async Task<FollowerLogAppliedResult> AdvanceAppliedAsync(ulong appliedIndex, CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);

        if (IsDisposed || Readiness != FollowerLogReadiness.Ready)
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
        await FollowerLogAppend.PersistMetaOrFailReadinessAsync(_journal, this, candidate, cancellationToken).ConfigureAwait(false);
        SetMeta(candidate);
        FollowerLogRecovery.PruneAppliedEntries(_journal, this);
        return new FollowerLogAppliedResult(true, string.Empty, appliedIndex);
    }

    /// <inheritdoc />
    public async Task<FollowerLogCommitResult> AdvanceCommitAsync(ulong commitIndex, CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);

        if (IsDisposed || Readiness != FollowerLogReadiness.Ready)
            return new FollowerLogCommitResult(false, FollowerLogRefusal.NotReady, _meta.CommitIndex);

        // Commit index moves only monotonically.
        if (commitIndex <= _meta.CommitIndex)
            return new FollowerLogCommitResult(true, string.Empty, _meta.CommitIndex);

        // Never beyond the locally durable last index.
        if (commitIndex > _lastLogIndex)
            return new FollowerLogCommitResult(false, FollowerLogRefusal.NotReady, _meta.CommitIndex);

        var candidate = _meta with { CommitIndex = commitIndex };
        await FollowerLogAppend.PersistMetaOrFailReadinessAsync(_journal, this, candidate, cancellationToken).ConfigureAwait(false);
        SetMeta(candidate);
        _faults.OnCommitAdvanced();
        return new FollowerLogCommitResult(true, string.Empty, commitIndex);
    }

    /// <inheritdoc />
    public async Task<FollowerLogAppendResult> AppendAsync(FollowerLogAppendRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request.LeaderNodeId);

        // Payload ownership is materialized lazily: the caller is blocked on this append, so validation
        // reads its buffer directly and only entries that will actually hit disk are copied (inside
        // AppendVerifiedBatchAsync, synchronously after PrepareAppendBatch and before any await).
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);

        if (IsDisposed || Readiness != FollowerLogReadiness.Ready)
            return new FollowerLogAppendResult(false, FollowerLogRefusal.NotReady, _meta.CurrentTerm, _lastLogIndex);

        var termError = await FollowerLogAppend.AdvanceTermIfHigherAsync(_journal, this, request, cancellationToken).ConfigureAwait(false);
        if (termError != null)
            return termError.Value;

        var consistencyError = FollowerLogAppend.VerifyPreviousLogConsistency(_journal, this, request);
        return consistencyError ?? await FollowerLogAppend.AppendVerifiedBatchAsync(_journal, this, request, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<FollowerLogReconcileResult> ReconcileTailAsync(ulong fromIndex, ulong prevLogTerm, ulong leaderTerm, CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);

        if (IsDisposed || Readiness != FollowerLogReadiness.Ready)
            return new FollowerLogReconcileResult(false, FollowerLogRefusal.NotReady, _lastLogIndex, 0, Readiness == FollowerLogReadiness.Failed);

        // A stale leader term authorizes nothing: unlike the append path, repair never advances the
        // current term, it only refuses instructions from deposed leaders.
        if (leaderTerm < _meta.CurrentTerm)
            return new FollowerLogReconcileResult(false, FollowerLogRefusal.StaleTerm, _lastLogIndex, 0, false);

        // A repair instruction is lifecycle-owned and trusted, but it still fails closed if it would cross the
        // durable commit boundary. No leader response may destructively revise a committed prefix.
        // A zero index is caller-bug input rather than storage corruption, so it is refused without quarantine.
        if (fromIndex == 0UL)
            return new FollowerLogReconcileResult(false, FollowerLogRefusal.LogMismatch, _lastLogIndex, 0, false);

        if (fromIndex <= _meta.CommitIndex)
        {
            SetReadiness(FollowerLogReadiness.Failed);
            return new FollowerLogReconcileResult(false, FollowerLogRefusal.LogMismatch, _lastLogIndex, 0, true);
        }

        if (fromIndex == _lastLogIndex + 1UL)
            return new FollowerLogReconcileResult(true, string.Empty, _lastLogIndex, 0, false);

        if (fromIndex > _lastLogIndex || !_journal.EntryOffsets.ContainsKey(fromIndex))
            return new FollowerLogReconcileResult(false, FollowerLogRefusal.LogMismatch, _lastLogIndex, 0, false);

        // Previous-log consistency, mirroring the append path: a stale leader must not truncate a tail
        // written by the current term. An unverifiable predecessor (compacted below the snapshot baseline)
        // is refused without quarantine; a term conflict at or below the commit boundary fails readiness.
        if (!CheckPrevTerm(fromIndex - 1UL, prevLogTerm))
        {
            if (fromIndex - 1UL <= _meta.CommitIndex)
            {
                SetReadiness(FollowerLogReadiness.Failed);
                return new FollowerLogReconcileResult(false, FollowerLogRefusal.LogMismatch, _lastLogIndex, 0, true);
            }

            return new FollowerLogReconcileResult(false, FollowerLogRefusal.LogMismatch, _lastLogIndex, 0, false);
        }

        try
        {
            var released = await FollowerLogDurable.TruncateFromAsync(_journal, this, fromIndex, cancellationToken).ConfigureAwait(false);
            return new FollowerLogReconcileResult(true, string.Empty, _lastLogIndex, released, false);
        }
        catch
        {
            // The low-level operation reconciles its indexes with any possible SetLength outcome. The explicit
            // repair path additionally quarantines storage because the caller cannot prove which durable boundary
            // survived an I/O fault until restart recovery scans the file again.
            SetReadiness(FollowerLogReadiness.Failed);
            throw;
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        using var lockGuard = await _gate.LockAsync(CancellationToken.None).ConfigureAwait(false);
        _durability.Dispose();
        _ = FileEx.TryDeleteFile(_journal.Paths.MetadataTempPath);
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<FollowerLogEntry>> GetCommittedEntriesAsync(CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);

        _faults.OnBeforeMemoryApply();
        var result = new List<FollowerLogEntry>();
        foreach (var pair in _journal.Entries)
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

    /// <inheritdoc />
    public async ValueTask<FollowerLogStatus> GetStatusAsync(CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);

        return new FollowerLogStatus(
            GroupId,
            _meta.TopologyFingerprint,
            _meta.ConfigurationGeneration,
            _meta.CurrentTerm,
            _meta.VotedFor,
            _lastLogIndex,
            LastLogTerm(),
            _meta.CommitIndex,
            _meta.LastAppliedIndex,
            Readiness);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<FollowerLogEntry>> GetUncommittedTailAsync(CancellationToken cancellationToken)
    {
        var result = new List<FollowerLogEntry>();
        foreach (var pair in _journal.Entries)
        {
            if (pair.Key <= _meta.CommitIndex)
                continue;

            result.Add(pair.Value);
        }

        return ValueTask.FromResult<IReadOnlyList<FollowerLogEntry>>(result);
    }

    /// <summary>
    /// Installs the snapshot baseline without pruning the retained indexes. Reserved for paths whose index
    /// lifecycle is owned separately: snapshot publication retains the covered prefix until the applied watermark
    /// releases it, while recovery and installation rebuild both indexes from the retained tail afterward.
    /// </summary>
    /// <param name="baseline">The restored snapshot baseline.</param>
    void IFollowerLogContext.RestoreBaseline(SnapshotBaseline baseline) => _journal.RestoreBaseline(baseline);

    /// <inheritdoc />
    void IFollowerLogState.SetLastLogIndex(ulong logIndex) => SetLastLogIndex(logIndex);

    /// <inheritdoc />
    void IFollowerLogState.SetLogLength(long logLength) => SetLogLength(logLength);

    /// <inheritdoc />
    void IFollowerLogState.SetMeta(GroupLogMetadata meta) => SetMeta(meta);

    /// <inheritdoc />
    void IFollowerLogState.SetReadiness(FollowerLogReadiness readiness) => SetReadiness(readiness);

    /// <summary>Compacts the journal prefix covered by the published snapshot, retaining the installable state.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The compaction outcome.</returns>
    internal async Task<GroupCompactionResult> CompactAsync(CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);

        if (IsDisposed || Readiness != FollowerLogReadiness.Ready)
            return new GroupCompactionResult(false, null, FollowerLogRefusal.NotReady);

        return await FollowerLogSnapshot.CompactAsync(_journal, this, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Creates and durably publishes a snapshot covering the committed prefix up to <paramref name="lastIncludedIndex" />.</summary>
    /// <param name="lastIncludedIndex">The highest committed journal index the snapshot covers.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The published snapshot.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the log is not ready or the snapshot would include uncommitted entries.</exception>
    internal async Task<GroupSnapshot> CreateSnapshotAsync(ulong lastIncludedIndex, CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);

        if (IsDisposed || Readiness != FollowerLogReadiness.Ready)
            throw new InvalidOperationException($"Replica group '{GroupId}' cannot create a snapshot; the log is {(IsDisposed ? "disposed" : "not ready")}.");

        FollowerLogSnapshot.ValidateSnapshotRequest(_journal, this, lastIncludedIndex);
        var snapshot = FollowerLogSnapshot.BuildSnapshot(_journal, this, lastIncludedIndex);
        await _journal.Snapshot.PublishAsync(snapshot, cancellationToken).ConfigureAwait(false);

        // The covered prefix stays readable in memory until the applied watermark releases it,
        // so publication records the baseline alone instead of pruning the indexes.
        _journal.RestoreBaseline(new SnapshotBaseline(snapshot.LastIncludedIndex, snapshot.LastIncludedTerm));
        return snapshot;
    }

    /// <summary>Installs a validated snapshot, resetting the journal to start at its included index plus one.</summary>
    /// <param name="snapshot">The snapshot to install.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The installation outcome.</returns>
    internal async Task<GroupSnapshotInstallResult> InstallSnapshotAsync(GroupSnapshot snapshot, CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);

        if (IsDisposed || Readiness != FollowerLogReadiness.Ready)
            return GroupSnapshotInstallResult.Refused(FollowerLogRefusal.NotReady);

        return await FollowerLogSnapshot.InstallAsync(_journal, this, snapshot, cancellationToken).ConfigureAwait(false);
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
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);

        await OpenCoreAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task OpenCoreAsync(CancellationToken cancellationToken)
    {
        if (!_composition.Contains(GroupId))
            throw new InvalidOperationException($"Group '{GroupId}' is not part of the local static composition.");

        _ = await DirectoryEx.CreateDirectoryAsync(_journal.Paths.GroupDirectory, cancellationToken: cancellationToken).ConfigureAwait(false);
        _ = FileEx.TryDeleteFile(_journal.Paths.MetadataTempPath);
        _ = FileEx.TryDeleteFile(_journal.Paths.LogTempPath);

        var metaExists = File.Exists(_journal.Paths.MetadataPath);
        var logExists = File.Exists(_journal.Paths.LogPath);

        switch (metaExists)
        {
            case false when !logExists && !_journal.Snapshot.SnapshotExists:
                var fresh = new GroupLogMetadata(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 0UL, string.Empty, 0UL, 0UL, 0UL);
                await FollowerLogAppend.PersistMetaOrFailReadinessAsync(_journal, this, fresh, cancellationToken).ConfigureAwait(false);
                SetMeta(fresh);
                SetLastLogIndex(0);
                SetLogLength(0);
                _durability.Open(_journal.Paths.LogPath, _logLength);
                Readiness = FollowerLogReadiness.Ready;
                return;

            case false when !logExists:
                // A published snapshot is durable state even when metadata and the log are absent. Seed recovery with
                // empty metadata so RestoreSnapshotBaseAsync validates the snapshot instead of creating zeroed ready state.
                SetMeta(new GroupLogMetadata(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 0UL, string.Empty, 0UL, 0UL, 0UL));
                await FollowerLogRecovery.RecoverLogFileAsync(_journal, this, cancellationToken).ConfigureAwait(false);
                _durability.Open(_journal.Paths.LogPath, _logLength);
                Readiness = FollowerLogReadiness.Ready;
                return;
            case true:
                var metaBytes = await File.ReadAllBytesAsync(_journal.Paths.MetadataPath, cancellationToken).ConfigureAwait(false);
                if (!GroupLogCodec.TryDecodeMeta(metaBytes, out var decoded) || !string.Equals(decoded.GroupId, GroupId, StringComparison.Ordinal))
                {
                    Readiness = FollowerLogReadiness.Failed;
                    throw new InvalidDataException($"Replica group '{GroupId}' metadata is corrupt.");
                }

                SetMeta(decoded);
                break;

            default:
                // The log file exists without its atomically published metadata, so the committed boundary is unknown.
                // Assuming CommitIndex = 0 would treat every durable frame as an uncommitted tail and truncate it,
                // destroying possibly-committed data. Fail readiness instead; the group requires explicit repair.
                Readiness = FollowerLogReadiness.Failed;
                throw new InvalidDataException($"Replica group '{GroupId}' metadata is missing while the log file exists; the group requires recovery or repair.");
        }

        await FollowerLogRecovery.RecoverLogFileAsync(_journal, this, cancellationToken).ConfigureAwait(false);
        _durability.Open(_journal.Paths.LogPath, _logLength);
        Readiness = FollowerLogReadiness.Ready;
    }

    private bool CheckPrevTerm(ulong prev, ulong expected)
    {
        if (prev == 0UL)
            return expected == 0UL;

        if (_journal.EntryOffsets.TryGetValue(prev, out var location))
            return location.Term == expected;

        if (_journal.SnapshotBaseline.LastIncludedIndex == prev)
            return _journal.SnapshotBaseline.LastIncludedTerm == expected;

        return false;
    }

    private ulong LastLogTerm()
    {
        if (_lastLogIndex == 0UL)
            return 0UL;
        if (_journal.EntryOffsets.TryGetValue(_lastLogIndex, out var location))
            return location.Term;
        return _journal.SnapshotBaseline.LastIncludedIndex == _lastLogIndex ? _journal.SnapshotBaseline.LastIncludedTerm : 0UL;
    }

    private void SetLastLogIndex(ulong logIndex) => _lastLogIndex = logIndex;

    private void SetLogLength(long logLength) => _logLength = logLength;

    private void SetMeta(GroupLogMetadata meta) => _meta = meta;

    private void SetReadiness(FollowerLogReadiness readiness) => Readiness = readiness;

    /// <summary>Append-protocol operations for a follower log.</summary>
    private static class FollowerLogAppend
    {
        internal static async Task<FollowerLogAppendResult?> AdvanceTermIfHigherAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            FollowerLogAppendRequest request,
            CancellationToken cancellationToken)
        {
            // Higher term is persisted durably before any further response; the old leader stops being authoritative.
            if (request.CurrentTerm > owner.Meta.CurrentTerm)
            {
                var candidate = owner.Meta with { CurrentTerm = request.CurrentTerm, VotedFor = string.Empty };
                await PersistMetaOrFailReadinessAsync(journal, owner, candidate, cancellationToken).ConfigureAwait(false);
                owner.SetMeta(candidate);
                return null;
            }

            if (request.CurrentTerm < owner.Meta.CurrentTerm)
                return new FollowerLogAppendResult(false, FollowerLogRefusal.StaleTerm, owner.Meta.CurrentTerm, owner.LastLogIndex);

            return null;
        }

        internal static async Task<FollowerLogAppendResult> AppendVerifiedBatchAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            FollowerLogAppendRequest request,
            CancellationToken cancellationToken)
        {
            // Validate the whole batch for contiguity and conflicts before writing anything.
            var entries = request.Entries;
            var lastVerifiedIndex = entries.Length == 0 ? request.PrevLogIndex : entries.Span[entries.Length - 1].LogIndex;
            if (entries.Length == 0)
                return await CompleteAppendAsync(journal, owner, request.LeaderCommitIndex, lastVerifiedIndex, false, cancellationToken).ConfigureAwait(false);

            var error = PrepareAppendBatch(journal, owner, request, out var toAppend, out var truncateAtIndex);
            if (error != null)
                return error.Value;

            // Materialize payload ownership for the append set synchronously, before the first await: once
            // the durable write is scheduled it must never reference the caller's buffer, and a cancellation
            // during truncation aborts the batch without writing any frame.
            var ownedToAppend = toAppend is { Count: > 0 } ? MaterializeOwnedEntries(toAppend) : null;

            if (truncateAtIndex != null)
                _ = await FollowerLogDurable.TruncateFromAsync(journal, owner, truncateAtIndex.Value, cancellationToken).ConfigureAwait(false);

            if (ownedToAppend != null)
                await FollowerLogDurable.AppendFramesDurableAsync(journal, owner, ownedToAppend, cancellationToken).ConfigureAwait(false);

            return await CompleteAppendAsync(journal, owner, request.LeaderCommitIndex, lastVerifiedIndex, ownedToAppend != null || truncateAtIndex != null, cancellationToken)
               .ConfigureAwait(false);
        }

        internal static async Task PersistMetaOrFailReadinessAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            GroupLogMetadata candidate,
            CancellationToken cancellationToken)
        {
            try
            {
                await FollowerLogDurable.PersistMetaAsync(journal, candidate, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Cancellation is not a publication failure; preserve readiness so the caller can retry.
                throw;
            }
            catch
            {
                owner.SetReadiness(FollowerLogReadiness.Failed);
                throw;
            }
        }

        internal static FollowerLogAppendResult? VerifyPreviousLogConsistency(FollowerLogJournal journal, IFollowerLogContext owner, FollowerLogAppendRequest request)
        {
            // Previous-log consistency; the term at an applied index was released from memory, so the check
            // covers only the retained region above the applied watermark. The term of an applied entry is read
            // back from the retained frame metadata; a leader claiming a conflicting term there violates the
            // Leader Completeness property.
            if (request.PrevLogIndex > owner.LastLogIndex)
                return new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, owner.Meta.CurrentTerm, owner.LastLogIndex);

            if (request.PrevLogIndex <= 0)
                return null;

            if (request.PrevLogIndex <= owner.Meta.LastAppliedIndex)
            {
                var term = TermAtApplied(journal, request.PrevLogIndex);
                if (term != 0UL && term == request.PrevLogTerm)
                    return null;

                // A term of zero at an applied index means the frame was compacted away and is not the snapshot
                // baseline: the prefix cannot be verified from memory. Terms start at 1, so a zero term is always
                // unverifiable and must be refused rather than accepted as consistent.
                if (term == 0UL)
                    return new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, owner.Meta.CurrentTerm, owner.LastLogIndex);

                return FailReadiness(owner);
            }

            if (TermAt(journal, request.PrevLogIndex) == request.PrevLogTerm)
                return null;

            // A term conflict at or below the committed index violates the Leader Completeness property; fail readiness.
            if (request.PrevLogIndex <= owner.Meta.CommitIndex)
                return FailReadiness(owner);

            return new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, owner.Meta.CurrentTerm, owner.LastLogIndex);

            static ulong TermAt(FollowerLogJournal journal, ulong logIndex)
            {
                return journal.Entries.TryGetValue(logIndex, out var entry) ? entry.Term : 0UL;
            }

            static ulong TermAtApplied(FollowerLogJournal journal, ulong logIndex)
            {
                if (journal.EntryOffsets.TryGetValue(logIndex, out var location))
                    return location.Term;

                // The snapshot base's frame was compacted away, but its term is retained for consistency checks.
                return journal.SnapshotBaseline.LastIncludedIndex > 0UL && logIndex == journal.SnapshotBaseline.LastIncludedIndex ? journal.SnapshotBaseline.LastIncludedTerm : 0UL;
            }
        }

        private static async Task<FollowerLogAppendResult> CompleteAppendAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            ulong leaderCommitIndex,
            ulong lastVerifiedIndex,
            bool metaDirty,
            CancellationToken cancellationToken)
        {
            var commitAdvanced = false;
            GroupLogMetadata? commitLogical = null;
            if (leaderCommitIndex > owner.Meta.CommitIndex)
            {
                var target = Math.Min(leaderCommitIndex, lastVerifiedIndex);
                if (target > owner.Meta.CommitIndex)
                {
                    commitLogical = owner.Meta with { CommitIndex = target };
                    commitAdvanced = true;
                }
            }

            if (commitAdvanced || metaDirty)
            {
                await PersistMetaOrFailReadinessAsync(journal, owner, commitLogical ?? owner.Meta, cancellationToken).ConfigureAwait(false);
                if (commitLogical is { } candidate)
                    owner.SetMeta(candidate);
            }

            if (commitAdvanced)
                owner.Faults.OnCommitAdvanced();

            return new FollowerLogAppendResult(true, string.Empty, owner.Meta.CurrentTerm, owner.LastLogIndex);
        }

        private static FollowerLogAppendResult FailReadiness(IFollowerLogContext owner)
        {
            owner.SetReadiness(FollowerLogReadiness.Failed);
            return new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, owner.Meta.CurrentTerm, owner.LastLogIndex);
        }

        private static bool IsSatisfiedByLocalState(FollowerLogJournal journal, IFollowerLogContext owner, in FollowerLogEntry candidate)
        {
            if (candidate.LogIndex <= owner.LastLogIndex && journal.TryGetEntry(candidate.LogIndex, out var existing) && existing.Term == candidate.Term &&
                existing.PayloadSpan.SequenceEqual(candidate.PayloadSpan))
                return true;

            // The term of an applied entry was released with its payload, so it is read back from the retained
            // frame metadata. Compaction removes the covered prefix from EntryOffsets, so a frame that is absent
            // there is correctly not acknowledged; a retained frame is verifiable by term alone because Leader
            // Completeness forbids a conflicting term at an applied index.
            return candidate.LogIndex <= owner.Meta.LastAppliedIndex && journal.TryGetEntryOffset(candidate.LogIndex, out var location) && location.Term == candidate.Term;
        }

        private static List<FollowerLogEntry> MaterializeOwnedEntries(List<FollowerLogEntry> source)
        {
            // Ownership is needed only for what will actually hit disk or be retained above the applied
            // watermark; duplicates satisfied by local state and refused batches are never copied.
            var owned = new List<FollowerLogEntry>(source.Count);
            for (var i = 0; i < source.Count; i++)
            {
                var entry = source[i];
                owned.Add(new FollowerLogEntry(entry.LogIndex, entry.Term, entry.Payload.ToArray()));
            }

            return owned;
        }

        private static FollowerLogAppendResult? PrepareAppendBatch(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            FollowerLogAppendRequest request,
            out List<FollowerLogEntry>? toAppend,
            out ulong? truncateAtIndex)
        {
            toAppend = null;
            truncateAtIndex = null;
            var nextExpected = request.PrevLogIndex + 1;
            var entries = request.Entries.Span;

            for (var i = 0; i < entries.Length; i++)
            {
                var entry = entries[i];
                if (entry.LogIndex != nextExpected)
                    return new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, owner.Meta.CurrentTerm, owner.LastLogIndex);

                nextExpected++;

                // Once the divergent tail is being rewritten, every subsequent entry must be re-appended durably.
                if (truncateAtIndex != null)
                {
                    toAppend!.Add(entry);
                    continue;
                }

                // Entries already satisfied by local state need no durable writing: duplicates already present with
                // identical content, and applied entries whose payloads were released after application (Leader
                // Completeness guarantees a current-term leader cannot create a conflict at an applied index).
                if (IsSatisfiedByLocalState(journal, owner, in entry))
                    continue;

                if (entry.LogIndex <= owner.Meta.CommitIndex)
                    return FailReadiness(owner);

                if (entry.LogIndex <= owner.LastLogIndex)
                    truncateAtIndex = entry.LogIndex;

                toAppend ??= [];
                toAppend.Add(entry);
            }

            return null;
        }
    }

    /// <summary>Durable write coordination for log frames and metadata.</summary>
    private static class FollowerLogDurable
    {
        internal static async Task AppendFramesDurableAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            List<FollowerLogEntry> toAppend,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (toAppend.Count == 0)
                return;

            // The file header precedes the first frame and is written exactly once
            // when the log file is still empty.
            var writeHeader = owner.LogLength == 0;

            // The whole batch is encoded into a single contiguous buffer, so the OS
            // performs one sequential writing instead of many small ones.
            var totalLength = writeHeader ? GroupLogCodec.LogFileHeader.Length : 0;
            for (var i = 0; i < toAppend.Count; i++)
                totalLength += GroupLogCodec.ComputeFrameEncodedLength(toAppend[i].Payload.Length);

            var buffer = ArrayPool<byte>.Shared.Rent(totalLength);
            var position = writeHeader ? GroupLogCodec.LogFileHeader.Length : 0;
            var startOffset = writeHeader ? 0 : owner.LogLength;

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
            var work = new AppendDurableWork(owner.Durability, buffer, startOffset, totalLength, owner.Faults);
            await WorkPool.RunAsync(work, TaskCreationOptions.None, CancellationToken.None).ConfigureAwait(false);

            // Only after the durable writing succeeds do the in-memory indexes gain the entries,
            // so a crash mid-appending can never leave the index ahead of the file.
            for (var i = 0; i < offsets.Count; i++)
                journal.AddEntry(toAppend[i], offsets[i].Value, toAppend[i].Term);

            owner.SetLastLogIndex(offsets[^1].Key);
            owner.SetLogLength(startOffset + totalLength);
            owner.SetMeta(owner.Meta with { LastLogIndex = owner.LastLogIndex });
        }

        internal static Task PersistMetaAsync(FollowerLogJournal journal, GroupLogMetadata meta, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var encodedLength = GroupLogCodec.ComputeMetaEncodedLength(meta);
            var buffer = ArrayPool<byte>.Shared.Rent(encodedLength);
            GroupLogCodec.EncodeMeta(meta, buffer.AsSpan(0, encodedLength));
            var work = new MetaDurableWork(journal.Paths.MetadataTempPath, journal.Paths.MetadataPath, buffer, encodedLength);

            // The buffer is returned only inside MetaDurableWork.Execute; a non-cancelable scheduling token
            // after the explicit check guarantees the worker always runs and the buffer is always returned.
            return WorkPool.RunAsync(work, TaskCreationOptions.None, CancellationToken.None);
        }

        internal static async Task<(int Length, List<long> Offsets)> ReplaceLogAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            List<FollowerLogEntry> tail,
            CancellationToken cancellationToken)
        {
            var work = new ReplaceDurableWork(owner.Durability, journal.Paths.LogTempPath, journal.Paths.LogPath, tail, owner.Faults);
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                await WorkPool.RunAsync(work, TaskCreationOptions.None, CancellationToken.None).ConfigureAwait(false);
            }
            catch
            {
                // Execute returns every rented buffer in its final block once it starts; return them here only
                // when the work item never ran, so the pool never sees a double Return.
                if (!work.Started)
                    work.ReturnBuffers();

                throw;
            }

            return (work.Length, work.Offsets);
        }

        internal static async Task<int> TruncateFromAsync(FollowerLogJournal journal, IFollowerLogContext owner, ulong logIndex, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!journal.EntryOffsets.TryGetValue(logIndex, out var location))
                throw new InvalidOperationException($"Replica group '{owner.GroupId}' cannot truncate from a missing index '{logIndex}'.");

            var released = 0;
            try
            {
                var durable = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                var work = new TruncateDurableWork(owner.Durability, location.Offset, owner.Faults, durable);
                try
                {
                    await WorkPool.RunAsync(work, TaskCreationOptions.None, CancellationToken.None).ConfigureAwait(false);
                }
                finally
                {
                    // OnFlushed may throw after the flush completed. The work item records that durable boundary so
                    // reservations are released whenever the truncated bytes are no longer recoverable.
                    if (durable.Task.IsCompletedSuccessfully)
                        released = owner.Idempotency.ReleaseFromIndex(logIndex);
                }
            }
            finally
            {
                // Reconcile the in-memory indexes even when the durable truncate throws (SetLength may already
                // have been applied), so the ready log never validates against a suffix that may no longer be
                // durable. The exception still propagates so the caller can retry; at worst, the in-memory state
                // trails the durable file (an uncommitted tail the leader will rewrite).
                journal.RemoveEntriesAbove(logIndex - 1UL);
                owner.SetLastLogIndex(logIndex - 1);
                owner.SetLogLength(location.Offset);
                owner.SetMeta(owner.Meta with { LastLogIndex = owner.LastLogIndex });
            }

            return released;
        }

        /// <summary>Background work that durably writes appended frames and flushes them.</summary>
        [Immutable]
        private sealed class AppendDurableWork : IWorkPoolItem
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

            void IWorkPoolItem.Execute()
            {
                try
                {
                    _durability.Write(_buffer.AsMemory(0, _length), _startOffset);
                }
                finally
                {
                    ArrayPool<byte>.Shared.ReturnCleared(_buffer);
                }

                _faults.OnFrameWritten();
                _durability.Flush();
                _faults.OnFlushed();
            }
        }

        /// <summary>Background work that writes and atomically publishes metadata.</summary>
        [Immutable]
        private sealed class MetaDurableWork : IWorkPoolItem
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

            void IWorkPoolItem.Execute()
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

                    _ = FileEx.PublishFile(_metaTempPath, _metaPath);
                }
                finally
                {
                    ArrayPool<byte>.Shared.ReturnCleared(_buffer);
                }
            }
        }

        /// <summary>Writes and flushes the header plus each retained frame incrementally before atomically publishing the result as the log.</summary>
        /// <remarks>
        /// Frames are encoded into short-lived pooled buffers one at a time, so the retained tail is never materialized into a
        /// single contiguous array (which could overflow <see cref="int" /> or exhaust contiguous memory for a large tail).
        /// </remarks>
        private sealed class ReplaceDurableWork : IWorkPoolItem
        {
            private readonly GroupLogDurability _durability;
            private readonly IFollowerLogFaultHooks _faults;
            private readonly string _finalPath;
            private readonly List<FollowerLogEntry> _tail;
            private readonly string _tempPath;
            private byte[]? _headerBuffer;

            internal ReplaceDurableWork(GroupLogDurability durability, string tempPath, string finalPath, List<FollowerLogEntry> tail, IFollowerLogFaultHooks faults)
            {
                _durability = durability;
                _tempPath = tempPath;
                _finalPath = finalPath;
                _tail = tail;
                _faults = faults;
            }

            internal int Length { get; private set; }

            internal List<long> Offsets { get; } = [];

            internal bool Started { get; private set; }

            void IWorkPoolItem.Execute()
            {
                Started = true;
                var published = false;
                try
                {
                    const FileOptions options = FileOptions.WriteThrough;
                    var headerLength = GroupLogCodec.LogFileHeader.Length;
                    _headerBuffer = ArrayPool<byte>.Shared.Rent(headerLength);
                    GroupLogCodec.LogFileHeader.CopyTo(_headerBuffer);

                    long total = headerLength;
                    for (var i = 0; i < _tail.Count; i++)
                        total += GroupLogCodec.ComputeFrameEncodedLength(_tail[i].Payload.Length);
                    if (total > int.MaxValue)
                        throw new InvalidOperationException($"Compaction tail of {total} bytes exceeds the maximum log size.");

                    var (offsets, length) = WriteTail(_headerBuffer, headerLength, total, options);
                    Offsets.AddRange(offsets);
                    Length = length;
                    _durability.Replace(_tempPath, _finalPath, length);
                    published = true;
                }
                finally
                {
                    if (!published)
                        _ = FileEx.TryDeleteFile(_tempPath);

                    ReturnBuffers();
                }
            }

            internal void ReturnBuffers()
            {
                if (_headerBuffer == null)
                    return;
                ArrayPool<byte>.Shared.ReturnCleared(_headerBuffer);
                _headerBuffer = null;
            }

            private (List<long> Offsets, int Length) WriteTail(byte[] headerBuffer, int headerLength, long total, FileOptions options)
            {
                var offsets = new List<long>(_tail.Count);
                using (var handle = File.OpenHandle(_tempPath, FileMode.Create, FileAccess.Write, FileShare.None, options))
                {
                    RandomAccess.Write(handle, headerBuffer.AsSpan(0, headerLength), 0);
                    var position = headerLength;
                    for (var i = 0; i < _tail.Count; i++)
                    {
                        var entry = _tail[i];
                        var frameLength = GroupLogCodec.ComputeFrameEncodedLength(entry.Payload.Length);
                        var frameBuffer = ArrayPool<byte>.Shared.Rent(frameLength);
                        try
                        {
                            GroupLogCodec.EncodeFrame(frameBuffer.AsSpan(0, frameLength), entry);
                            RandomAccess.Write(handle, frameBuffer.AsSpan(0, frameLength), position);
                        }
                        finally
                        {
                            // The frame bytes are durable in the open handle; release the pooled
                            // buffer immediately, so a large tail never holds every frame buffer at once.
                            ArrayPool<byte>.Shared.ReturnCleared(frameBuffer);
                        }

                        offsets.Add(position);
                        position += frameLength;
                    }

                    RandomAccess.SetLength(handle, total);
                    RandomAccess.FlushToDisk(handle);
                }

                _faults.OnFlushed();
                return (offsets, int.CreateTruncating(total));
            }
        }

        /// <summary>Background work that durably truncates the log before a conflicting tail is rewritten.</summary>
        [Immutable]
        private sealed class TruncateDurableWork : IWorkPoolItem
        {
            private readonly GroupLogDurability _durability;
            private readonly TaskCompletionSource<bool> _durable;
            private readonly IFollowerLogFaultHooks _faults;
            private readonly long _length;

            internal TruncateDurableWork(GroupLogDurability durability, long length, IFollowerLogFaultHooks faults, TaskCompletionSource<bool> durable)
            {
                _durability = durability;
                _length = length;
                _faults = faults;
                _durable = durable;
            }

            void IWorkPoolItem.Execute()
            {
                _durability.Truncate(_length);
                _durability.Flush();
                _ = _durable.TrySetResult(true);
                _faults.OnFlushed();
            }
        }
    }

    /// <summary>Startup recovery: rebuilds the in-memory log from the durable frame file.</summary>
    private static class FollowerLogRecovery
    {
        private const int FrameHeaderByteCount = 9;

        internal static void PruneAppliedEntries(FollowerLogJournal journal, IFollowerLogContext owner) => journal.ReleaseAppliedEntries(owner.Meta.LastAppliedIndex);

        internal static async Task RecoverLogFileAsync(FollowerLogJournal journal, IFollowerLogContext owner, CancellationToken cancellationToken)
        {
            // A published snapshot restores the committed baseline; the durable log then continues from its included
            // index plus one when the covered prefix was compacted away.
            var snapshotBase = await RestoreSnapshotBaseAsync(journal, owner, cancellationToken).ConfigureAwait(false);

            // A missing log file simply means the group has never persisted anything beyond the snapshot baseline.
            if (!File.Exists(journal.Paths.LogPath))
            {
                ResetAndEnsure(owner, snapshotBase);
                return;
            }

            var handle = File.OpenHandle(
                journal.Paths.LogPath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite | FileShare.Delete,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
            using (handle)
            {
                // An empty file is treated the same as no file at all.
                if (RandomAccess.GetLength(handle) == 0)
                {
                    ResetAndEnsure(owner, snapshotBase);
                    return;
                }

                // Anything preceding the expected header marks the file as unusable.
                if (RandomAccess.GetLength(handle) < GroupLogCodec.LogFileHeader.Length)
                {
                    owner.SetReadiness(FollowerLogReadiness.Failed);
                    throw new InvalidDataException($"Replica group '{owner.GroupId}' log header is corrupt.");
                }

                // A header-only file carries no frames, so only the snapshot baseline (if any) is durable.
                if (RandomAccess.GetLength(handle) == GroupLogCodec.LogFileHeader.Length)
                {
                    ResetAndEnsure(owner, snapshotBase);
                    return;
                }

                await ReadAndValidateLogFileHeaderAsync(handle, owner, cancellationToken).ConfigureAwait(false);

                journal.ClearEntries();

                // A populated log starts at index one (full history), right after the compacted snapshot
                // base, or — when a newer snapshot was published after the last compaction — inside the
                // snapshot-covered prefix. In the third shape the discarded head is backed by the published
                // snapshot, so the walk resumes from the last journaled index below the first frame. Trusting
                // the snapshot base against a full log would misread its first frame as a committed gap, so
                // the starting index is derived from the first frame.
                var firstIndex = await PeekFirstFrameIndexAsync(handle, cancellationToken).ConfigureAwait(false);
                if (firstIndex == 0 && snapshotBase > 0)
                {
                    // The first suffix frame after the validated snapshot base is torn; the entire suffix is
                    // unrecoverable. Truncate the file to the header and restart from the snapshot baseline,
                    // but only while the snapshot covers the whole committed prefix: destroying the file below
                    // a higher durable commit watermark would turn a repairable shortfall into a permanent one.
                    if (snapshotBase < owner.Meta.CommitIndex)
                    {
                        owner.SetReadiness(FollowerLogReadiness.Failed);
                        throw new InvalidDataException(
                            $"Replica group '{owner.GroupId}' first journal frame above snapshot base '{snapshotBase}' is unreadable while the commit watermark '{owner.Meta.CommitIndex}' exceeds it.");
                    }

                    await TruncateAndResetAsync(journal, owner, GroupLogCodec.LogFileHeader.Length, snapshotBase, cancellationToken).ConfigureAwait(false);
                    return;
                }

                owner.SetLastLogIndex(DeriveWalkBaseIndex(firstIndex, snapshotBase));

                var result = await WalkFramesAsync(journal, owner, handle, cancellationToken).ConfigureAwait(false);

                // A crash between snapshot publication and journal truncation leaves the old journal in place; a
                // surviving frame at the restored snapshot's included index carrying a different term means the
                // suffix above it belongs to a divergent history and is discarded, restarting from the snapshot.
                if (result.BoundaryDivergent)
                {
                    await TruncateAndResetAsync(journal, owner, GroupLogCodec.LogFileHeader.Length, snapshotBase, cancellationToken).ConfigureAwait(false);
                    return;
                }

                // A torn trailing frame is truncated back to the last valid boundary on disk,
                // since a CRC mismatch alone cannot tell an appended tail from corruption.
                if (result.Truncated && result.LastValidEnd < RandomAccess.GetLength(handle))
                    await ScheduleTruncateAsync(journal, result.LastValidEnd, cancellationToken).ConfigureAwait(false);

                owner.SetLogLength(result.LastValidEnd);
                owner.SetMeta(owner.Meta with { LastLogIndex = owner.LastLogIndex });
                PruneAppliedEntries(journal, owner);
                EnsureCommittedPrefixCovered(owner, snapshotBase);
            }
        }

        /// <summary>Fails recovery for a gap within the committed region.</summary>
        /// <param name="owner">The log being recovered.</param>
        /// <param name="lastValidEnd">The byte offset after the last valid frame.</param>
        /// <param name="nextLogIndex">The index of the missing frame.</param>
        /// <exception cref="InvalidDataException">The gap lies within the committed region.</exception>
        private static WalkResult CommittedGap(IFollowerLogContext owner, long lastValidEnd, ulong nextLogIndex)
        {
            if (nextLogIndex > owner.Meta.CommitIndex)
                return new WalkResult(lastValidEnd, true);
            owner.SetReadiness(FollowerLogReadiness.Failed);
            throw new InvalidDataException($"Replica group '{owner.GroupId}' committed log has a gap at index '{nextLogIndex}'.");
        }

        /// <summary>Derives the index the frame walk resumes from, given the first durable frame and the snapshot base.</summary>
        /// <remarks>
        ///     <para>
        ///     A populated log starts at index one (full history), right after the compacted snapshot base, or —
        ///     when a newer snapshot was published after the last compaction — inside the snapshot-covered prefix.
        ///     In the third shape the discarded head is backed by the published snapshot, so the walk resumes from
        ///     the last journaled index below the first frame.
        ///     </para>
        ///     <para>
        ///     Trusting the snapshot base against a full log would misread its first frame as a committed gap, so
        ///     the starting index is derived from the first frame instead.
        ///     </para>
        /// </remarks>
        /// <param name="firstIndex">The log index of the first durable frame, or zero when no valid frame opens the file.</param>
        /// <param name="snapshotBase">The restored snapshot baseline index.</param>
        /// <returns>The last journaled index below the first frame the walk continues from.</returns>
        private static ulong DeriveWalkBaseIndex(ulong firstIndex, ulong snapshotBase)
        {
            if (firstIndex == 1 || snapshotBase == 0)
                return 0UL;

            if (firstIndex <= snapshotBase)
                return firstIndex - 1UL;

            if (firstIndex == snapshotBase + 1)
                return snapshotBase;

            return 0UL;
        }

        /// <summary>Detects a surviving journal frame whose term diverges from the restored snapshot at its included index.</summary>
        /// <param name="journal">The paired in-memory journal state.</param>
        /// <param name="logIndex">The validated frame's log index.</param>
        /// <param name="term">The validated frame's term.</param>
        /// <returns>The divergent-boundary result, or <see langword="null" /> when the frame agrees with the snapshot.</returns>
        private static WalkResult? DivergentBoundary(FollowerLogJournal journal, ulong logIndex, ulong term)
        {
            if (journal.SnapshotBaseline.LastIncludedIndex != logIndex || journal.SnapshotBaseline.LastIncludedTerm == term)
                return null;

            return new WalkResult(GroupLogCodec.LogFileHeader.Length, true, true);
        }

        private static void EnsureCommittedPrefixCovered(IFollowerLogContext owner, ulong snapshotBase)
        {
            // Indexes at or below the restored snapshot baseline stay covered by the published snapshot even when
            // the walked journal ends below them: a crash between snapshot publication and the installation log
            // rewrite leaves the previous, shorter log in place under already-advanced watermarks. The next
            // replication round rebuilds the discarded span from the leader.
            if (owner.Meta.CommitIndex <= Math.Max(owner.LastLogIndex, snapshotBase))
                return;
            owner.SetReadiness(FollowerLogReadiness.Failed);
            throw new InvalidDataException($"Replica group '{owner.GroupId}' commit index exceeds the durable log.");
        }

        /// <summary>Reads the log index of the first frame from the fixed header boundary.</summary>
        /// <param name="handle">The open log file handle.</param>
        /// <param name="cancellationToken">A token to observe while reading.</param>
        /// <returns>The first frame's log index, or <c language="csharp">0</c> when no valid frame opens the file.</returns>
        private static async Task<ulong> PeekFirstFrameIndexAsync(SafeFileHandle handle, CancellationToken cancellationToken)
        {
            var start = GroupLogCodec.LogFileHeader.Length;
            var length = RandomAccess.GetLength(handle);
            if (length - start < FrameHeaderByteCount)
                return 0UL;

            var frameHeader = ArrayPool<byte>.Shared.Rent(FrameHeaderByteCount);
            try
            {
                var headerEnd = await HandleEx.TryReadExactAsync(handle, frameHeader.AsMemory(0, FrameHeaderByteCount), start, cancellationToken).ConfigureAwait(false);
                if (headerEnd == null || !GroupLogCodec.TryReadFrameHeaderLength(frameHeader.AsSpan(0, FrameHeaderByteCount), out var frameLength))
                    return 0UL;

                if (frameLength > length - start)
                    return 0UL;

                var frame = ArrayPool<byte>.Shared.Rent(frameLength);
                try
                {
                    frameHeader.AsSpan(0, FrameHeaderByteCount).CopyTo(frame);
                    var frameEnd = await HandleEx.TryReadExactAsync(
                        handle,
                        frame.AsMemory(FrameHeaderByteCount, frameLength - FrameHeaderByteCount),
                        start + FrameHeaderByteCount,
                        cancellationToken).ConfigureAwait(false);
                    return frameEnd != null && GroupLogCodec.TryReadFrameFields(frame.AsSpan(0, frameLength), out var logIndex, out _) ? logIndex : 0UL;
                }
                finally
                {
                    ArrayPool<byte>.Shared.ReturnCleared(frame);
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.ReturnCleared(frameHeader);
            }
        }

        /// <summary>Reads and verifies the log file header, failing recovery when it is corrupt.</summary>
        /// <param name="handle">The open log file handle.</param>
        /// <param name="owner">The log being recovered.</param>
        /// <param name="cancellationToken">A token to observe while reading.</param>
        /// <exception cref="InvalidDataException">The log file header is corrupt.</exception>
        private static async Task ReadAndValidateLogFileHeaderAsync(SafeFileHandle handle, IFollowerLogContext owner, CancellationToken cancellationToken)
        {
            var header = ArrayPool<byte>.Shared.Rent(GroupLogCodec.LogFileHeader.Length);
            try
            {
                var end = await HandleEx.TryReadExactAsync(handle, header.AsMemory(0, GroupLogCodec.LogFileHeader.Length), 0, cancellationToken).ConfigureAwait(false);
                if (end == null || !header.AsSpan(0, GroupLogCodec.LogFileHeader.Length).SequenceEqual(GroupLogCodec.LogFileHeader))
                {
                    owner.SetReadiness(FollowerLogReadiness.Failed);
                    throw new InvalidDataException($"Replica group '{owner.GroupId}' log header is corrupt.");
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.ReturnCleared(header);
            }
        }

        /// <summary>
        /// Reads the frame starting at <paramref name="frameStart" /> into scratch buffers: validates the
        /// header, grows the frame buffer when needed, and reads payload bytes at their explicit offset.
        /// A partial header/payload or an oversized declaration yields a terminal torn-tail result.
        /// </summary>
        /// <param name="owner">The log being recovered.</param>
        /// <param name="handle">The open log file handle.</param>
        /// <param name="frameHeader">Scratch buffer for the fixed-size frame header.</param>
        /// <param name="previousFrame">The previously rented frame buffer to reuse when large enough.</param>
        /// <param name="frameStart">File offset of the frame to read.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The read outcome: buffers, frame length, next log index, and a terminal result when torn.</returns>
        private static async Task<FrameReadOutcome> ReadNextFrameAsync(
            IFollowerLogContext owner,
            SafeFileHandle handle,
            byte[] frameHeader,
            byte[]? previousFrame,
            long frameStart,
            CancellationToken cancellationToken)
        {
            var nextLogIndex = owner.LastLogIndex + 1;

            // A partial header is a torn trailing frame.
            if (RandomAccess.GetLength(handle) - frameStart < FrameHeaderByteCount)
                return new FrameReadOutcome(previousFrame ?? ArrayPool<byte>.Shared.Rent(1), null, 0, nextLogIndex, UncommittedTail(owner, frameStart, nextLogIndex));

            var headerEnd = await HandleEx.TryReadExactAsync(handle, frameHeader.AsMemory(0, FrameHeaderByteCount), frameStart, cancellationToken).ConfigureAwait(false);
            if (headerEnd == null)
                return new FrameReadOutcome(previousFrame ?? ArrayPool<byte>.Shared.Rent(1), null, 0, nextLogIndex, UncommittedTail(owner, frameStart, nextLogIndex));

            // A header that fails structural validation is either a torn tail or a corrupt committed frame.
            if (!GroupLogCodec.TryReadFrameHeaderLength(frameHeader.AsSpan(0, FrameHeaderByteCount), out var frameLength))
                return new FrameReadOutcome(previousFrame ?? ArrayPool<byte>.Shared.Rent(1), null, 0, nextLogIndex, UncommittedTail(owner, frameStart, nextLogIndex));

            // A frame that ends past EOF is a torn trailing frame.
            if (RandomAccess.GetLength(handle) - (frameStart + FrameHeaderByteCount) < frameLength - FrameHeaderByteCount)
                return new FrameReadOutcome(previousFrame ?? ArrayPool<byte>.Shared.Rent(1), null, 0, nextLogIndex, UncommittedTail(owner, frameStart, nextLogIndex));

            var previous = previousFrame;
            var frame = RentFrameBuffer(previous, frameLength, out var exhausted);

            var payloadOffset = frameStart + FrameHeaderByteCount;
            frameHeader.AsSpan(0, FrameHeaderByteCount).CopyTo(frame);
            var payloadEnd = await HandleEx.TryReadExactAsync(handle, frame.AsMemory(FrameHeaderByteCount, frameLength - FrameHeaderByteCount), payloadOffset, cancellationToken)
                                           .ConfigureAwait(false);

            // The frame content is incomplete: hand ownership back to the caller's pooling path.
            if (payloadEnd == null)
                return new FrameReadOutcome(frame, exhausted, 0, nextLogIndex, UncommittedTail(owner, frameStart, nextLogIndex));

            return new FrameReadOutcome(frame, exhausted, frameLength, nextLogIndex, null);
        }

        /// <summary>
        /// Reconciles the published snapshot's term, vote, topology fingerprint, configuration generation, and
        /// watermarks with the durable metadata, persisting the repaired metadata before readiness so a replica
        /// restarted after a crash between snapshot publication and metadata advance does not load stale state.
        /// </summary>
        /// <param name="journal">The paired in-memory journal state.</param>
        /// <param name="owner">The log being recovered.</param>
        /// <param name="snapshot">The published snapshot loaded from the disk.</param>
        /// <param name="cancellationToken">A token to observe while persisting metadata.</param>
        private static async Task ReconcileSnapshotMetadataAsync(FollowerLogJournal journal, IFollowerLogContext owner, GroupSnapshot snapshot, CancellationToken cancellationToken)
        {
            var included = snapshot.LastIncludedIndex;
            var commit = Math.Max(owner.Meta.CommitIndex, Math.Min(snapshot.CommitIndex, included));
            var candidate = owner.Meta;
            var changed = false;

            if (commit > candidate.CommitIndex)
            {
                candidate = candidate with { CommitIndex = commit };
                changed = true;
            }

            // The applied watermark is owned by AdvanceAppliedAsync, not by the snapshot boundary. A snapshot carries
            // only idempotency/commit state, not the application payload, and the installation/recovery log rewrite discards
            // every frame at or below the LastIncludedIndex. Advancing LastAppliedIndex to the boundary here would skip the
            // committed-but-unapplied entries still held in the log, so their mutations would never be applied. The state
            // machine catches up by applying those retained entries through GetCommittedEntriesAsync instead.
            if (included > candidate.LastAppliedIndex && included > owner.Meta.LastLogIndex)
            {
                candidate = candidate with { LastAppliedIndex = included };
                changed = true;
            }

            // A snapshot is authoritative for the covered prefix: a higher included term advances the durable
            // term and clears any vote cast in the older term before readiness, mirroring InstallAsync.
            if (snapshot.LastIncludedTerm > candidate.CurrentTerm)
            {
                candidate = candidate with { CurrentTerm = snapshot.LastIncludedTerm, VotedFor = string.Empty };
                changed = true;
            }

            if (candidate.TopologyFingerprint.IsEmpty && !snapshot.TopologyFingerprint.IsEmpty)
            {
                candidate = candidate with { TopologyFingerprint = snapshot.TopologyFingerprint };
                changed = true;
            }

            if (snapshot.ConfigurationGeneration > candidate.ConfigurationGeneration)
            {
                candidate = candidate with { ConfigurationGeneration = snapshot.ConfigurationGeneration };
                changed = true;
            }

            if (!changed)
                return;

            await FollowerLogAppend.PersistMetaOrFailReadinessAsync(journal, owner, candidate, cancellationToken).ConfigureAwait(false);
            owner.SetMeta(candidate);
        }

        /// <summary>Records a CRC-validated frame, keeping the payload only above the applied watermark.</summary>
        /// <param name="journal">The paired in-memory journal state.</param>
        /// <param name="owner">The log being recovered.</param>
        /// <param name="nextLogIndex">The index the frame must carry.</param>
        /// <param name="lastValidEnd">The byte offset after the last valid frame.</param>
        /// <param name="frame">The complete frame at its start offset.</param>
        /// <returns>The recovery result when the frame must be truncated; otherwise <see langword="null" />.</returns>
        private static WalkResult? RecordFrame(FollowerLogJournal journal, IFollowerLogContext owner, ulong nextLogIndex, long lastValidEnd, ReadOnlySpan<byte> frame)
        {
            // Frames at or below the applied watermark need only their index and term, not their payload.
            if (nextLogIndex <= owner.Meta.LastAppliedIndex)
            {
                if (!GroupLogCodec.TryReadFrameFields(frame, out var logIndex, out var term))
                    return UncommittedTail(owner, lastValidEnd, nextLogIndex);

                if (logIndex != nextLogIndex)
                    return CommittedGap(owner, lastValidEnd, nextLogIndex);

                if (DivergentBoundary(journal, logIndex, term) is { } divergent)
                    return divergent;

                journal.AddEntryOffset(logIndex, lastValidEnd, term);
                owner.SetLastLogIndex(logIndex);
                return null;
            }

            if (!GroupLogCodec.TryReadFrame(frame, out var entry))
                return UncommittedTail(owner, lastValidEnd, nextLogIndex);

            if (entry.LogIndex != nextLogIndex)
                return CommittedGap(owner, lastValidEnd, nextLogIndex);

            if (DivergentBoundary(journal, entry.LogIndex, entry.Term) is { } divergentBoundary)
                return divergentBoundary;

            journal.AddEntry(entry, lastValidEnd, entry.Term);
            owner.SetLastLogIndex(entry.LogIndex);
            return null;
        }

        /// <summary>Rents a frame buffer of at least <paramref name="frameLength" /> bytes, reusing the current one when it fits.</summary>
        /// <param name="current">The previously rented buffer, if any.</param>
        /// <param name="frameLength">The required frame length.</param>
        /// <param name="exhausted">The returned buffer when the current one was replaced; the caller must return it to the pool.</param>
        /// <returns>A rented buffer large enough for the frame.</returns>
        private static byte[] RentFrameBuffer(byte[]? current, int frameLength, out byte[]? exhausted)
        {
            // Detach before returning so a Rent failure can never leave an already-returned
            // array referenced here for a second return in the outer finally.
            exhausted = null;
            if (current != null && current.Length >= frameLength)
                return current;

            exhausted = current;
            return ArrayPool<byte>.Shared.Rent(frameLength);
        }

        private static void ResetAndEnsure(IFollowerLogContext owner, ulong snapshotBase)
        {
            ResetLogState(owner, snapshotBase);
            EnsureCommittedPrefixCovered(owner, snapshotBase);
        }

        private static void ResetLogState(IFollowerLogContext owner, ulong snapshotBase)
        {
            owner.SetLogLength(0);
            owner.SetLastLogIndex(snapshotBase);
            owner.SetMeta(owner.Meta with { LastLogIndex = snapshotBase });
        }

        /// <summary>Restores the idempotency baseline from a compatible published snapshot and returns its included index.</summary>
        /// <param name="journal">The paired in-memory journal state.</param>
        /// <param name="owner">The log being recovered.</param>
        /// <param name="cancellationToken">A token to observe while reading.</param>
        /// <returns>The snapshot's included index, or <c language="csharp">0</c> when no snapshot is published.</returns>
        /// <exception cref="InvalidDataException">
        /// The published snapshot is corrupt, belongs to another group, or conflicts
        /// with the durable topology metadata.
        /// </exception>
        private static async Task<ulong> RestoreSnapshotBaseAsync(FollowerLogJournal journal, IFollowerLogContext owner, CancellationToken cancellationToken)
        {
            if (!journal.Snapshot.SnapshotExists)
                return 0UL;

            try
            {
                var snapshot = await journal.Snapshot.ReadPublishedAsync(cancellationToken).ConfigureAwait(false);
                if (snapshot == null)
                    return 0UL;

                if (!string.Equals(snapshot.Value.GroupId, owner.GroupId, StringComparison.Ordinal))
                {
                    owner.SetReadiness(FollowerLogReadiness.Failed);
                    throw new InvalidDataException($"Replica group '{owner.GroupId}' snapshot belongs to group '{snapshot.Value.GroupId}'.");
                }

                if (FollowerLogSnapshot.SnapshotTopologyMismatch(owner, snapshot.Value) != null)
                {
                    owner.SetReadiness(FollowerLogReadiness.Failed);
                    throw new InvalidDataException($"Replica group '{owner.GroupId}' snapshot topology or configuration generation conflicts with durable metadata.");
                }

                // A snapshot whose commit index falls below its included index is malformed: the committed prefix it
                // claims to cover is internally inconsistent, and adopting its boundary would let LastAppliedIndex
                // exceed CommitIndex. ReconcileSnapshotMetadataAsync assumes the decoder already rejected this; guard
                // here so recovery fails readiness before persisting an incoherent watermark, matching
                // ValidateInstallEligibility on the installation path.
                if (snapshot.Value.CommitIndex < snapshot.Value.LastIncludedIndex)
                {
                    owner.SetReadiness(FollowerLogReadiness.Failed);
                    throw new InvalidDataException(
                        $"Replica group '{owner.GroupId}' snapshot commit index '{snapshot.Value.CommitIndex}' is below its included index '{snapshot.Value.LastIncludedIndex}'.");
                }

                // Terms start at 1. A zero-included term collides with the unverifiable-term sentinel used by
                // TermAtApplied and would make DivergentBoundary discard the entire durable suffix.
                if (snapshot.Value is { LastIncludedIndex: > 0UL, LastIncludedTerm: 0UL })
                {
                    owner.SetReadiness(FollowerLogReadiness.Failed);
                    throw new InvalidDataException($"Replica group '{owner.GroupId}' snapshot included term is zero at index '{snapshot.Value.LastIncludedIndex}'.");
                }

                // Reconcile and durably persist the snapshot metadata BEFORE mutating in-memory recovery state. If the
                // persisting is canceled (OperationCanceledException, preserved as retryable by PersistMetaOrFailReadinessAsync)
                // or fails, the instance is left without partially restored idempotency/baseline, so a retry of OpenAsync on
                // the same instance cannot operate on inconsistent state. See F37.
                await ReconcileSnapshotMetadataAsync(journal, owner, snapshot.Value, cancellationToken).ConfigureAwait(false);
                owner.Idempotency.RestoreFromSnapshot(snapshot.Value.CommittedOutcomes);

                // Recovery rebuilds both indexes from the durable log after this point; the baseline is
                // restored without pruning because the walk owns the index lifecycle.
                owner.RestoreBaseline(new SnapshotBaseline(snapshot.Value.LastIncludedIndex, snapshot.Value.LastIncludedTerm));
                return snapshot.Value.LastIncludedIndex;
            }
            catch (InvalidDataException)
            {
                owner.SetReadiness(FollowerLogReadiness.Failed);
                throw;
            }
        }

        private static Task ScheduleTruncateAsync(FollowerLogJournal journal, long truncateLength, CancellationToken cancellationToken)
        {
            var work = new RecoveryTruncateWork(journal.Paths.LogPath, truncateLength);
            return WorkPool.RunAsync(work, TaskCreationOptions.None, cancellationToken);
        }

        private static async Task TruncateAndResetAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            long truncateLength,
            ulong snapshotBase,
            CancellationToken cancellationToken)
        {
            var work = new RecoveryTruncateWork(journal.Paths.LogPath, truncateLength);
            await WorkPool.RunAsync(work, TaskCreationOptions.None, cancellationToken).ConfigureAwait(false);
            journal.ClearEntries();
            ResetLogState(owner, snapshotBase);
            EnsureCommittedPrefixCovered(owner, snapshotBase);
        }

        /// <summary>Fails recovery for an invalid frame within the committed region.</summary>
        /// <param name="owner">The log being recovered.</param>
        /// <param name="lastValidEnd">The byte offset after the last valid frame.</param>
        /// <param name="nextLogIndex">The index of the invalid frame.</param>
        /// <exception cref="InvalidDataException">The invalid frame lies within the committed region.</exception>
        private static WalkResult UncommittedTail(IFollowerLogContext owner, long lastValidEnd, ulong nextLogIndex)
        {
            if (nextLogIndex > owner.Meta.CommitIndex)
                return new WalkResult(lastValidEnd, true);
            owner.SetReadiness(FollowerLogReadiness.Failed);
            throw new InvalidDataException($"Replica group '{owner.GroupId}' committed log frame at index '{nextLogIndex}' is corrupt.");
        }

        private static async Task<WalkResult> WalkFramesAsync(FollowerLogJournal journal, IFollowerLogContext owner, SafeFileHandle handle, CancellationToken cancellationToken)
        {
            long lastValidEnd = GroupLogCodec.LogFileHeader.Length;

            // Two pool rents for the whole walk instead of two per frame; the frame buffer grows on demand.
            var frameHeader = ArrayPool<byte>.Shared.Rent(FrameHeaderByteCount);
            byte[]? frame = null;
            try
            {
                while (lastValidEnd < RandomAccess.GetLength(handle))
                {
                    var outcome = await ReadNextFrameAsync(owner, handle, frameHeader, frame, lastValidEnd, cancellationToken).ConfigureAwait(false);
                    if (outcome.Terminal != null)
                        return outcome.Terminal.Value;

                    frame = outcome.Frame;
                    if (outcome.Exhausted != null)
                        ArrayPool<byte>.Shared.ReturnCleared(outcome.Exhausted);

                    var tail = RecordFrame(journal, owner, outcome.NextLogIndex, lastValidEnd, outcome.Frame.AsSpan(0, outcome.FrameLength));
                    if (tail != null)
                        return tail.Value;

                    lastValidEnd += outcome.FrameLength;
                }

                return new WalkResult(lastValidEnd, false);
            }
            finally
            {
                ArrayPool<byte>.Shared.ReturnCleared(frameHeader);
                if (frame != null)
                    ArrayPool<byte>.Shared.ReturnCleared(frame);
            }
        }

        /// <summary>Outcome of reading a single recovery frame into scratch buffers.</summary>
        /// <param name="Frame">The rented frame buffer holding header plus payload.</param>
        /// <param name="Exhausted">The previously rented buffer to return to the pool, when it was replaced.</param>
        /// <param name="FrameLength">Total length of the frame read; zero when torn.</param>
        /// <param name="NextLogIndex">The log index expected for this frame position.</param>
        /// <param name="Terminal">A torn-tail walk result; non-null when the walk must stop.</param>
        [Immutable]
        private readonly record struct FrameReadOutcome(byte[] Frame, byte[]? Exhausted, int FrameLength, ulong NextLogIndex, WalkResult? Terminal);

        /// <summary>Result of walking the log frames during startup recovery.</summary>
        /// <param name="LastValidEnd">The byte offset after the last valid frame.</param>
        /// <param name="Truncated">Determines whether a divergent tail was truncated.</param>
        /// <param name="BoundaryDivergent">
        /// Determines whether the surviving journal diverges from the restored
        /// snapshot at the snapshot's included index.
        /// </param>
        [Immutable]
        private readonly record struct WalkResult(long LastValidEnd, bool Truncated, bool BoundaryDivergent = false);

        /// <summary>Background work that durably truncates the log during startup recovery.</summary>
        [Immutable]
        private sealed class RecoveryTruncateWork : IWorkPoolItem
        {
            private readonly long _length;
            private readonly string _path;

            internal RecoveryTruncateWork(string path, long length)
            {
                _path = path;
                _length = length;
            }

            void IWorkPoolItem.Execute()
            {
                const FileOptions options = FileOptions.WriteThrough;
                using var handle = File.OpenHandle(_path, FileMode.Open, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete, options);
                RandomAccess.SetLength(handle, _length);
                if (!OperatingSystem.IsWindows())
                    RandomAccess.FlushToDisk(handle);
            }
        }
    }

    /// <summary>Snapshot creation, installation, and compaction for a follower log.</summary>
    private static class FollowerLogSnapshot
    {
        internal static GroupSnapshot BuildSnapshot(FollowerLogJournal journal, IFollowerLogContext owner, ulong lastIncludedIndex)
        {
            // The two adjacent ulong arguments are LastIncludedIndex and CommitIndex, in declaration order; both
            // equal the snapshot boundary for a freshly created snapshot.
            if (journal.EntryOffsets.TryGetValue(lastIncludedIndex, out var location))
            {
                return new GroupSnapshot(
                    owner.GroupId,
                    owner.Meta.TopologyFingerprint,
                    owner.Meta.ConfigurationGeneration,
                    location.Term,
                    lastIncludedIndex,
                    lastIncludedIndex,
                    ExportCoveredOutcomes(owner, lastIncludedIndex));
            }

            // The covered index may be the snapshot base itself, whose frame was already compacted away.
            if (journal.SnapshotBaseline.LastIncludedIndex != 0UL && lastIncludedIndex == journal.SnapshotBaseline.LastIncludedIndex)
            {
                return new GroupSnapshot(
                    owner.GroupId,
                    owner.Meta.TopologyFingerprint,
                    owner.Meta.ConfigurationGeneration,
                    journal.SnapshotBaseline.LastIncludedTerm,
                    lastIncludedIndex,
                    lastIncludedIndex,
                    ExportCoveredOutcomes(owner, lastIncludedIndex));
            }

            throw new InvalidOperationException($"Replica group '{owner.GroupId}' cannot snapshot from a missing index '{lastIncludedIndex}'.");
        }

        internal static async Task<GroupCompactionResult> CompactAsync(FollowerLogJournal journal, IFollowerLogContext owner, CancellationToken cancellationToken)
        {
            if (!journal.Snapshot.SnapshotExists)
                return new GroupCompactionResult(false, null, FollowerLogRefusal.NotReady);

            var snapshot = await journal.Snapshot.ReadPublishedAsync(cancellationToken).ConfigureAwait(false);
            if (snapshot == null)
                return new GroupCompactionResult(false, null, FollowerLogRefusal.NotReady);

            var included = snapshot.Value.LastIncludedIndex;

            // The durable prefix is dropped only when the published snapshot covers every committed entry; otherwise
            // committed frames would be lost without a snapshot to restore them.
            if (included == 0UL || included < owner.Meta.CommitIndex || included > owner.LastLogIndex || included > owner.Meta.LastAppliedIndex)
                return new GroupCompactionResult(false, null, FollowerLogRefusal.NotReady);

            // Frames in (included, LastAppliedIndex] were released from `Entries` by PruneAppliedEntries, so the
            // retained tail cannot reproduce them. Compacting here would drop committed frames the snapshot does
            // not cover and leave a committed gap for recovery.
            if (included < owner.Meta.LastAppliedIndex)
                return new GroupCompactionResult(false, null, FollowerLogRefusal.NotReady);

            // Validate the published snapshot's identity and topology against the durable local state before any
            // destructive rewrite. A snapshot that diverges from the replica must not be used to truncate the durable
            // prefix; the suffix must instead survive as a replicable tail.
            if (!string.Equals(snapshot.Value.GroupId, owner.GroupId, StringComparison.Ordinal))
                return new GroupCompactionResult(false, null, FollowerLogRefusal.NotMember);

            if (SnapshotTopologyMismatch(owner, snapshot.Value) is { } topologyRefusal)
                return new GroupCompactionResult(false, null, topologyRefusal);

            if (journal.EntryOffsets.TryGetValue(included, out var boundary) && boundary.Term != snapshot.Value.LastIncludedTerm)
                return new GroupCompactionResult(false, null, FollowerLogRefusal.LogMismatch);

            var tail = CollectRetainedTail(journal, included);

            var retainedLogIndexes = CollectRetainedLogIndexes(tail);

            // The capacity refusal must happen before the durable rewrite: once ReplaceLogAsync discards the covered
            // prefix, a refused restore would leave the journal without its committed frames. Fail readiness like
            // the post-rewrite refusal path below does.
            if (!owner.Idempotency.WouldRestoreFit(snapshot.Value.CommittedOutcomes, retainedLogIndexes))
            {
                owner.SetReadiness(FollowerLogReadiness.Failed);
                return new GroupCompactionResult(false, null, FollowerLogRefusal.NotReady);
            }

            (int Length, List<long> Offsets) result;
            try
            {
                result = await FollowerLogDurable.ReplaceLogAsync(journal, owner, tail, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                owner.SetReadiness(FollowerLogReadiness.Failed);
                throw;
            }

            var (length, offsets) = result;
            ReindexTail(journal, tail, offsets);

            owner.SetLastLogIndex(tail.Count == 0 ? included : tail[^1].LogIndex);
            owner.SetLogLength(length);

            // ReindexTail already left only entries above the boundary, so the paired advance prunes nothing.
            journal.AdvanceBaseline(new SnapshotBaseline(snapshot.Value.LastIncludedIndex, snapshot.Value.LastIncludedTerm));

            var candidate = owner.Meta with { LastLogIndex = Math.Max(included, owner.LastLogIndex) };
            await FollowerLogAppend.PersistMetaOrFailReadinessAsync(journal, owner, candidate, cancellationToken).ConfigureAwait(false);
            owner.SetMeta(candidate);

            // The discarded prefix is now owned by the snapshot, which exports only resolved outcomes; any remaining
            // record at or below `included` has lost its durable journal frame and must be released, while records
            // carried by the retained tail stay authoritative.
            try
            {
                if (!owner.Idempotency.TryRestoreFromSnapshot(snapshot.Value.CommittedOutcomes, retainedLogIndexes))
                {
                    // A refused restore leaves the map holding records whose journal frames were already
                    // discarded by the rewrite; the refusal path must fail readiness like the catch below.
                    owner.SetReadiness(FollowerLogReadiness.Failed);
                    return new GroupCompactionResult(false, null, FollowerLogRefusal.NotReady);
                }
            }
            catch
            {
                // A failed idempotency restore leaves the in-memory map holding discarded-prefix records whose
                // journal frames no longer exist; mark the log failed so it is never surfaced as Ready.
                owner.SetReadiness(FollowerLogReadiness.Failed);
                throw;
            }

            return new GroupCompactionResult(true, journal.Snapshot.SnapshotPath, string.Empty);
        }

        internal static async Task<GroupSnapshotInstallResult> InstallAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            GroupSnapshot snapshot,
            CancellationToken cancellationToken)
        {
            if (ValidateInstallEligibility(owner, snapshot) is { } refusal)
                return GroupSnapshotInstallResult.Refused(refusal);

            var tail = CollectInstallTail(journal, snapshot);
            var retainedLogIndexes = CollectRetainedLogIndexes(tail);

            // The capacity refusal must happen before any durable write: publishing the snapshot and persisting the
            // installation candidate ahead of a refused restore would leave a published snapshot and advanced watermarks
            // the old journal cannot support, failing recovery on every restart. Fail readiness like compaction's
            // pre-rewrite refusal path does.
            if (!owner.Idempotency.WouldRestoreFit(snapshot.CommittedOutcomes, retainedLogIndexes))
            {
                owner.SetReadiness(FollowerLogReadiness.Failed);
                return GroupSnapshotInstallResult.Refused(FollowerLogRefusal.NotReady);
            }

            var fingerprint = owner.Meta.TopologyFingerprint.IsEmpty ? snapshot.TopologyFingerprint : owner.Meta.TopologyFingerprint;
            var installedLastIndex = tail.Count == 0 ? snapshot.LastIncludedIndex : tail[^1].LogIndex;
            var candidate = BuildInstallCandidateMeta(owner, snapshot, fingerprint, installedLastIndex);

            // Publish the snapshot before advancing metadata. If publication fails, the old metadata and log remain
            // authoritative. If metadata or log rewriting fails afterward, recovery can use the new snapshot while
            // treating any old log suffix as a durable tail.
            await journal.Snapshot.PublishAsync(snapshot, cancellationToken).ConfigureAwait(false);
            await FollowerLogAppend.PersistMetaOrFailReadinessAsync(journal, owner, candidate, cancellationToken).ConfigureAwait(false);
            owner.SetMeta(candidate);

            // Restore the idempotency baseline before re-appending the retained tail so a durable fault during the
            // rewrite cannot leave the in-memory map holding entries the installation discarded while the snapshot and
            // metadata are already durable and readiness stays Ready.
            // The retained-tail rewrite below rebuilds both indexes; pruning here would mutate state the
            // rewrite owns, so the baseline is restored without its paired prune.
            owner.RestoreBaseline(new SnapshotBaseline(snapshot.LastIncludedIndex, snapshot.LastIncludedTerm));
            try
            {
                if (!owner.Idempotency.TryRestoreFromSnapshot(snapshot.CommittedOutcomes, retainedLogIndexes))
                {
                    // The snapshot and metadata are already durable and the log rewrite is skipped, so the
                    // journal no longer matches the persisted metadata. Never surface this state as Ready.
                    owner.SetReadiness(FollowerLogReadiness.Failed);
                    return GroupSnapshotInstallResult.Refused(FollowerLogRefusal.NotReady);
                }
            }
            catch
            {
                // A failed idempotency restore after the snapshot and metadata are durable would leave the
                // in-memory map holding discarded-prefix records whose journal frames no longer exist; mark
                // the log failed so it is never surfaced as Ready.
                owner.SetReadiness(FollowerLogReadiness.Failed);
                throw;
            }

            try
            {
                await ApplyInstallLogRewriteAsync(journal, owner, tail, snapshot.LastIncludedIndex, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                // A failed durable rewrite leaves the published snapshot and persisted metadata ahead of the journal.
                // Mark the log failed so it is never surfaced as Ready with inconsistent in-memory indexes; recovery
                // reconciles durable state from the snapshot on the next OpenAsync.
                owner.SetReadiness(FollowerLogReadiness.Failed);
                throw;
            }

            return GroupSnapshotInstallResult.Installed;
        }

        /// <summary>
        /// Returns the topology-mismatch refusal when the snapshot's topology fingerprint or configuration generation
        /// conflicts with the durable metadata; an empty durable fingerprint adopts the snapshot's fingerprint and never
        /// conflicts. Returns <see langword="null" /> when the snapshot is compatible.
        /// </summary>
        /// <param name="owner">The log the snapshot targets.</param>
        /// <param name="snapshot">The snapshot to validate.</param>
        /// <returns>The <see cref="FollowerLogRefusal.TopologyMismatch" /> marker, or <see langword="null" />.</returns>
        internal static string? SnapshotTopologyMismatch(IFollowerLogContext owner, GroupSnapshot snapshot)
        {
            var memory = owner.Meta.TopologyFingerprint;
            if ((!memory.IsEmpty && !memory.Span.SequenceEqual(snapshot.TopologyFingerprint.Span)) || snapshot.ConfigurationGeneration < owner.Meta.ConfigurationGeneration)
                return FollowerLogRefusal.TopologyMismatch;

            return null;
        }

        internal static void ValidateSnapshotRequest(FollowerLogJournal journal, IFollowerLogContext owner, ulong lastIncludedIndex)
        {
            if (owner.Readiness != FollowerLogReadiness.Ready)
                throw new InvalidOperationException($"Replica group '{owner.GroupId}' is not ready to create a snapshot.");

            if (lastIncludedIndex == 0UL)
                throw new InvalidOperationException($"Replica group '{owner.GroupId}' cannot snapshot at index zero.");

            if (lastIncludedIndex > owner.Meta.CommitIndex)
            {
                throw new InvalidOperationException(
                    $"Replica group '{owner.GroupId}' cannot snapshot an uncommitted index '{lastIncludedIndex}' (commit index is '{owner.Meta.CommitIndex}').");
            }

            if (lastIncludedIndex > owner.LastLogIndex)
                throw new InvalidOperationException($"Replica group '{owner.GroupId}' cannot snapshot beyond the durable last index '{owner.LastLogIndex}'.");

            if (lastIncludedIndex < journal.SnapshotBaseline.LastIncludedIndex)
            {
                throw new InvalidOperationException(
                    $"Replica group '{owner.GroupId}' cannot snapshot below the published baseline '{journal.SnapshotBaseline.LastIncludedIndex}'.");
            }
        }

        /// <summary>Atomically rewrites the durable log from the header plus the retained tail.</summary>
        /// <param name="journal">The paired in-memory journal state.</param>
        /// <remarks>
        /// The header plus the retained tail is written to a temp file and swapped into place, so a crash mid-rewrite
        /// leaves either the previous log (with its durable suffix) or the new log intact. There is no intermediate
        /// header-only state that could lose committed entries.
        /// </remarks>
        /// <param name="owner">The log being rewritten.</param>
        /// <param name="tail">The retained tail entries.</param>
        /// <param name="lastIncludedIndex">The snapshot's last included index, used when the tail is empty.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        private static async Task ApplyInstallLogRewriteAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            List<FollowerLogEntry> tail,
            ulong lastIncludedIndex,
            CancellationToken cancellationToken)
        {
            var (length, offsets) = await FollowerLogDurable.ReplaceLogAsync(journal, owner, tail, cancellationToken).ConfigureAwait(false);
            ReindexTail(journal, tail, offsets);
            owner.SetLastLogIndex(tail.Count == 0 ? lastIncludedIndex : tail[^1].LogIndex);
            owner.SetLogLength(length);
        }

        /// <summary>Builds the metadata candidate that adopts the snapshot's authoritative prefix and topology.</summary>
        /// <param name="owner">The log the snapshot targets.</param>
        /// <param name="snapshot">The snapshot being installed.</param>
        /// <param name="fingerprint">The topology fingerprint to record.</param>
        /// <param name="installedLastIndex">The last index the rewritten durable log will support: the retained tail's last entry, or the snapshot boundary when no tail is retained.</param>
        /// <returns>The next metadata value.</returns>
        private static GroupLogMetadata BuildInstallCandidateMeta(IFollowerLogContext owner, GroupSnapshot snapshot, ReadOnlyMemory<byte> fingerprint, ulong installedLastIndex)
        {
            return owner.Meta with
            {
                TopologyFingerprint = fingerprint,
                ConfigurationGeneration = Math.Max(owner.Meta.ConfigurationGeneration, snapshot.ConfigurationGeneration),
                CurrentTerm = Math.Max(owner.Meta.CurrentTerm, snapshot.LastIncludedTerm),
                VotedFor = snapshot.LastIncludedTerm > owner.Meta.CurrentTerm ? string.Empty : owner.Meta.VotedFor,

                // The persisted last index must describe the journal the installation leaves behind. When the boundary
                // term diverges the tail is empty and the rewrite produces a header-only log ending at the snapshot
                // boundary; keeping the higher pre-install value here would leave recovery reading a metadata index no
                // durable frame supports.
                LastLogIndex = installedLastIndex,
                CommitIndex = Math.Max(owner.Meta.CommitIndex, Math.Min(snapshot.CommitIndex, snapshot.LastIncludedIndex)),

                // The applied watermark follows the same rule as the recovery path. A snapshot install rewrites the
                // durable log to a header plus the retained tail above the boundary, so frames at or below the boundary
                // leave the durable log and survive only as the snapshot's applied state. When the pre-installation durable
                // log did not reach the boundary (included > LastLogIndex), no offset can exist at the boundary and the
                // collected tail is empty, so the rewritten journal ends at the boundary (installedLastIndex ==
                // included): the watermark may adopt the boundary without ever exceeding the durable journal. Otherwise,
                // it stays, letting GetCommittedEntriesAsync re-supply the retained frames through the durable tail
                // instead of skipping them.
                LastAppliedIndex = snapshot.LastIncludedIndex > owner.Meta.LastAppliedIndex && snapshot.LastIncludedIndex > owner.Meta.LastLogIndex ? snapshot.LastIncludedIndex
                    : owner.Meta.LastAppliedIndex,
            };
        }

        /// <summary>Collects the durable log entries whose index is above the snapshot boundary when the boundary matches.</summary>
        /// <param name="journal">The paired in-memory journal state.</param>
        /// <param name="snapshot">The snapshot being installed.</param>
        /// <returns>The retained tail entries, or an empty list when the boundary does not match.</returns>
        private static List<FollowerLogEntry> CollectInstallTail(FollowerLogJournal journal, GroupSnapshot snapshot)
        {
            var tail = new List<FollowerLogEntry>();
            var boundaryMatches = journal.EntryOffsets.TryGetValue(snapshot.LastIncludedIndex, out var boundary) ? boundary.Term == snapshot.LastIncludedTerm
                : journal.SnapshotBaseline.LastIncludedIndex == snapshot.LastIncludedIndex && journal.SnapshotBaseline.LastIncludedTerm == snapshot.LastIncludedTerm;
            if (!boundaryMatches)
                return tail;

            foreach (var pair in journal.Entries)
            {
                if (pair.Key > snapshot.LastIncludedIndex)
                    tail.Add(pair.Value);
            }

            return tail;
        }

        /// <summary>Extracts the journal indexes carried by the retained tail entries.</summary>
        /// <param name="tail">The retained tail entries.</param>
        /// <returns>The journal indexes of the retained tail.</returns>
        private static List<ulong> CollectRetainedLogIndexes(List<FollowerLogEntry> tail)
        {
            var retainedLogIndexes = new List<ulong>(tail.Count);
            for (var i = 0; i < tail.Count; i++)
                retainedLogIndexes.Add(tail[i].LogIndex);

            return retainedLogIndexes;
        }

        private static List<FollowerLogEntry> CollectRetainedTail(FollowerLogJournal journal, ulong boundaryIndex)
        {
            var tail = new List<FollowerLogEntry>();

            foreach (var pair in journal.Entries)
            {
                if (pair.Key > boundaryIndex)
                    tail.Add(pair.Value);
            }

            return tail;
        }

        private static List<GroupIdempotencyRecord> ExportCoveredOutcomes(IFollowerLogContext owner, ulong lastIncludedIndex)
        {
            var outcomes = new List<GroupIdempotencyRecord>();
            foreach (var record in owner.Idempotency.ExportResolved())
            {
                if (record.LogIndex <= lastIncludedIndex)
                    outcomes.Add(record);
            }

            return outcomes;
        }

        private static void ReindexTail(FollowerLogJournal journal, List<FollowerLogEntry> tail, List<long> offsets)
        {
            journal.ClearEntries();
            for (var i = 0; i < tail.Count; i++)
                journal.AddEntry(tail[i], offsets[i], tail[i].Term);
        }

        /// <summary>Returns <see langword="true" /> when any committed outcome lies beyond the snapshot boundary.</summary>
        /// <param name="outcomes">The committed outcomes carried by the snapshot.</param>
        /// <param name="lastIncludedIndex">The highest journal index covered by the snapshot.</param>
        /// <returns><see langword="true" /> when at least one outcome exceeds the boundary.</returns>
        private static bool SnapshotHasOutcomeBeyondBoundary(IReadOnlyList<GroupIdempotencyRecord> outcomes, ulong lastIncludedIndex)
        {
            for (var i = 0; i < outcomes.Count; i++)
            {
                if (outcomes[i].LogIndex > lastIncludedIndex)
                    return true;
            }

            return false;
        }

        /// <summary>Returns <see langword="true" /> when any committed outcome carried by the snapshot is still unresolved.</summary>
        /// <param name="outcomes">The committed outcomes carried by the snapshot.</param>
        /// <returns><see langword="true" /> when at least one outcome has no resolution timestamp.</returns>
        private static bool SnapshotHasUnresolvedOutcome(IReadOnlyList<GroupIdempotencyRecord> outcomes)
        {
            for (var i = 0; i < outcomes.Count; i++)
            {
                if (outcomes[i].ResolvedUtc == null)
                    return true;
            }

            return false;
        }

        /// <summary>Returns the refusal when the snapshot is not eligible for installation; otherwise <see langword="null" />.</summary>
        /// <param name="owner">The log the snapshot targets.</param>
        /// <param name="snapshot">The snapshot to validate.</param>
        /// <returns>The refusal marker, or <see langword="null" /> when the snapshot is eligible.</returns>
        private static string? ValidateInstallEligibility(IFollowerLogContext owner, GroupSnapshot snapshot)
        {
            if (!string.Equals(snapshot.GroupId, owner.GroupId, StringComparison.Ordinal))
                return FollowerLogRefusal.NotMember;

            if (SnapshotTopologyMismatch(owner, snapshot) is { } refusal)
                return refusal;

            if (snapshot.LastIncludedIndex == 0UL)
                return FollowerLogRefusal.NotReady;

            // Terms start at 1, and a zero baseline term collides with the "unverifiable term" sentinel used by
            // TermAtApplied and would make DivergentBoundary discard the whole durable suffix on the next recovery.
            if (snapshot.LastIncludedTerm == 0UL)
                return FollowerLogRefusal.NotReady;

            // The snapshot is authoritative for the covered prefix; refusing an installation below the current commit
            // watermark guarantees no committed entry is dropped without a covering snapshot.
            if (snapshot.LastIncludedIndex < owner.Meta.CommitIndex)
                return FollowerLogRefusal.NotReady;

            // Refuse an installation whose boundary sits below the replica's already-applied watermark. BuildInstallCandidateMeta
            // would otherwise set LastAppliedIndex to the included index, moving the applied watermark backward and discarding
            // committed-and-applied frames via the installation log rewrite. The monotonic-applied-index invariant that
            // AdvanceAppliedAsync enforces must hold across installation too.
            if (snapshot.LastIncludedIndex < owner.Meta.LastAppliedIndex)
                return FollowerLogRefusal.NotReady;

            // A snapshot whose commit index falls below its included index is malformed: the committed prefix it
            // claims to cover is internally inconsistent, and adopting its boundary would let LastAppliedIndex exceed
            // CommitIndex. Refuse such snapshots so the watermark invariants stay coherent.
            if (snapshot.CommitIndex < snapshot.LastIncludedIndex)
                return FollowerLogRefusal.NotReady;

            // A snapshot that carries an unresolved outcome is malformed: publishing it would write invalid idempotency
            // state to disk and poison the next recovery, which would then fail readiness. Refuse before any durable
            // write so the in-memory snapshot is rejected without a partial installation.
            if (SnapshotHasUnresolvedOutcome(snapshot.CommittedOutcomes))
                return FollowerLogRefusal.NotReady;

            // A direct GroupSnapshot input bypasses the on-disk decoder, so enforce its boundary invariant before
            // publication as well. Otherwise, recovery would later classify the published snapshot as corrupt.
            if (SnapshotHasOutcomeBeyondBoundary(snapshot.CommittedOutcomes, snapshot.LastIncludedIndex))
                return FollowerLogRefusal.NotReady;

            return null;
        }
    }

    /// <summary>No-op fault hooks used when none are supplied.</summary>
    [Immutable]
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
