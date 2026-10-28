using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Replication;

/// <summary>Durable, ordered follower log for a single replica group.</summary>
/// <remarks>
///     <para>
///     The log stores canonical entry bytes in an append-only file with per-frame CRC32C and publishes
///     per-group metadata (term, commit index, applied index, voted-for) through a two-slot file written in place and flushed.
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
    "SQR0002",
    Justification = "Each member is one gated entry point of the IFollowerLog contract; moving some to another type would take the gate with them.")]
internal sealed class FollowerLog : IFollowerLog, IFollowerLogContext
{
    private static readonly IFollowerLogFaultHooks DefaultFaults = new NoOpFaultHooks();

    private readonly FollowerLogAckRegistry _acks = new();
    private readonly GroupComposition _composition;
    private readonly GroupLogDurability _durability = new();
    private readonly IFollowerLogFaultHooks _faults;

    [SuppressMessage(
        "Reliability",
        "CA2213:Disposable fields should be disposed",
        Justification = "A drained dispose keeps _gate open so later callers get NotReady; only a dispose past its shutdown budget disposes it to fault the callers queued behind a stuck holder.")]
    private readonly AsyncLock _gate = new();

    private readonly GroupIdempotencyState _idempotency;

    private readonly FollowerLogJournal _journal;
    private readonly FollowerLogMirror _mirror;
    private readonly ILogger _log;

    /// <summary>
    /// The lifecycle bits of <see cref="FollowerLogLifecycle" />. The disposed bit is set once and never cleared; the complete bit follows the
    /// member state of the metadata. Writers hold <c language="csharp">_gate</c>; readers take no gate and read through a volatile barrier.
    /// </summary>
    private int _lifecycle;

    private ulong _commitHint;
    private ulong _lastLogIndex;

    private long _logLength;
    private GroupLogMetadata _meta;

    /// <summary>
    /// The published readiness as its raw enum value. Writers hold <c language="csharp">_gate</c>, but external
    /// pollers read <see cref="Readiness" /> without taking the gate, so every access goes through a volatile
    /// barrier and the value is never observed stale.
    /// </summary>
    private int _readiness = FollowerLogReadinessValue.Unknown;

    internal FollowerLog(string persistenceRoot, string groupId, GroupComposition composition, ILogger<FollowerLog> log, FollowerLogOptions options)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        ArgumentNullException.ThrowIfNull(composition);
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(options);
        _composition = composition;
        _faults = options.FaultHooks ?? DefaultFaults;
        _log = log;
        GroupId = groupId;
        var paths = FollowerLogPaths.Create(persistenceRoot, groupId);
        var snapshot = new GroupSnapshotStore(persistenceRoot, groupId, options.MaxSnapshotBytes, _faults);
        _journal = new FollowerLogJournal(paths, snapshot, new GroupMetadataStore(paths.MetadataPath, paths.MetadataTempPath, groupId, _faults, log));
        _mirror = new FollowerLogMirror(_journal, log, groupId, options.ShutdownBudget, options.ShutdownTimeProvider);
        _idempotency = new GroupIdempotencyState(
            options.IdempotencyCapacity,
            options.IdempotencyRetention ?? GroupIdempotencyState.DefaultRetention,
            options.TimeProvider,
            options.IdempotencyMinRetention,
            options.ResolveIdempotencyMaxOutcomeBytes());
        Idempotency = _idempotency;
    }

    internal FollowerLog(string persistenceRoot, string groupId, GroupComposition composition, ILogger<FollowerLog> log, TimeProvider timeProvider)
        : this(persistenceRoot, groupId, composition, log, new FollowerLogOptions { TimeProvider = timeProvider, ShutdownTimeProvider = TimeProvider.System })
    {
    }

    internal FollowerLog(
        string persistenceRoot,
        string groupId,
        GroupComposition composition,
        ILogger<FollowerLog> log,
        IFollowerLogFaultHooks faultHooks,
        TimeProvider timeProvider)
        : this(
            persistenceRoot,
            groupId,
            composition,
            log,
            new FollowerLogOptions { FaultHooks = faultHooks, TimeProvider = timeProvider, ShutdownTimeProvider = TimeProvider.System })
    {
    }

    /// <inheritdoc />
    FollowerLogAckRegistry IFollowerLogDurability.Acks => _acks;

    /// <inheritdoc />
    GroupLogDurability IFollowerLogDurability.Durability => _durability;

    /// <inheritdoc />
    IFollowerLogFaultHooks IFollowerLogDurability.Faults => _faults;

    public string GroupId { get; }

    /// <inheritdoc />
    GroupIdempotencyState IFollowerLog.Idempotency => _idempotency;

    /// <inheritdoc />
    GroupIdempotencyState IFollowerLogState.Idempotency => _idempotency;

    /// <inheritdoc />
    [SuppressMessage(
        "Major Code Smell",
        "S2292:Trivial properties should be auto-implemented",
        Justification = "An explicit interface implementation cannot be auto-implemented; the state stays private to the log and its gate.")]
    ulong IFollowerLogState.CommitHint
    {
        get => _commitHint;
        set => _commitHint = value;
    }

    /// <inheritdoc />
    [SuppressMessage(
        "Major Code Smell",
        "S2292:Trivial properties should be auto-implemented",
        Justification = "An explicit interface implementation cannot be auto-implemented; the state stays private to the log and its gate.")]
    ulong IFollowerLogState.LastLogIndex
    {
        get => _lastLogIndex;
        set => _lastLogIndex = value;
    }

    /// <inheritdoc />
    [SuppressMessage(
        "Major Code Smell",
        "S2292:Trivial properties should be auto-implemented",
        Justification = "An explicit interface implementation cannot be auto-implemented; the state stays private to the log and its gate.")]
    long IFollowerLogDurability.LogLength
    {
        get => _logLength;
        set => _logLength = value;
    }

    /// <inheritdoc />
    [SuppressMessage(
        "Major Code Smell",
        "S2292:Trivial properties should be auto-implemented",
        Justification = "An explicit interface implementation cannot be auto-implemented; the state stays private to the log and its gate.")]
    GroupLogMetadata IFollowerLogState.Meta
    {
        get => _meta;
        set => FollowerLogLifecycle.Assign(ref _meta, ref _lifecycle, in value);
    }

    public FollowerLogReadiness Readiness
    {
        get => FollowerLogReadinessValue.FromValue(Volatile.Read(ref _readiness));
        set => Volatile.Write(ref _readiness, FollowerLogReadinessValue.ToValue(value));
    }

    /// <inheritdoc />
    public bool IsComplete => HasLifecycle(FollowerLogLifecycle.Complete);

    /// <inheritdoc />
    public bool IsDisposed => HasLifecycle(FollowerLogLifecycle.Disposed);

    /// <summary>Gets the number of flushes of the group log file and of the metadata file this log completed.</summary>
    internal long FlushCount => _durability.FlushCount + Metadata.FlushCount;

    /// <summary>Gets the durable idempotency state of the replica group.</summary>
    internal GroupIdempotencyState Idempotency { get; }

    /// <summary>Gets the published snapshot file path, or <see langword="null" /> when none is published.</summary>
    internal string? SnapshotPath => _journal.Snapshot.SnapshotExists ? _journal.Snapshot.SnapshotPath : null;

    /// <summary>Gets the store of the durable group metadata.</summary>
    internal GroupMetadataStore Metadata => _journal.Metadata;

    /// <summary>Gets a value indicating whether the log is not disposed of and its startup validation succeeded.</summary>
    /// <remarks>Callers hold <c language="csharp">_gate</c>; a finished mirror copy that failed fails the readiness here, without waiting for an unfinished one.</remarks>
    private bool IsOpen
    {
        get
        {
            if (_mirror.TakeReadinessFailure())
                Readiness = FollowerLogReadiness.Failed;

            return !IsDisposed && Readiness == FollowerLogReadiness.Ready;
        }
    }

    /// <summary>
    /// Gets the refusal marker of a refused call: <see cref="FollowerLogRefusal.Incomplete" /> for an open log whose copy is marked incomplete,
    /// <see cref="FollowerLogRefusal.Unseeded" /> for any other open log whose copy is not complete, otherwise <see cref="FollowerLogRefusal.NotReady" />.
    /// </summary>
    /// <remarks>Callers hold <c language="csharp">_gate</c>.</remarks>
    private string RefusalCode =>
        IsOpen && _meta.MemberState != GroupMemberState.Complete ? FollowerLogBootstrap.NotCompleteRefusal(_meta.MemberState) : FollowerLogRefusal.NotReady;

    /// <summary>Gets a value indicating whether a call that mutates the log or the applied and commit watermarks is refused.</summary>
    /// <remarks>Callers hold <c language="csharp">_gate</c>. Only a complete copy of an open log takes such a call.</remarks>
    private bool Refuses => !IsOpen || _meta.MemberState != GroupMemberState.Complete;

    /// <summary>Gets a value indicating whether a pre-vote or a vote is refused.</summary>
    /// <remarks>
    /// Callers hold <c language="csharp">_gate</c>. An incomplete copy keeps its term, its vote and its log, so it votes like a complete one; an unseeded copy lost
    /// them and does not.
    /// </remarks>
    private bool RefusesBallot => !IsOpen || _meta.MemberState is not (GroupMemberState.Complete or GroupMemberState.Incomplete);

    /// <inheritdoc />
    public async Task<FollowerLogAppliedResult> AdvanceAppliedAsync(ulong appliedIndex, CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);

        return Refuses
            ? new FollowerLogAppliedResult(false, RefusalCode, _meta.LastAppliedIndex)
            : await FollowerLogAppend.AdvanceAppliedMonotonicAsync(_journal, this, appliedIndex, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<FollowerLogCommitResult> AdvanceCommitAsync(ulong commitIndex, CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);

        return Refuses
            ? new FollowerLogCommitResult(false, RefusalCode, _meta.CommitIndex)
            : await FollowerLogAppend.AdvanceCommitMonotonicAsync(_journal, this, commitIndex, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<FollowerLogAppendResult> AppendAsync(FollowerLogAppendRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request.LeaderNodeId);

        // Payload ownership is materialized lazily: the caller is blocked on this appending, so validation
        // reads its buffer directly and only entries that will actually hit disk are copied (inside
        // AppendVerifiedBatchAsync, synchronously after PrepareAppendBatch and before any await).
        AsyncLockHolder lockGuard;
        try
        {
            lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (ObjectDisposedException ex)
        {
            // The gate is disposed by a dispose that gave up on its holder: this append never ran, so nothing of it was scheduled.
            throw new ReplicaShutdownRefusedException(nameof(FollowerLog), ex);
        }

        using (lockGuard)
            return await AppendLockedAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<FollowerLogAppendResult> AppendAsync(
        FollowerLogAppendRequest request,
        ReadOnlyMemory<byte> topologyFingerprint,
        ulong configurationGeneration,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request.LeaderNodeId);

        AsyncLockHolder lockGuard;
        try
        {
            lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (ObjectDisposedException ex)
        {
            // The gate is disposed by a dispose that gave up on its holder: this append never ran, so nothing of it was scheduled.
            throw new ReplicaShutdownRefusedException(nameof(FollowerLog), ex);
        }

        using (lockGuard)
        {
            return CaptureStatus().IsTopologyMismatch(topologyFingerprint, configurationGeneration)
                ? new FollowerLogAppendResult(false, FollowerLogRefusal.TopologyMismatch, _meta.CurrentTerm, _lastLogIndex)
                : await AppendLockedAsync(request, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<GroupCompactionOutcome> CompactThroughAsync(ulong index, CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);

        return Refuses ? GroupCompactionOutcome.NotReady : await FollowerLogSnapshot.CompactThroughAsync(_journal, this, index, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if ((Interlocked.Or(ref _lifecycle, FollowerLogLifecycle.Disposed) & FollowerLogLifecycle.Disposed) != 0)
            return;

        // A durable flush cannot be canceled and may never return on a stalled disk, so the drain is bounded. On
        // expiry the in-flight waiter is faulted with ObjectDisposedException, the callers queued behind the stuck
        // holder are released by disposing the gate, and the handle, its worker thread and the holder are leaked
        // loudly: the handle is never closed under a running flush. Not throwing keeps the host disposing the rest.
        var acquired = await _mirror.AcquireDrainedGateAsync(_gate, _acks).ConfigureAwait(false);
        if (acquired is not { } lockGuard)
            return;

        using (lockGuard)
        {
            // Closing the registry makes a later durable operation fail fast instead of parking on a closed handle.
            _ = _acks.FaultAll(new ObjectDisposedException(nameof(FollowerLog)));
            _durability.Dispose();
            _journal.Metadata.Dispose();
            _ = FileEx.TryDeleteFile(_journal.Paths.MetadataTempPath);
        }
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<FollowerLogEntry>> GetCommittedEntriesAsync(CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);

        _faults.OnBeforeMemoryApply();
        return _journal.CollectCommittedEntries(_meta.CommitIndex, _meta.LastAppliedIndex);
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<FollowerLogEntry>> GetCommittedEntriesAsync(ulong afterIndex, int maxCount, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxCount);
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);

        _faults.OnBeforeMemoryApply();
        return _journal.CollectCommittedRange(afterIndex, _meta.CommitIndex, maxCount);
    }

    /// <inheritdoc />
    /// <remarks>
    /// Applied payloads are released from memory, but their frame offsets are kept, so the frames are read back from the file under the
    /// gate, which keeps truncation and compaction from moving them meanwhile.
    /// </remarks>
    public async Task<int> ReadRecentCommittedAsync(int maxCount, Func<FollowerLogEntry, bool> visit, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxCount);
        ArgumentNullException.ThrowIfNull(visit);
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        if (IsDisposed || Readiness != FollowerLogReadiness.Ready)
            throw new InvalidOperationException($"Replica group '{GroupId}' log is not ready to read its committed entries.");

        // Only the newest maxCount offsets are kept while walking the retained ones in log order.
        var newest = new Queue<(ulong LogIndex, long Offset)>(Math.Min(maxCount, _journal.EntryOffsets.Count));
        foreach (var (logIndex, location) in _journal.EntryOffsets)
        {
            if (logIndex > _meta.CommitIndex || maxCount == 0)
                break;

            if (newest.Count == maxCount)
                _ = newest.Dequeue();
            newest.Enqueue((logIndex, location.Offset));
        }

        var frames = newest.ToArray();
        Array.Reverse(frames);
        return await GroupLogFrameReader.ReadAsync(_journal.LogPath, frames, visit, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    /// <remarks>
    /// The read runs under the log gate, so no truncation or compaction moves a frame meanwhile; payloads released from memory after they
    /// were applied are read back from their retained frames, so every entry comes from disk and is checked against its frame checksum.
    /// </remarks>
    public async Task<FollowerLogEntriesRead> ReadEntriesAsync(ulong fromIndex, int maxCount, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfZero(fromIndex);
        ArgumentOutOfRangeException.ThrowIfNegative(maxCount);
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        return IsDisposed || Readiness != FollowerLogReadiness.Ready
            ? throw new InvalidOperationException($"Replica group '{GroupId}' log is not ready to read its entries.")
            : await FollowerLogRead.ReadRangeAsync(_journal, this, fromIndex, maxCount, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<FollowerLogTail> GetLeaderTailAsync(CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);

        // One gate acquisition: a commit advancing between the status and the tail would pair the status with a shorter tail.
        var status = CaptureStatus();
        return status.LastLogIndex == status.CommitIndex
            ? new FollowerLogTail(status, 0UL, [])
            : new FollowerLogTail(status, TermAt(status.CommitIndex), _journal.CollectUncommittedTail(status.CommitIndex));
    }

    /// <inheritdoc />
    async ValueTask<FollowerLogRetention> IFollowerLog.GetRetentionAsync(CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        return new FollowerLogRetention(_logLength, _journal.EntryOffsets.Count, _journal.Entries.Count, _journal.SnapshotBaseline.LastIncludedIndex);
    }

    /// <inheritdoc />
    public async ValueTask<FollowerLogStatus> GetStatusAsync(CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        return CaptureStatus();
    }

    /// <inheritdoc />
    public async ValueTask<ulong> GetTermAtAsync(ulong logIndex, CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        return TermAt(logIndex);
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<FollowerLogEntry>> GetUncommittedTailAsync(CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        return _journal.CollectUncommittedTail(_meta.CommitIndex);
    }

    /// <inheritdoc />
    public async Task<string?> BeginStateInstallAsync(
        StateInstallLeader leader,
        ulong index,
        ReadOnlyMemory<byte> fingerprint,
        ulong generation,
        GroupSnapshot snapshot,
        CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        return await FollowerLogStateInstall.BeginAsync(
            _journal,
            this,
            IsOpen,
            CaptureStatus().IsTopologyMismatch(fingerprint, generation),
            (leader, index, snapshot),
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<GroupSnapshotCapture> CaptureSnapshotAsync(CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        return FollowerLogStateInstall.Capture(_journal, this, IsOpen);
    }

    /// <inheritdoc />
    public async Task<GroupSnapshotInstallResult> CompleteStateInstallAsync(GroupSnapshot snapshot, StateInstallLeader leader, CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        return await FollowerLogStateInstall.CompleteAsync(_journal, this, IsOpen, snapshot, leader, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Installs the snapshot baseline without pruning the retained indexes. Reserved for paths whose index
    /// lifecycle is owned separately: snapshot publication retains the covered prefix until the applied watermark
    /// releases it, while recovery and installation rebuild both indexes from the retained tail afterward.
    /// </summary>
    /// <param name="baseline">The restored snapshot baseline.</param>
    void IFollowerLogContext.RestoreBaseline(SnapshotBaseline baseline) => _journal.RestoreBaseline(baseline);

    /// <summary>Latches the log failed after an append whose frames may have reached the disk, and logs it.</summary>
    /// <param name="cause">The failure of the write, the flush, or the index update.</param>
    void IFollowerLogContext.LatchAppendUnconfirmed(Exception cause)
    {
        Readiness = FollowerLogReadiness.Failed;
        ServerLog.FollowerLogAppendUnconfirmed(_log, cause, GroupId);
    }

    void IFollowerLogContext.LatchTruncateUnconfirmed(Exception cause)
    {
        Readiness = FollowerLogReadiness.Failed;
        ServerLog.FollowerLogTruncateUnconfirmed(_log, cause, GroupId);
    }

    /// <inheritdoc />
    public async Task<FollowerLogVoteResult> CheckPreVoteAsync(ElectionVoteRequest request, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(request.CandidateId);
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);

        // Term one belongs to the static provisional leader, so no election may run for it or below.
        return (RefusesBallot, request.Term <= 1UL) switch
        {
            (true, _) => new FollowerLogVoteResult(false, RefusalCode, _meta.CurrentTerm),
            (false, true) => new FollowerLogVoteResult(false, FollowerLogRefusal.StaleTerm, _meta.CurrentTerm),
            (false, false) => FollowerLogElection.CheckPreVote(_journal, this, in request),
        };
    }

    /// <inheritdoc />
    public async Task<FollowerLogVoteResult> RequestVoteAsync(ElectionVoteRequest request, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(request.CandidateId);
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);

        // Term one belongs to the static provisional leader: a request for it or below can never win and must not persist a vote.
        return (RefusesBallot, request.Term <= 1UL) switch
        {
            (true, _) => new FollowerLogVoteResult(false, RefusalCode, _meta.CurrentTerm),
            (false, true) => new FollowerLogVoteResult(false, FollowerLogRefusal.StaleTerm, _meta.CurrentTerm),
            (false, false) => await FollowerLogElection.RequestVoteAsync(_journal, this, request, cancellationToken).ConfigureAwait(false),
        };
    }

    /// <inheritdoc />
    public async Task<string?> AcceptBootstrapOfferAsync(ReadOnlyMemory<byte> topologyFingerprint, ulong configurationGeneration, CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        return await FollowerLogBootstrap.AcceptOfferAsync(
            _journal,
            this,
            IsOpen,
            CaptureStatus().IsTopologyMismatch(topologyFingerprint, configurationGeneration),
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<string?> CompleteIfOfferedAsync(ReadOnlyMemory<byte> topologyFingerprint, ulong configurationGeneration, CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        return await FollowerLogBootstrap.CompleteIfOfferedAsync(
            _journal,
            this,
            IsOpen,
            CaptureStatus().IsTopologyMismatch(topologyFingerprint, configurationGeneration),
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task CompleteBootstrapAsync(CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        await FollowerLogBootstrap.CompleteOwnerAsync(_journal, this, IsOpen, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<ulong> ObserveTermAsync(ulong term, CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        if (Refuses || term <= _meta.CurrentTerm)
            return _meta.CurrentTerm;

        // The higher term is durable before anyone acts on it; any vote of an older term is cleared with it.
        var observed = _meta with { CurrentTerm = term, VotedFor = string.Empty };
        await FollowerLogAppend.PersistMirroredMetaOrFailReadinessAsync(_journal, this, observed, cancellationToken).ConfigureAwait(false);
        FollowerLogLifecycle.Assign(ref _meta, ref _lifecycle, in observed);
        return term;
    }

    /// <summary>Opens the group log, running startup validation and recovering only the committed prefix.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the group log is open and ready.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the group is not part of the local static composition. No storage directory is created.</exception>
    /// <exception cref="InvalidDataException">Thrown when metadata or a committed log frame is corrupt; readiness is set to <see cref="FollowerLogReadiness.Failed" />.</exception>
    internal async Task OpenAsync(CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);

        if (!_composition.Contains(GroupId))
            throw new InvalidOperationException($"Group '{GroupId}' is not part of the local static composition.");

        _ = await DirectoryEx.CreateDirectoryAsync(_journal.Paths.GroupDirectory, cancellationToken: cancellationToken).ConfigureAwait(false);
        _ = FileEx.TryDeleteFile(_journal.Paths.MetadataTempPath);

        await FollowerLogStartup.OpenGroupAsync(_journal, this, cancellationToken).ConfigureAwait(false);
        if (_meta.MemberState != GroupMemberState.Complete)
            ServerLog.FollowerLogOpenedNotComplete(_log, GroupId, _meta.MemberState == GroupMemberState.Unseeded ? "unseeded" : "incomplete");
    }

    /// <summary>Records the configured topology in metadata that carries none yet, and refuses a log written for another topology.</summary>
    /// <param name="fingerprint">The configured topology fingerprint.</param>
    /// <param name="generation">The configured configuration generation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the metadata records the configured topology.</returns>
    /// <exception cref="InvalidDataException">The log was written for a different topology fingerprint or a newer configuration generation.</exception>
    /// <remarks>
    /// Without a recorded topology every topology check compares against nothing until the first snapshot, so a follower would accept
    /// appends from a leader of another topology and a restart would apply entries for keys this node may no longer own.
    /// </remarks>
    internal async Task AdoptTopologyAsync(ReadOnlyMemory<byte> fingerprint, ulong generation, CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        ObjectDisposedException.ThrowIf(IsDisposed, this);

        if (CaptureStatus().IsTopologyMismatch(fingerprint, generation))
        {
            throw new InvalidDataException(
                $"Replica group '{GroupId}' log was written for a different topology (stored fingerprint {Convert.ToHexString(_meta.TopologyFingerprint.Span)} " +
                $"generation {_meta.ConfigurationGeneration.ToString(CultureInfo.InvariantCulture)}, configured fingerprint {Convert.ToHexString(fingerprint.Span)} " +
                $"generation {generation.ToString(CultureInfo.InvariantCulture)}); applying it could write keys this node no longer owns. " +
                "Run 'squirix-server doctor' to inspect the data directory.");
        }

        if (!_meta.TopologyFingerprint.IsEmpty)
            return;

        byte[] owned = [.. fingerprint.Span];
        var candidate = _meta with { TopologyFingerprint = owned, ConfigurationGeneration = generation };
        await StoreMetaAsync(candidate, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Appends a batch after the readiness, term, and consistency checks.</summary>
    /// <param name="request">The appending request.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The outcome of the appending attempt.</returns>
    /// <exception cref="ReplicaShutdownRefusedException">The shutdown refused or cut the term write, so no frame was scheduled.</exception>
    /// <remarks>Callers hold <c language="csharp">_gate</c>.</remarks>
    private async Task<FollowerLogAppendResult> AppendLockedAsync(FollowerLogAppendRequest request, CancellationToken cancellationToken)
    {
        if (Refuses)
            return new FollowerLogAppendResult(false, RefusalCode, _meta.CurrentTerm, _lastLogIndex);

        FollowerLogAppendResult? termError;
        try
        {
            termError = await FollowerLogAppend.AdvanceTermIfHigherAsync(_journal, this, request, cancellationToken).ConfigureAwait(false);
        }
        catch (ObjectDisposedException ex)
        {
            // The term write was refused or cut by the shutdown, and the frames of the entries are scheduled only after it succeeds.
            throw new ReplicaShutdownRefusedException(nameof(FollowerLog), ex);
        }

        if (termError != null)
            return termError.Value;

        var consistencyError = FollowerLogAppend.VerifyPreviousLogConsistency(_journal, this, in request);
        return consistencyError ?? await FollowerLogAppend.AppendVerifiedBatchAsync(_journal, this, request, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Persists new metadata durably and only then makes it the metadata of the log.</summary>
    /// <param name="candidate">The new metadata.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes once the metadata is durable and current.</returns>
    /// <remarks>Runs under the log gate. A failed write leaves the metadata as it was; it fails the readiness unless it was canceled or faulted by a dispose.</remarks>
    private async Task StoreMetaAsync(GroupLogMetadata candidate, CancellationToken cancellationToken)
    {
        await FollowerLogAppend.PersistMetaOrFailReadinessAsync(_journal, this, candidate, cancellationToken).ConfigureAwait(false);
        FollowerLogLifecycle.Assign(ref _meta, ref _lifecycle, in candidate);
    }

    /// <summary>Reads one lifecycle bit through a volatile barrier, without <c language="csharp">_gate</c>.</summary>
    /// <param name="bit">The bit of <see cref="FollowerLogLifecycle" /> to read.</param>
    /// <returns><see langword="true" /> when the bit is set.</returns>
    private bool HasLifecycle(int bit) => (Volatile.Read(ref _lifecycle) & bit) != 0;

    /// <summary>Builds the status of the durable log state.</summary>
    /// <returns>The durable log status.</returns>
    /// <remarks>Callers hold <c language="csharp">_gate</c>.</remarks>
    private FollowerLogStatus CaptureStatus()
    {
        var lastLogTerm = 0UL;
        if (_lastLogIndex != 0UL && _journal.EntryOffsets.TryGetValue(_lastLogIndex, out var location))
            lastLogTerm = location.Term;
        else if (_lastLogIndex != 0UL && _journal.SnapshotBaseline.LastIncludedIndex == _lastLogIndex)
            lastLogTerm = _journal.SnapshotBaseline.LastIncludedTerm;

        return new FollowerLogStatus(
            GroupId,
            _meta.TopologyFingerprint,
            _meta.ConfigurationGeneration,
            _meta.CurrentTerm,
            _meta.VotedFor,
            _lastLogIndex,
            lastLogTerm,
            _meta.CommitIndex,
            _meta.LastAppliedIndex,
            Readiness,
            _meta.MemberState)
        {
            CommitHint = _commitHint,
        };
    }

    /// <summary>Returns the term the log retains for an index.</summary>
    /// <param name="logIndex">The log index; zero is the log origin.</param>
    /// <returns>The term of the retained entry or snapshot baseline at <paramref name="logIndex" />; zero at the log origin.</returns>
    /// <exception cref="InvalidDataException">The log retains no term for a non-zero <paramref name="logIndex" />.</exception>
    /// <remarks>Callers hold <c language="csharp">_gate</c>.</remarks>
    private ulong TermAt(ulong logIndex) => logIndex switch
    {
        0UL => 0UL,
        _ when _journal.TryGetEntryOffset(logIndex, out var location) => location.Term,
        _ when _journal.SnapshotBaseline.LastIncludedIndex == logIndex => _journal.SnapshotBaseline.LastIncludedTerm,
        _ => throw new InvalidDataException($"Replica group '{GroupId}' retains no term for log index '{logIndex}'."),
    };

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

        public void OnFramesScheduling()
        {
        }

        public void OnMetaPublishing()
        {
        }

        public void OnSnapshotPublishing()
        {
        }
    }
}
