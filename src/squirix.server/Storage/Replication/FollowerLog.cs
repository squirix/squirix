using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Win32.SafeHandles;
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
    private readonly ILogger _log;
    private readonly TimeSpan _shutdownBudget;
    private readonly TimeProvider _shutdownClock;

    /// <summary>
    /// The lifecycle bits of <see cref="FollowerLogLifecycle" />. The disposed bit is set once and never cleared; the complete bit follows the
    /// member state of the metadata. Writers hold <c language="csharp">_gate</c>; readers take no gate and read through a volatile barrier.
    /// </summary>
    private int _lifecycle;

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
        _shutdownBudget = options.ShutdownBudget;
        _shutdownClock = options.ShutdownTimeProvider;
        GroupId = groupId;
        var paths = FollowerLogPaths.Create(persistenceRoot, groupId);
        var snapshot = new GroupSnapshotStore(persistenceRoot, groupId, options.MaxSnapshotBytes, _faults);
        _journal = new FollowerLogJournal(paths, snapshot, new GroupMetadataStore(paths.MetadataPath, paths.MetadataTempPath, groupId, _faults, log));
        _idempotency = new GroupIdempotencyState(
            options.IdempotencyCapacity,
            options.IdempotencyRetention ?? GroupIdempotencyState.DefaultRetention,
            options.TimeProvider,
            options.IdempotencyMinRetention);
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
            FailReadinessOnFailedMirror();
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
        AsyncLockHolder lockGuard;
        var drainHolder = default(AsyncLockHolder);
        using (var budget = new CancellationTokenSource(_shutdownBudget, _shutdownClock))
        {
            try
            {
                lockGuard = await _gate.LockAsync(budget.Token).ConfigureAwait(false);

                // The mirror copy of a deferred publish runs after the gate is released, so it is waited for under the gate, which no new
                // publish can pass meanwhile. Its own failure is not a dispose failure: the first copy it follows is already the durable record.
                try
                {
                    await DrainPendingMirrorAsync(budget.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // The holder is released only after the gate is disposed, or the gate would hand itself to a queued caller.
                    drainHolder = lockGuard;
                    throw;
                }
            }
            catch (OperationCanceledException)
            {
                // The gate goes first: faulting the in-flight waiter lets its append release the gate, which would hand it to a queued
                // caller that then completes with a refusal instead of ObjectDisposedException.
                _gate.Dispose();
                drainHolder.Dispose();
                var faulted = _acks.FaultAll(new ObjectDisposedException(nameof(FollowerLog)));
                ServerLog.FollowerLogLeakedOnShutdownTimeout(_log, GroupId, _shutdownBudget, faulted);
                return;
            }
        }

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

    internal async Task<FollowerLogReconcileResult> ReconcileTailAsync(ulong fromIndex, ulong prevLogTerm, ulong leaderTerm, CancellationToken cancellationToken)
    {
        using var lockGuard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);

        return Refuses
            ? new FollowerLogReconcileResult(false, RefusalCode, 0, Readiness == FollowerLogReadiness.Failed)
            : await FollowerLogReconcile.ReconcileTailAsync(_journal, this, fromIndex, prevLogTerm, leaderTerm, cancellationToken).ConfigureAwait(false);
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

    /// <summary>Waits for the pending mirror copy to finish, bounded by <paramref name="cancellationToken" />, and logs its failure.</summary>
    /// <param name="cancellationToken">Cancellation token that bounds the wait.</param>
    /// <returns>A task that completes once no mirror copy is pending.</returns>
    /// <exception cref="OperationCanceledException">The token was canceled before the mirror copy finished.</exception>
    /// <remarks>Callers hold <c language="csharp">_gate</c>.</remarks>
    [SuppressMessage("Usage", "VSTHRD003", Justification = "The pending mirror is the ack of a work item this log scheduled; the work item or the shutdown drain completes it.")]
    private async Task DrainPendingMirrorAsync(CancellationToken cancellationToken)
    {
        if (_journal.Metadata.PendingMirror is not { } pending)
            return;

        var finished = await Task.WhenAny(pending).WaitAsync(cancellationToken).ConfigureAwait(false);

        _journal.Metadata.PendingMirror = null;
        if (finished.Exception is { } failure)
            ServerLog.FollowerLogMirrorFailedOnDispose(_log, failure.GetBaseException(), GroupId);
    }

    /// <summary>Fails the readiness when the pending mirror copy has finished with a failure that says something about the storage.</summary>
    /// <remarks>Callers hold <c language="csharp">_gate</c>. An unfinished mirror copy is left pending; the next publish or the dispose waits for it.</remarks>
    private void FailReadinessOnFailedMirror()
    {
        if (_journal.Metadata.PendingMirror is not { IsCompleted: true } pending)
            return;

        _journal.Metadata.PendingMirror = null;
        if (pending.Exception is not { } failure || !FollowerLogAppend.FailsReadiness(failure.GetBaseException()))
            return;

        Readiness = FollowerLogReadiness.Failed;
        ServerLog.FollowerLogMirrorFailed(_log, failure.GetBaseException(), GroupId);
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
            _meta.MemberState);
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

    /// <summary>Maps <see cref="FollowerLogReadiness" /> to and from the raw value a follower log publishes through a volatile barrier.</summary>
    private static class FollowerLogReadinessValue
    {
        /// <summary>The raw value of <see cref="FollowerLogReadiness.Unknown" />, which a log starts with before startup validation.</summary>
        internal const int Unknown = 0;

        private const int FailedValue = 2;
        private const int ReadyValue = 1;

        /// <summary>Converts a raw published value to the readiness it stands for.</summary>
        /// <param name="value">The raw value.</param>
        /// <returns>The readiness; <see cref="FollowerLogReadiness.Unknown" /> for a value that stands for none.</returns>
        internal static FollowerLogReadiness FromValue(int value)
        {
            return value switch
            {
                ReadyValue => FollowerLogReadiness.Ready,
                FailedValue => FollowerLogReadiness.Failed,
                _ => FollowerLogReadiness.Unknown,
            };
        }

        /// <summary>Converts a readiness to the raw value published for it.</summary>
        /// <param name="readiness">The readiness.</param>
        /// <returns>The raw value.</returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="readiness" /> is not a defined readiness.</exception>
        internal static int ToValue(FollowerLogReadiness readiness)
        {
            return readiness switch
            {
                FollowerLogReadiness.Unknown => Unknown,
                FollowerLogReadiness.Ready => ReadyValue,
                FollowerLogReadiness.Failed => FailedValue,
                _ => throw new ArgumentOutOfRangeException(nameof(readiness), readiness, "Unsupported enum value."),
            };
        }
    }

    /// <summary>Append-protocol operations for a follower log.</summary>
    private static class FollowerLogAppend
    {
        internal static async Task<FollowerLogAppliedResult> AdvanceAppliedMonotonicAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            ulong appliedIndex,
            CancellationToken cancellationToken)
        {
            // Applied index moves only monotonically.
            if (appliedIndex <= owner.Meta.LastAppliedIndex)
                return new FollowerLogAppliedResult(true, string.Empty, owner.Meta.LastAppliedIndex);

            // Never applied beyond the committed index.
            if (appliedIndex > owner.Meta.CommitIndex)
                return new FollowerLogAppliedResult(false, FollowerLogRefusal.NotReady, owner.Meta.LastAppliedIndex);

            // The watermark is persisted before the payloads are released; on a crash between the two, restart
            // reloads the frames, but the durable watermark still suppresses re-application of the applied prefix.
            var candidate = owner.Meta with { LastAppliedIndex = appliedIndex };
            await PersistMetaOrFailReadinessAsync(journal, owner, candidate, cancellationToken).ConfigureAwait(false);
            owner.Meta = candidate;
            journal.ReleaseAppliedEntries(owner.Meta.LastAppliedIndex);
            return new FollowerLogAppliedResult(true, string.Empty, appliedIndex);
        }

        internal static async Task<FollowerLogCommitResult> AdvanceCommitMonotonicAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            ulong commitIndex,
            CancellationToken cancellationToken)
        {
            // Commit index moves only monotonically.
            if (commitIndex <= owner.Meta.CommitIndex)
                return new FollowerLogCommitResult(true, string.Empty, owner.Meta.CommitIndex);

            // Never beyond the locally durable last index.
            if (commitIndex > owner.LastLogIndex)
                return new FollowerLogCommitResult(false, FollowerLogRefusal.NotReady, owner.Meta.CommitIndex);

            var candidate = owner.Meta with { CommitIndex = commitIndex };
            await PersistMetaOrFailReadinessAsync(journal, owner, candidate, cancellationToken).ConfigureAwait(false);
            owner.Meta = candidate;
            owner.Faults.OnCommitAdvanced();
            return new FollowerLogCommitResult(true, string.Empty, commitIndex);
        }

        internal static async Task<FollowerLogAppendResult?> AdvanceTermIfHigherAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            FollowerLogAppendRequest request,
            CancellationToken cancellationToken)
        {
            // Higher term is persisted durably before any further response; the old leader stops being authoritative.
            var term = owner.Meta.CurrentTerm;
            if (request.CurrentTerm <= term)
                return request.CurrentTerm < term ? new FollowerLogAppendResult(false, FollowerLogRefusal.StaleTerm, term, owner.LastLogIndex) : null;

            var candidate = owner.Meta with { CurrentTerm = request.CurrentTerm, VotedFor = string.Empty };
            await PersistMirroredMetaOrFailReadinessAsync(journal, owner, candidate, cancellationToken).ConfigureAwait(false);
            owner.Meta = candidate;
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
                return await CompleteAppendAsync(journal, owner, request.LeaderCommitIndex, lastVerifiedIndex, cancellationToken).ConfigureAwait(false);

            var error = PrepareAppendBatch(journal, owner, in request, out var toAppend, out var truncateAtIndex);
            if (error != null)
                return error.Value;

            // Materialize payload ownership for the appending set synchronously, before the first awaits: once
            // the durable writing is scheduled, it must never reference the caller's buffer, and a cancellation
            // during truncation aborts the batch without writing any frame.
            var ownedToAppend = toAppend is { Count: > 0 } ? MaterializeOwnedEntries(toAppend) : null;

            if (truncateAtIndex != null)
            {
                try
                {
                    _ = await FollowerLogDurable.TruncateFromAsync(journal, owner, truncateAtIndex.Value, cancellationToken).ConfigureAwait(false);
                }
                catch (ObjectDisposedException ex)
                {
                    // The truncate was refused or cut by the shutdown, and the frames of the entries are scheduled only after it succeeds.
                    throw new ReplicaShutdownRefusedException(nameof(FollowerLog), ex);
                }
            }

            if (ownedToAppend != null)
                await FollowerLogDurable.AppendFramesDurableAsync(journal, owner, ownedToAppend, cancellationToken).ConfigureAwait(false);

            // Durable frames are part of the log whatever happens next: a restart recovers them. Cancelling the metadata write behind
            // them would report an append that already happened as refused, so it runs to completion.
            if (ownedToAppend == null)
                return await CompleteAppendAsync(journal, owner, request.LeaderCommitIndex, lastVerifiedIndex, cancellationToken).ConfigureAwait(false);

            try
            {
                return await CompleteAppendAsync(journal, owner, request.LeaderCommitIndex, lastVerifiedIndex, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // The frames are in the log file, so the failure of what follows them cannot be reported as a refusal. That includes a
                // shutdown that faulted the metadata write: its flush may still land through the leaked handle.
                owner.LatchAppendUnconfirmed(ex);
                throw new GroupLogAppendUnconfirmedException(ex);
            }
        }

        internal static bool IsLogUpToDate(ulong candidateLastTerm, ulong candidateLastIndex, ulong localLastTerm, ulong localLastIndex) =>
            candidateLastTerm != localLastTerm ? candidateLastTerm > localLastTerm : candidateLastIndex >= localLastIndex;

        internal static async Task PersistMetaOrFailReadinessAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            GroupLogMetadata candidate,
            CancellationToken cancellationToken)
        {
            try
            {
                await AwaitPendingMirrorAsync(journal, cancellationToken).ConfigureAwait(false);
                await FollowerLogDurable.PersistMetaAsync(journal, owner, in candidate, false, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (FailsReadiness(ex))
            {
                owner.Readiness = FollowerLogReadiness.Failed;
                throw;
            }
        }

        /// <summary>
        /// Persists a vote grant, a term step, or a member state change into both metadata slots, failing readiness like
        /// <see cref="PersistMetaOrFailReadinessAsync" />.
        /// </summary>
        /// <param name="journal">The storage surface.</param>
        /// <param name="owner">The owning follower log.</param>
        /// <param name="candidate">The metadata to store in both slots.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>A task that completes once both copies are durable.</returns>
        internal static async Task PersistMirroredMetaOrFailReadinessAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            GroupLogMetadata candidate,
            CancellationToken cancellationToken)
        {
            try
            {
                await AwaitPendingMirrorAsync(journal, cancellationToken).ConfigureAwait(false);
                await FollowerLogDurable.PersistMetaAsync(journal, owner, in candidate, true, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (FailsReadiness(ex))
            {
                owner.Readiness = FollowerLogReadiness.Failed;
                throw;
            }
        }

        /// <summary>
        /// Persists a vote grant or a term step into both metadata slots, answering after the first copy is durable: the mirror copy is written
        /// right after by the same work item and is left pending on the owner. Fails readiness like <see cref="PersistMetaOrFailReadinessAsync" />.
        /// </summary>
        /// <param name="journal">The storage surface.</param>
        /// <param name="owner">The owning follower log.</param>
        /// <param name="candidate">The metadata to store in both slots.</param>
        /// <param name="cancellationToken">Cancellation token; it bounds the wait for an earlier mirror copy and the scheduling, not the write.</param>
        /// <returns>A task that completes once the first copy is durable; the mirror copy may still be pending.</returns>
        /// <remarks>
        /// A failed first copy writes no mirror copy and is reported here. A failed mirror copy is reported by the next metadata publish, or by the
        /// next gate entry that finds it finished; the first copy stays the durable record.
        /// </remarks>
        internal static async Task PersistDeferredMirrorOrFailAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            GroupLogMetadata candidate,
            CancellationToken cancellationToken)
        {
            try
            {
                await AwaitPendingMirrorAsync(journal, cancellationToken).ConfigureAwait(false);
                var (firstCopy, mirror) = FollowerLogDurable.PersistMetaDeferredMirror(journal, owner, in candidate, cancellationToken);
                try
                {
                    await firstCopy.ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // The mirror ack of a failed first copy is faulted with the same failure and has no other reader.
                    _ = mirror.ContinueWith(
                        static finished => finished.Exception,
                        CancellationToken.None,
                        TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                        TaskScheduler.Default);
                    throw;
                }

                journal.Metadata.PendingMirror = mirror;
            }
            catch (Exception ex) when (FailsReadiness(ex))
            {
                owner.Readiness = FollowerLogReadiness.Failed;
                throw;
            }
        }

        /// <summary>Tells whether a failed metadata publish fails readiness.</summary>
        /// <param name="exception">The failure.</param>
        /// <returns><see langword="false" /> for cancellation and a dispose, which say nothing about the storage: cancellation lets the caller retry, and a write faulted by a dispose may still land and is reconciled by restart recovery.</returns>
        internal static bool FailsReadiness(Exception exception) => exception is not (OperationCanceledException or ObjectDisposedException);

        internal static bool PrevTermMatches(FollowerLogJournal journal, ulong prev, ulong expected)
        {
            return prev switch
            {
                0UL => expected == 0UL,
                _ when journal.EntryOffsets.TryGetValue(prev, out var location) => location.Term == expected,
                _ when journal.SnapshotBaseline.LastIncludedIndex == prev => journal.SnapshotBaseline.LastIncludedTerm == expected,
                _ => false,
            };
        }

        internal static FollowerLogAppendResult? VerifyPreviousLogConsistency(FollowerLogJournal journal, IFollowerLogContext owner, in FollowerLogAppendRequest request)
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

                // A term of zero at an applied index means the frame was compacted away and is not the snapshot
                // baseline: the prefix cannot be verified from memory. Terms start at 1, so a zero term is always
                // unverifiable and must be refused rather than accepted as consistent.
                var logMatches = term != 0UL && term == request.PrevLogTerm;
                var unverifiable = term == 0UL;
                return (logMatches, unverifiable) switch
                {
                    (true, _) => null,
                    (false, true) => new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, owner.Meta.CurrentTerm, owner.LastLogIndex),
                    (false, false) => FailReadiness(owner),
                };
            }

            // A term conflict at or below the committed index violates the Leader Completeness property; fail readiness.
            var termMatches = TermAt(journal, request.PrevLogIndex) == request.PrevLogTerm;
            var belowCommitted = request.PrevLogIndex <= owner.Meta.CommitIndex;
            return (termMatches, belowCommitted) switch
            {
                (true, _) => null,
                (false, true) => FailReadiness(owner),
                (false, false) => new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, owner.Meta.CurrentTerm, owner.LastLogIndex),
            };

            static ulong TermAt(FollowerLogJournal journal, ulong logIndex)
            {
                return journal.Entries.TryGetValue(logIndex, out var entry) ? entry.Term : 0UL;
            }

            static ulong TermAtApplied(FollowerLogJournal journal, ulong logIndex)
            {
                // The snapshot base's frame was compacted away, but its term is retained for consistency checks.
                var baseline = journal.SnapshotBaseline;
                var baselineTerm = baseline.LastIncludedIndex > 0UL && logIndex == baseline.LastIncludedIndex ? baseline.LastIncludedTerm : 0UL;
                return journal.EntryOffsets.TryGetValue(logIndex, out var location) ? location.Term : baselineTerm;
            }
        }

        /// <summary>Waits for the mirror copy of an earlier deferred publish, so the metadata store is never written concurrently.</summary>
        /// <param name="journal">The storage surface.</param>
        /// <param name="cancellationToken">Cancellation token that bounds the wait.</param>
        /// <returns>A task that completes once no mirror copy is pending.</returns>
        /// <exception cref="OperationCanceledException">The token was canceled before the mirror copy finished; the mirror copy stays pending.</exception>
        /// <remarks>The failure of the mirror copy is thrown once, to the first publish that waits for it, and that publish does not run.</remarks>
        [SuppressMessage("Usage", "VSTHRD003", Justification = "The pending mirror is the ack of a work item this log scheduled; the work item or the shutdown drain completes it.")]
        private static async Task AwaitPendingMirrorAsync(FollowerLogJournal journal, CancellationToken cancellationToken)
        {
            if (journal.Metadata.PendingMirror is not { } pending)
                return;

            if (!pending.IsCompleted)
                await pending.WaitAsync(cancellationToken).ConfigureAwait(false);

            journal.Metadata.PendingMirror = null;
            await pending.ConfigureAwait(false);
        }

        private static async Task<FollowerLogAppendResult> CompleteAppendAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            ulong leaderCommitIndex,
            ulong lastVerifiedIndex,
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

            // The last log index is not stored: recovery derives it from the log file.
            if (commitLogical is { } candidate)
            {
                await PersistMetaOrFailReadinessAsync(journal, owner, candidate, cancellationToken).ConfigureAwait(false);
                owner.Meta = candidate;
            }

            if (commitAdvanced)
                owner.Faults.OnCommitAdvanced();

            return new FollowerLogAppendResult(true, string.Empty, owner.Meta.CurrentTerm, owner.LastLogIndex);
        }

        private static FollowerLogAppendResult FailReadiness(IFollowerLogContext owner)
        {
            owner.Readiness = FollowerLogReadiness.Failed;
            return new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, owner.Meta.CurrentTerm, owner.LastLogIndex);
        }

        private static bool IsSatisfiedByLocalState(FollowerLogJournal journal, IFollowerLogContext owner, in FollowerLogEntry candidate)
        {
            // The term of an applied entry was released with its payload, so it is read back from the retained
            // frame metadata. Compaction removes the covered prefix from EntryOffsets, so a frame that is absent
            // there is correctly not acknowledged; a retained frame is verifiable by term alone because Leader
            // Completeness forbids a conflicting term at an applied index.
            var localSatisfied = candidate.LogIndex <= owner.LastLogIndex && journal.TryGetEntry(candidate.LogIndex, out var existing) && existing.Term == candidate.Term &&
                                 existing.PayloadSpan.SequenceEqual(candidate.PayloadSpan);
            var appliedSatisfied = candidate.LogIndex <= owner.Meta.LastAppliedIndex && journal.TryGetEntryOffset(candidate.LogIndex, out var loc) && loc.Term == candidate.Term;
            return localSatisfied || appliedSatisfied;
        }

        private static List<FollowerLogEntry> MaterializeOwnedEntries(List<FollowerLogEntry> source)
        {
            // Ownership is needed only for what will actually hit disk or be retained above the applied
            // watermark; duplicates satisfied by local state and refused batches are never copied.
            var owned = new List<FollowerLogEntry>(source.Count);
            for (var i = 0; i < source.Count; i++)
                owned.Add(new FollowerLogEntry(source[i].LogIndex, source[i].Term, source[i].Payload.ToArray()));

            return owned;
        }

        private static FollowerLogAppendResult? PrepareAppendBatch(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            in FollowerLogAppendRequest request,
            out List<FollowerLogEntry>? toAppend,
            out ulong? truncateAtIndex)
        {
            toAppend = null;
            truncateAtIndex = null;
            var nextExpected = request.PrevLogIndex + 1;
            var entries = request.Entries.Span;

            for (var i = 0; i < entries.Length; i++)
            {
                // A leader never sends an entry of a term above its own: such a batch is malformed, and taking it would leave this
                // copy with a last log term above its current term.
                var entry = entries[i];
                if (entry.LogIndex != nextExpected || entry.Term > request.CurrentTerm)
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

            var startOffset = owner.LogLength;
            var (buffer, totalLength, offsets) = EncodeFrames(toAppend, startOffset);

            // The durability worker flushes the exact-size buffer and reports any I/O fault through the ack. The
            // buffer is owned by the work item and returned only inside its Execute; scheduling with a non-cancelable
            // token after the explicit check guarantees the callback always runs and the buffer is returned once the work is scheduled.
            var work = new AppendDurableWork(owner.Acks, owner.Durability, buffer, startOffset, totalLength, owner.Faults);

            owner.Faults.OnFramesScheduling();

            // The classification point is the scheduling: RunTrackedAsync is synchronous up to it, so a throw here means nothing was handed
            // to the storage and the failure is definite. It must stay synchronous up to the scheduling for this to hold. The rented
            // buffer of that path is not returned to the pool; it is collected like any array. That path is reachable only through a narrow
            // race with the dispose, which closes the ack registry.
            Task durable;
            try
            {
                durable = work.RunTrackedAsync();
            }
            catch (ObjectDisposedException ex)
            {
                throw new ReplicaShutdownRefusedException(nameof(FollowerLog), ex);
            }

            try
            {
                await durable.ConfigureAwait(false);

                // Only after the durable writing succeeds do the in-memory indexes gain the entries,
                // so a crash mid-appending can never leave the index ahead of the file.
                IndexAppendedFrames(journal, owner, toAppend, offsets, startOffset + totalLength);
            }
            catch (Exception ex)
            {
                // A failure after the frames were handed to the storage leaves them possibly on disk, where a restart recovers them as a tail
                // that can commit, and a write after a failed flush is unsafe: a torn leftover behind the next frame can pass for a record.
                // A shutdown that faulted the waiter leaves the write running, so it is no different. The log therefore refuses every
                // further append until the node restarts.
                owner.LatchAppendUnconfirmed(ex);
                throw new GroupLogAppendUnconfirmedException(ex);
            }
        }

        internal static Task PersistMetaAsync(FollowerLogJournal journal, IFollowerLogContext owner, in GroupLogMetadata meta, bool mirrored, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // A non-cancelable scheduling token after the explicit check guarantees the worker always runs.
            return new MetaDurableWork(owner.Acks, journal.Metadata, in meta, mirrored).RunTrackedAsync();
        }

        internal static (Task FirstCopy, Task Mirror) PersistMetaDeferredMirror(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            in GroupLogMetadata meta,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            return new DeferredMirrorMetaDurableWork(owner.Acks, journal.Metadata, in meta).RunDeferredTracked();
        }

        internal static async Task<(int Length, List<long> Offsets)> ReplaceLogAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            List<FollowerLogEntry> tail,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var standbySlot = 1 - journal.LogSlot;
            var generation = journal.LogGeneration + 1;
            var work = new ReplaceDurableWork(owner.Acks, owner.Durability, journal.Paths.GetLogSlotPath(standbySlot), generation, tail, owner.Faults);
            try
            {
                await work.RunTrackedAsync().ConfigureAwait(false);
            }
            finally
            {
                // Once the header is flushed the new file is the durable log, even when a later step fails.
                if (work.Sealed)
                    journal.UseLog(standbySlot, generation);
            }

            return (work.Length, work.Offsets);
        }

        internal static async Task<int> TruncateFromAsync(FollowerLogJournal journal, IFollowerLogContext owner, ulong logIndex, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!journal.EntryOffsets.TryGetValue(logIndex, out var location))
                throw new InvalidOperationException($"Replica group '{owner.GroupId}' cannot truncate from a missing index '{logIndex}'.");

            var released = 0;
            var durable = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            try
            {
                var work = new TruncateDurableWork(owner.Acks, owner.Durability, location.Offset, owner.Faults, durable);
                try
                {
                    await work.RunTrackedAsync().ConfigureAwait(false);
                }
                finally
                {
                    // OnFlushed may throw after the flush completed. The work item records that durable boundary so
                    // reservations are released whenever the truncated bytes are no longer recoverable.
                    if (durable.Task.IsCompletedSuccessfully)
                        released = owner.Idempotency.ReleaseFromIndex(logIndex);
                }
            }
            catch (Exception ex)
            {
                // A truncate that failed before its durable point may or may not have cut the file, while the memory below is reconciled to
                // the cut. The log cannot tell which, so it refuses further appends until a restart re-reads the file.
                if (!durable.Task.IsCompletedSuccessfully)
                    owner.LatchTruncateUnconfirmed(ex);

                throw;
            }
            finally
            {
                // Reconcile the in-memory indexes even when the durable truncate throws (SetLength may already
                // have been applied), so the ready log never validates against a suffix that may no longer be
                // durable. The failure still propagates; the append whose frames were never scheduled is a definite failure.
                journal.RemoveEntriesAbove(logIndex - 1UL);
                owner.LastLogIndex = logIndex - 1;
                owner.LogLength = location.Offset;
            }

            return released;
        }

        /// <summary>Encodes a batch of entries into one rented buffer and records the on-disk offset of each frame.</summary>
        /// <param name="toAppend">The entries to encode, in log order.</param>
        /// <param name="startOffset">The log file offset the first frame is written at.</param>
        /// <returns>The rented buffer, the encoded length of the batch, and the log index and offset of each frame.</returns>
        private static (byte[] Buffer, int Length, List<KeyValuePair<ulong, long>> Offsets) EncodeFrames(List<FollowerLogEntry> toAppend, long startOffset)
        {
            // The whole batch is encoded into a single contiguous buffer, so the OS
            // performs one sequential writing instead of many small ones.
            var totalLength = 0;
            for (var i = 0; i < toAppend.Count; i++)
                totalLength += GroupLogCodec.ComputeFrameEncodedLength(toAppend[i].Payload.Length);

            var buffer = ArrayPool<byte>.Shared.Rent(totalLength);
            var position = 0;

            // Each frame is encoded at its final on-disk offset, and that offset is remembered,
            // so the in-memory index can serve reads without re-walking the file.
            var offsets = new List<KeyValuePair<ulong, long>>(toAppend.Count);
            for (var i = 0; i < toAppend.Count; i++)
            {
                var entry = toAppend[i];
                var encodedLength = GroupLogCodec.ComputeFrameEncodedLength(entry.Payload.Length);
                var frameOffset = startOffset + position;
                GroupLogCodec.EncodeFrame(buffer.AsSpan(position, encodedLength), in entry);
                offsets.Add(new KeyValuePair<ulong, long>(entry.LogIndex, frameOffset));
                position += encodedLength;
            }

            return (buffer, totalLength, offsets);
        }

        private static void IndexAppendedFrames(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            List<FollowerLogEntry> appended,
            List<KeyValuePair<ulong, long>> offsets,
            long logLength)
        {
            for (var i = 0; i < offsets.Count; i++)
                journal.AddEntry(appended[i], offsets[i].Value, appended[i].Term);

            owner.LastLogIndex = offsets[^1].Key;
            owner.LogLength = logLength;
        }

        /// <summary>Durable work whose caller awaits a tracked ack instead of the pool task.</summary>
        /// <remarks>
        /// The pool thread owns the outcome of the operation: it completes the ack with the result or the failure of
        /// <see cref="Run" /> and then untracks it. The only other writer is the shutdown drain of the owning log, which
        /// faults the ack with <see cref="ObjectDisposedException" />; the first writer wins, so a late outcome is a no-op.
        /// </remarks>
        [Immutable]
        private abstract class TrackedDurableWorkBase : IWorkPoolItem
        {
            private readonly TaskCompletionSource _ack = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly FollowerLogAckRegistry _acks;

            protected TrackedDurableWorkBase(FollowerLogAckRegistry acks)
            {
                _acks = acks;
            }

            void IWorkPoolItem.Execute()
            {
                // Outcome first, then untrack: an ack is never untracked before its outcome is written, and a drain
                // that takes an already completed ack leaves it as is and does not count it.
                try
                {
                    _ack.RunIsolated(this, static work => work.Run());
                }
                finally
                {
                    _acks.Complete(_ack);
                }
            }

            /// <summary>Tracks the ack and schedules the work item on a pool thread.</summary>
            /// <remarks>Synchronous up to the scheduling: a throw means nothing was scheduled, which callers rely on to tell a definite failure from an unconfirmed one.</remarks>
            /// <returns>The ack task; the discarded pool task never faults because <see cref="IWorkPoolItem.Execute" /> reports through the ack.</returns>
            /// <exception cref="ObjectDisposedException">The owning log was disposed; nothing was scheduled.</exception>
            [SuppressMessage("Usage", "VSTHRD003", Justification = "The ack is created by this work item and completed only by its own Execute or by the shutdown drain.")]
            internal Task RunTrackedAsync()
            {
                _acks.Track(_ack);
                try
                {
                    _ = WorkPool.RunAsync(this, TaskCreationOptions.None, CancellationToken.None);
                }
                catch
                {
                    _acks.Complete(_ack);
                    throw;
                }

                return _ack.Task;
            }

            /// <summary>Performs the durable operation on the pool thread.</summary>
            protected abstract void Run();
        }

        /// <summary>Background work that durably writes appended frames and flushes them.</summary>
        [Immutable]
        private sealed class AppendDurableWork : TrackedDurableWorkBase
        {
            private readonly byte[] _buffer;
            private readonly GroupLogDurability _durability;
            private readonly IFollowerLogFaultHooks _faults;
            private readonly int _length;
            private readonly long _startOffset;

            internal AppendDurableWork(FollowerLogAckRegistry acks, GroupLogDurability durability, byte[] buffer, long startOffset, int length, IFollowerLogFaultHooks faults)
                : base(acks)
            {
                _durability = durability;
                _buffer = buffer;
                _startOffset = startOffset;
                _length = length;
                _faults = faults;
            }

            protected override void Run()
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

        /// <summary>Background work that durably publishes metadata through the metadata store.</summary>
        [Immutable]
        private sealed class MetaDurableWork : TrackedDurableWorkBase
        {
            private readonly GroupLogMetadata _meta;
            private readonly bool _mirrored;
            private readonly GroupMetadataStore _store;

            internal MetaDurableWork(FollowerLogAckRegistry acks, GroupMetadataStore store, in GroupLogMetadata meta, bool mirrored)
                : base(acks)
            {
                _store = store;
                _meta = meta;
                _mirrored = mirrored;
            }

            protected override void Run()
            {
                if (_mirrored)
                    _store.PublishMirrored(in _meta);
                else
                    _store.Publish(in _meta);
            }
        }

        /// <summary>Background work that durably publishes metadata into one slot, signals it, and then publishes the same record into the other slot.</summary>
        /// <remarks>
        /// Both copies are written by this one work item, so the metadata store is never written concurrently. The ack of the base class is the
        /// mirror copy; the first copy has its own tracked ack, which the shutdown drain faults like any other. A failed first copy completes both
        /// with the failure and writes no mirror copy.
        /// </remarks>
        [Immutable]
        private sealed class DeferredMirrorMetaDurableWork : TrackedDurableWorkBase
        {
            private readonly FollowerLogAckRegistry _acks;
            private readonly TaskCompletionSource _firstCopy = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly GroupLogMetadata _meta;
            private readonly GroupMetadataStore _store;

            internal DeferredMirrorMetaDurableWork(FollowerLogAckRegistry acks, GroupMetadataStore store, in GroupLogMetadata meta)
                : base(acks)
            {
                _acks = acks;
                _store = store;
                _meta = meta;
            }

            /// <summary>Tracks both acks and schedules the work item on a pool thread.</summary>
            /// <returns>The ack of the first copy and the ack of the mirror copy.</returns>
            /// <exception cref="ObjectDisposedException">The owning log was disposed; nothing was scheduled.</exception>
            [SuppressMessage("Usage", "VSTHRD003", Justification = "The first-copy ack is created by this work item and completed only by its own Run or by the shutdown drain.")]
            internal (Task FirstCopy, Task Mirror) RunDeferredTracked()
            {
                _acks.Track(_firstCopy);
                try
                {
                    return (_firstCopy.Task, RunTrackedAsync());
                }
                catch
                {
                    _acks.Complete(_firstCopy);
                    throw;
                }
            }

            protected override void Run()
            {
                try
                {
                    _store.Publish(in _meta);
                }
                catch (Exception ex)
                {
                    _ = _firstCopy.TrySetException(ex);
                    _acks.Complete(_firstCopy);
                    throw;
                }

                _ = _firstCopy.TrySetResult();
                _acks.Complete(_firstCopy);
                _store.Publish(in _meta);
            }
        }

        /// <summary>Writes and flushes each retained frame into the inactive generation file, seals it, and makes it the live log.</summary>
        /// <remarks>
        /// Frames are encoded into short-lived pooled buffers one at a time, so the retained tail is never materialized into a
        /// single contiguous array (which could overflow <see cref="int" /> or exhaust contiguous memory for a large tail). The superseded
        /// file stays intact until the sealed header of the new one is flushed, so a crash at any point leaves a complete log.
        /// </remarks>
        private sealed class ReplaceDurableWork : TrackedDurableWorkBase
        {
            private readonly GroupLogDurability _durability;
            private readonly IFollowerLogFaultHooks _faults;
            private readonly ulong _generation;
            private readonly string _standbyPath;
            private readonly List<FollowerLogEntry> _tail;

            internal ReplaceDurableWork(
                FollowerLogAckRegistry acks,
                GroupLogDurability durability,
                string standbyPath,
                ulong generation,
                List<FollowerLogEntry> tail,
                IFollowerLogFaultHooks faults)
                : base(acks)
            {
                _durability = durability;
                _standbyPath = standbyPath;
                _generation = generation;
                _tail = tail;
                _faults = faults;
            }

            internal int Length { get; private set; }

            internal List<long> Offsets { get; } = [];

            internal bool Sealed { get; private set; }

            protected override void Run()
            {
                long total = GroupLogSlots.HeaderSize;
                for (var i = 0; i < _tail.Count; i++)
                    total += GroupLogCodec.ComputeFrameEncodedLength(_tail[i].Payload.Length);
                if (total > int.MaxValue)
                    throw new InvalidOperationException($"Compaction tail of {total} bytes exceeds the maximum log size.");

                var handle = _durability.OpenStandbyOnce(_standbyPath);
                try
                {
                    WriteTail(handle, total);
                    _faults.OnFlushed();
                    GroupLogDurability.Seal(handle, _generation);
                    Sealed = true;
                    _faults.OnFlushed();

                    // Adopt owns the handle from here on, whether or not emptying the superseded file succeeds.
                    var sealedHandle = handle;
                    handle = null;
                    _durability.Adopt(sealedHandle);
                }
                finally
                {
                    handle?.Dispose();
                }

                Length = int.CreateTruncating(total);
            }

            private void WriteTail(SafeFileHandle handle, long total)
            {
                long position = GroupLogSlots.HeaderSize;
                for (var i = 0; i < _tail.Count; i++)
                {
                    var entry = _tail[i];
                    var frameLength = GroupLogCodec.ComputeFrameEncodedLength(entry.Payload.Length);
                    var frameBuffer = ArrayPool<byte>.Shared.Rent(frameLength);
                    try
                    {
                        GroupLogCodec.EncodeFrame(frameBuffer.AsSpan(0, frameLength), in entry);
                        RandomAccess.Write(handle, frameBuffer.AsSpan(0, frameLength), position);
                    }
                    finally
                    {
                        // The frame bytes are in the open handle; release the pooled
                        // buffer immediately, so a large tail never holds every frame buffer at once.
                        ArrayPool<byte>.Shared.ReturnCleared(frameBuffer);
                    }

                    Offsets.Add(position);
                    position += frameLength;
                }

                RandomAccess.SetLength(handle, total);
                RandomAccess.FlushToDisk(handle);
            }
        }

        /// <summary>Background work that durably truncates the log before a conflicting tail is rewritten.</summary>
        [Immutable]
        private sealed class TruncateDurableWork : TrackedDurableWorkBase
        {
            private readonly GroupLogDurability _durability;
            private readonly TaskCompletionSource<bool> _durable;
            private readonly IFollowerLogFaultHooks _faults;
            private readonly long _length;

            internal TruncateDurableWork(FollowerLogAckRegistry acks, GroupLogDurability durability, long length, IFollowerLogFaultHooks faults, TaskCompletionSource<bool> durable)
                : base(acks)
            {
                _durability = durability;
                _length = length;
                _faults = faults;
                _durable = durable;
            }

            protected override void Run()
            {
                _durability.Truncate(_length);
                _durability.Flush();
                _ = _durable.TrySetResult(true);
                _faults.OnFlushed();
            }
        }
    }

    /// <summary>Election voting operations for a follower log.</summary>
    /// <remarks>
    /// The caller holds the log gate; term and vote transitions persist durably through the owner before
    /// any granted result is reported, so a restart never observes a phantom term or a second vote. A grant at a higher term stores the
    /// term and the vote as one record, and a refusal at a higher term stores the term alone. Either record is mirrored into both slots, and the
    /// result is reported after the first copy is durable: the mirror copy follows in the same work item and stays pending until the next
    /// metadata publish or the dispose waits for it.
    /// </remarks>
    private static class FollowerLogElection
    {
        internal static FollowerLogVoteResult CheckPreVote(FollowerLogJournal journal, IFollowerLogContext owner, in ElectionVoteRequest request)
        {
            // A pre-vote probe never steps the term: an isolated follower soliciting probes must not inflate
            // its durable term, and the reported term always stays the locally persisted one.
            return true switch
            {
                _ when request.Term < owner.Meta.CurrentTerm => new FollowerLogVoteResult(false, FollowerLogRefusal.StaleTerm, owner.Meta.CurrentTerm),
                _ when request.Term == owner.Meta.CurrentTerm && owner.Meta.VotedFor.Length != 0 &&
                       !string.Equals(owner.Meta.VotedFor, request.CandidateId, StringComparison.Ordinal) => new FollowerLogVoteResult(
                    false,
                    FollowerLogRefusal.AlreadyVoted,
                    owner.Meta.CurrentTerm),
                _ when !FollowerLogAppend.IsLogUpToDate(request.LastLogTerm, request.LastLogIndex, CurrentLastLogTerm(journal, owner), owner.LastLogIndex) =>
                    new FollowerLogVoteResult(false, FollowerLogRefusal.StaleLog, owner.Meta.CurrentTerm),
                _ => new FollowerLogVoteResult(true, string.Empty, owner.Meta.CurrentTerm),
            };
        }

        internal static async Task<FollowerLogVoteResult> RequestVoteAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            ElectionVoteRequest request,
            CancellationToken cancellationToken)
        {
            // A stale candidate term authorizes nothing and never touches durable state.
            if (request.Term < owner.Meta.CurrentTerm)
                return new FollowerLogVoteResult(false, FollowerLogRefusal.StaleTerm, owner.Meta.CurrentTerm);

            // The checks run against the state a higher term would step to, before anything is written: the step clears the previous vote, so a
            // higher term can never be refused as already voted, and a grant at a higher term stores the term and the vote as one record.
            var higherTerm = request.Term > owner.Meta.CurrentTerm;
            var votedFor = higherTerm ? string.Empty : owner.Meta.VotedFor;

            // At most one vote per term: only the recorded candidate may be re-granted.
            if (votedFor.Length != 0 && !string.Equals(votedFor, request.CandidateId, StringComparison.Ordinal))
                return new FollowerLogVoteResult(false, FollowerLogRefusal.AlreadyVoted, owner.Meta.CurrentTerm);

            // A candidate whose log trails the voter log cannot win the election. A higher term is still stored on its own: the voter has
            // seen it, and a crash must not forget it.
            if (!FollowerLogAppend.IsLogUpToDate(request.LastLogTerm, request.LastLogIndex, CurrentLastLogTerm(journal, owner), owner.LastLogIndex))
            {
                if (higherTerm)
                    await StoreAsync(journal, owner, owner.Meta with { CurrentTerm = request.Term, VotedFor = string.Empty }, cancellationToken).ConfigureAwait(false);

                return new FollowerLogVoteResult(false, FollowerLogRefusal.StaleLog, owner.Meta.CurrentTerm);
            }

            // A replayed grant for the recorded candidate needs no durable rewrite: the vote was
            // persisted before the first grant was reported, so the file already records it.
            if (!higherTerm && string.Equals(votedFor, request.CandidateId, StringComparison.Ordinal))
                return new FollowerLogVoteResult(true, string.Empty, owner.Meta.CurrentTerm);

            // The granted vote is persisted before reporting success, so a restart never grants a second vote. This is also the path of a
            // candidate's vote for itself. Once a higher term is going to be stored it is part of the log whatever happens next; the one record
            // is scheduled under the caller's token and, once its first copy is durable, the vote runs to completion.
            await StoreAsync(journal, owner, owner.Meta with { CurrentTerm = request.Term, VotedFor = request.CandidateId }, cancellationToken).ConfigureAwait(false);
            return new FollowerLogVoteResult(true, string.Empty, owner.Meta.CurrentTerm);
        }

        /// <summary>Persists a vote record mirrored into both slots and makes it the metadata of the log once its first copy is durable.</summary>
        /// <param name="journal">The storage surface.</param>
        /// <param name="owner">The owning follower log.</param>
        /// <param name="candidate">The metadata to store in both slots.</param>
        /// <param name="cancellationToken">Cancellation token for the wait and the scheduling.</param>
        /// <returns>A task that completes once the first copy is durable and the metadata is current.</returns>
        /// <remarks>
        /// The mirror copy follows right after in the same work item and stays pending on the owner, so the reply waits for one flush. A damaged
        /// newest slot would otherwise roll a term or a vote back to the previous record, which is why the record is mirrored at all.
        /// </remarks>
        private static async Task StoreAsync(FollowerLogJournal journal, IFollowerLogContext owner, GroupLogMetadata candidate, CancellationToken cancellationToken)
        {
            await FollowerLogAppend.PersistDeferredMirrorOrFailAsync(journal, owner, candidate, cancellationToken).ConfigureAwait(false);
            owner.Meta = candidate;
        }

        private static ulong CurrentLastLogTerm(FollowerLogJournal journal, IFollowerLogContext owner)
        {
            return true switch
            {
                _ when owner.LastLogIndex == 0UL => 0UL,
                _ when journal.TryGetEntryOffset(owner.LastLogIndex, out var location) => location.Term,
                _ when journal.SnapshotBaseline.LastIncludedIndex == owner.LastLogIndex => journal.SnapshotBaseline.LastIncludedTerm,
                _ => 0UL,
            };
        }
    }

    /// <summary>The lifecycle bits of a follower log that are read without the log gate: disposed, and complete.</summary>
    /// <remarks>
    /// The complete bit is a hint for a fast path: a reader that finds it set may be one step behind a state transfer that just cleared it, so the
    /// log checks the member state again under its gate before it serves a call.
    /// </remarks>
    private static class FollowerLogLifecycle
    {
        /// <summary>The log has been disposed of.</summary>
        internal const int Disposed = 1;

        /// <summary>The copy is complete; a state transfer clears it when it marks the copy incomplete, and its completion sets it again.</summary>
        internal const int Complete = 2;

        /// <summary>Makes metadata the metadata of the log, and publishes whether the copy is complete to readers outside the gate.</summary>
        /// <param name="slot">The metadata of the log.</param>
        /// <param name="lifecycle">The lifecycle bits of the log.</param>
        /// <param name="value">The new metadata.</param>
        internal static void Assign(ref GroupLogMetadata slot, ref int lifecycle, in GroupLogMetadata value)
        {
            slot = value;
            _ = value.MemberState == GroupMemberState.Complete ? Interlocked.Or(ref lifecycle, Complete) : Interlocked.And(ref lifecycle, ~Complete);
        }
    }

    /// <summary>Bootstrap operations of a follower log: the offer, and the completion of an unseeded copy.</summary>
    /// <remarks>
    /// The caller holds the log gate. Every record is published to both metadata slots through the mirrored path, which fails readiness on a
    /// fault like every other metadata write. A crash between the two slot writes leaves a legal state either way: after the first copy of the
    /// complete record a rollback to the offered record completes again on the next member call, and nothing was acknowledged in between, because
    /// that call is processed only after both copies are durable.
    /// </remarks>
    private static class FollowerLogBootstrap
    {
        internal static async Task<string?> AcceptOfferAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            bool open,
            bool topologyMismatch,
            CancellationToken cancellationToken)
        {
            var meta = owner.Meta;
            if (!open || meta.MemberState != GroupMemberState.Unseeded)
                return FollowerLogRefusal.NotReady;

            if (topologyMismatch)
                return FollowerLogRefusal.TopologyMismatch;

            // Published to both slots even when the copy holds the offer: a crash between the two slot writes of an earlier offer left it in one.
            await StoreAsync(journal, owner, meta with { BootstrapOffered = true }, cancellationToken).ConfigureAwait(false);
            return null;
        }

        internal static async Task CompleteOwnerAsync(FollowerLogJournal journal, IFollowerLogContext owner, bool open, CancellationToken cancellationToken)
        {
            var meta = owner.Meta;
            if (!open || meta.MemberState != GroupMemberState.Unseeded)
                throw new InvalidOperationException($"Replica group '{owner.GroupId}' copy is not an open unseeded copy, so it cannot be completed as the owner.");

            await StoreAsync(journal, owner, meta with { MemberState = GroupMemberState.Complete }, cancellationToken).ConfigureAwait(false);
        }

        internal static async Task<string?> CompleteIfOfferedAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            bool open,
            bool topologyMismatch,
            CancellationToken cancellationToken)
        {
            var meta = owner.Meta;
            if (!open)
                return FollowerLogRefusal.NotReady;

            if (topologyMismatch)
                return FollowerLogRefusal.TopologyMismatch;

            if (meta.MemberState == GroupMemberState.Complete)
                return null;

            if (meta.MemberState != GroupMemberState.Unseeded || !meta.BootstrapOffered)
                return NotCompleteRefusal(meta.MemberState);

            await StoreAsync(journal, owner, meta with { MemberState = GroupMemberState.Complete }, cancellationToken).ConfigureAwait(false);
            return null;
        }

        /// <summary>Gets the refusal marker of a copy that is not complete.</summary>
        /// <param name="state">The member state of the copy.</param>
        /// <returns><see cref="FollowerLogRefusal.Incomplete" /> for an incomplete copy, otherwise <see cref="FollowerLogRefusal.Unseeded" />.</returns>
        internal static string NotCompleteRefusal(GroupMemberState state) => state == GroupMemberState.Incomplete ? FollowerLogRefusal.Incomplete : FollowerLogRefusal.Unseeded;

        private static async Task StoreAsync(FollowerLogJournal journal, IFollowerLogContext owner, GroupLogMetadata candidate, CancellationToken cancellationToken)
        {
            await FollowerLogAppend.PersistMirroredMetaOrFailReadinessAsync(journal, owner, candidate, cancellationToken).ConfigureAwait(false);
            owner.Meta = candidate;
        }
    }

    /// <summary>Tail repair of a follower log: truncates the tail from an index a leader instructs.</summary>
    /// <remarks>The caller holds the log gate and has already refused a log that takes no mutating call.</remarks>
    private static class FollowerLogReconcile
    {
        internal static async Task<FollowerLogReconcileResult> ReconcileTailAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            ulong fromIndex,
            ulong prevLogTerm,
            ulong leaderTerm,
            CancellationToken cancellationToken)
        {
            // A stale leader term authorizes nothing: unlike the appending path, repair never advances the
            // current term, it only refuses instructions from deposed leaders.
            if (leaderTerm < owner.Meta.CurrentTerm)
                return new FollowerLogReconcileResult(false, FollowerLogRefusal.StaleTerm, 0, false);

            // A repair instruction is lifecycle-owned and trusted, but it still fails closed if it crosses the
            // durable commit boundary. No leader response may destructively revise a committed prefix.
            // A zero index is caller-bug input rather than storage corruption, so it is refused without quarantine.
            if (fromIndex == 0UL)
                return new FollowerLogReconcileResult(false, FollowerLogRefusal.LogMismatch, 0, false);

            if (fromIndex <= owner.Meta.CommitIndex)
            {
                owner.Readiness = FollowerLogReadiness.Failed;
                return new FollowerLogReconcileResult(false, FollowerLogRefusal.LogMismatch, 0, true);
            }

            // Previous-log consistency, mirroring the appending path: a stale leader must not truncate a tail
            // written by the current term. Verified before the no-op fast path, so a wrong term fails here
            // instead of costing another repair round-trip through the append consistency check.
            // An unverifiable predecessor (compacted below the snapshot baseline) is refused without
            // quarantine; a term conflict at or below the commit boundary fails readiness.
            if (!FollowerLogAppend.PrevTermMatches(journal, fromIndex - 1UL, prevLogTerm))
            {
                if (fromIndex - 1UL > owner.Meta.CommitIndex)
                    return new FollowerLogReconcileResult(false, FollowerLogRefusal.LogMismatch, 0, false);
                owner.Readiness = FollowerLogReadiness.Failed;
                return new FollowerLogReconcileResult(false, FollowerLogRefusal.LogMismatch, 0, true);
            }

            if (fromIndex == owner.LastLogIndex + 1UL)
                return new FollowerLogReconcileResult(true, string.Empty, 0, false);

            if (fromIndex > owner.LastLogIndex || !journal.EntryOffsets.ContainsKey(fromIndex))
                return new FollowerLogReconcileResult(false, FollowerLogRefusal.LogMismatch, 0, false);

            try
            {
                var released = await FollowerLogDurable.TruncateFromAsync(journal, owner, fromIndex, cancellationToken).ConfigureAwait(false);
                return new FollowerLogReconcileResult(true, string.Empty, released, false);
            }
            catch
            {
                // The low-level operation reconciles its indexes with any possible SetLength outcome. The explicit
                // repair path additionally quarantines storage because the caller cannot prove which durable boundary
                // survived an I/O fault until restart recovery scans the file again.
                owner.Readiness = FollowerLogReadiness.Failed;
                throw;
            }
        }
    }

    /// <summary>Reads a range of retained entries back from the log file.</summary>
    /// <remarks>The caller holds the log gate, so the frame offsets stay valid for the whole read.</remarks>
    private static class FollowerLogRead
    {
        internal static async Task<FollowerLogEntriesRead> ReadRangeAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            ulong fromIndex,
            int maxCount,
            CancellationToken cancellationToken)
        {
            var prev = fromIndex - 1UL;
            var lastLogIndex = owner.LastLogIndex;
            var commitIndex = owner.Meta.CommitIndex;

            // The position before the range must be verifiable by the follower: the origin, a retained frame, or the snapshot baseline.
            // Anything else is either compacted away or beyond the last index, and the caller backs up or fails closed.
            if (!TryGetRetainedTerm(journal, prev, out var prevTerm))
                return new FollowerLogEntriesRead(false, prev, 0UL, lastLogIndex, commitIndex, []);

            if (maxCount == 0 || fromIndex > lastLogIndex)
                return new FollowerLogEntriesRead(true, prev, prevTerm, lastLogIndex, commitIndex, []);

            var count = int.CreateTruncating(Math.Min(lastLogIndex - fromIndex + 1UL, ulong.CreateChecked(maxCount)));
            var frames = new (ulong LogIndex, long Offset)[count];
            for (var i = 0; i < count; i++)
            {
                var logIndex = fromIndex + ulong.CreateChecked(i);
                if (!journal.TryGetEntryOffset(logIndex, out var location))
                    throw new InvalidDataException($"Replica group '{owner.GroupId}' retains no frame for log index '{logIndex}' inside its retained range.");

                frames[i] = (logIndex, location.Offset);
            }

            var entries = new List<FollowerLogEntry>(count);
            _ = await GroupLogFrameReader.ReadAsync(
                journal.LogPath,
                frames,
                entry =>
                {
                    entries.Add(entry);
                    return true;
                },
                cancellationToken).ConfigureAwait(false);

            return new FollowerLogEntriesRead(true, prev, prevTerm, lastLogIndex, commitIndex, entries);
        }

        /// <summary>Resolves the term the log retains for <paramref name="logIndex" /> without throwing for an unretained index.</summary>
        /// <param name="journal">The paired in-memory journal state.</param>
        /// <param name="logIndex">The log index; zero is the log origin.</param>
        /// <param name="term">The retained term, or zero when the index is the origin or not retained.</param>
        /// <returns><see langword="true" /> when the index is the origin of a log never compacted, a retained frame, or the snapshot baseline.</returns>
        private static bool TryGetRetainedTerm(FollowerLogJournal journal, ulong logIndex, out ulong term)
        {
            // Once a snapshot baseline exists the origin is compacted away: the first retained frame follows the baseline, not the origin.
            if (logIndex == 0UL)
            {
                term = 0UL;
                return journal.SnapshotBaseline.LastIncludedIndex == 0UL;
            }

            if (journal.TryGetEntryOffset(logIndex, out var location))
            {
                term = location.Term;
                return true;
            }

            if (journal.SnapshotBaseline.LastIncludedIndex == logIndex)
            {
                term = journal.SnapshotBaseline.LastIncludedTerm;
                return true;
            }

            term = 0UL;
            return false;
        }
    }

    /// <summary>Startup validation of the on-disk group state.</summary>
    private static class FollowerLogStartup
    {
        /// <summary>Validates the on-disk state and recovers the committed prefix, leaving the log ready; the caller holds the gate.</summary>
        /// <param name="journal">The storage surface.</param>
        /// <param name="owner">The owning follower log.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>A task that completes when the group log is open and ready.</returns>
        /// <exception cref="InvalidDataException">Metadata is corrupt or missing beside a log file; readiness is set to <see cref="FollowerLogReadiness.Failed" />.</exception>
        internal static async Task OpenGroupAsync(FollowerLogJournal journal, IFollowerLogContext owner, CancellationToken cancellationToken)
        {
            var metaExists = File.Exists(journal.Paths.MetadataPath);
            SelectLog(journal, owner);
            var logExists = File.Exists(journal.Paths.LogSlot0Path) || File.Exists(journal.Paths.LogSlot1Path);

            switch (metaExists)
            {
                case false when !logExists && !journal.Snapshot.SnapshotExists:
                    // The fresh unseeded record is written to both slots, like a vote grant, so that damage to one slot does not make the file unreadable.
                    var fresh = new GroupLogMetadata(owner.GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 0UL, string.Empty, 0UL, 0UL, GroupMemberState.Unseeded, false);
                    await FollowerLogAppend.PersistMirroredMetaOrFailReadinessAsync(journal, owner, fresh, cancellationToken).ConfigureAwait(false);
                    owner.Meta = fresh;
                    owner.LastLogIndex = 0;
                    CreateFirstLog(journal, owner);
                    owner.Readiness = FollowerLogReadiness.Ready;
                    return;

                case false when !logExists:
                    // A published snapshot is a durable state even when metadata and the log are absent, but the vote memory of the group is
                    // gone with the metadata: a restart could grant a second vote in a term it already voted in. Fail readiness instead.
                    owner.Readiness = FollowerLogReadiness.Failed;
                    throw new InvalidDataException($"Replica group '{owner.GroupId}' metadata is missing while the snapshot file exists; the group requires recovery or repair.");
                case true:
                    // The store opens its handle only on a valid record, so a refused file is left untouched.
                    var loaded = journal.Metadata.Open();
                    if (loaded.Status != GroupMetadataReadStatus.Valid)
                    {
                        owner.Readiness = FollowerLogReadiness.Failed;
                        throw new InvalidDataException(GroupMetadataStore.DescribeFailure(loaded.Status, owner.GroupId));
                    }

                    owner.Meta = loaded.Metadata;
                    break;

                default:
                    // The log file exists without its metadata file, so the committed boundary is unknown.
                    // Assuming CommitIndex = 0 would treat every durable frame as an uncommitted tail and truncate it,
                    // destroying possibly-committed data. Fail readiness instead; the group requires explicit repair.
                    owner.Readiness = FollowerLogReadiness.Failed;
                    throw new InvalidDataException($"Replica group '{owner.GroupId}' metadata is missing while the log file exists; the group requires recovery or repair.");
            }

            await FollowerLogRecovery.RecoverLogFileAsync(journal, owner, cancellationToken).ConfigureAwait(false);
            if (journal.LogGeneration == 0UL)
            {
                CreateFirstLog(journal, owner);
            }
            else
            {
                // A retried open can still hold the superseded file from a rewrite that failed after its commit point; release it first.
                owner.Durability.Dispose();
                await FollowerLogRecovery.EmptySupersededLogAsync(journal, cancellationToken).ConfigureAwait(false);
                owner.Durability.Open(journal.LogPath, owner.LogLength);
            }

            owner.Readiness = FollowerLogReadiness.Ready;
        }

        private static void CreateFirstLog(FollowerLogJournal journal, IFollowerLogContext owner)
        {
            owner.Durability.Create(journal.Paths.GetLogSlotPath(0), 1UL);
            journal.UseLog(0, 1UL);
            owner.LogLength = GroupLogSlots.HeaderSize;
        }

        /// <summary>Picks the active group log file from the two generation files, refusing a log this build cannot trust.</summary>
        /// <param name="journal">The storage surface; its active log is set.</param>
        /// <param name="owner">The owning follower log, whose readiness fails on a refusal.</param>
        /// <exception cref="InvalidDataException">An earlier build's log exists, or the generation files are refused.</exception>
        private static void SelectLog(FollowerLogJournal journal, IFollowerLogContext owner)
        {
            if (File.Exists(journal.Paths.LegacyLogPath))
            {
                owner.Readiness = FollowerLogReadiness.Failed;
                throw new InvalidDataException(
                    $"Replica group '{owner.GroupId}' log at '{journal.Paths.LegacyLogPath}' was written by an earlier build and is not supported; the file was left unchanged.");
            }

            Span<byte> first = stackalloc byte[GroupLogSlots.HeaderSize];
            Span<byte> second = stackalloc byte[GroupLogSlots.HeaderSize];
            var firstLength = ReadLogHead(journal.Paths.LogSlot0Path, first, out var firstRead);
            var secondLength = ReadLogHead(journal.Paths.LogSlot1Path, second, out var secondRead);
            try
            {
                var (slot, generation) = GroupLogSlots.Select(first[..firstRead], firstLength, second[..secondRead], secondLength, journal.Paths.GroupDirectory);
                journal.UseLog(slot, generation);
            }
            catch (InvalidDataException)
            {
                owner.Readiness = FollowerLogReadiness.Failed;
                throw;
            }
        }

        private static long ReadLogHead(string path, Span<byte> head, out int read)
        {
            read = 0;
            if (!File.Exists(path))
                return -1;

            using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            var length = RandomAccess.GetLength(handle);
            while (read < head.Length)
            {
                var count = RandomAccess.Read(handle, head[read..], read);
                if (count == 0)
                    break;

                read += count;
            }

            return length;
        }
    }

    /// <summary>Startup recovery: rebuilds the in-memory log from the durable frame file.</summary>
    private static class FollowerLogRecovery
    {
        private const int FrameHeaderByteCount = 9;

        internal static async Task RecoverLogFileAsync(FollowerLogJournal journal, IFollowerLogContext owner, CancellationToken cancellationToken)
        {
            // A published snapshot restores the committed baseline; the durable log then continues from its included
            // index plus one when the covered prefix was compacted away.
            var snapshotBase = await RestoreSnapshotBaseAsync(journal, owner, cancellationToken).ConfigureAwait(false);

            // Generation 0 means the group has never persisted a log, so nothing exists beyond the snapshot baseline.
            if (journal.LogGeneration == 0UL)
            {
                await SnapshotBoundaryWatermarks.ResetAndEnsureAsync(journal, owner, snapshotBase, cancellationToken).ConfigureAwait(false);
                return;
            }

            var handle = File.OpenHandle(
                journal.LogPath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite | FileShare.Delete,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
            using (handle)
                await RecoverOpenedFileAsync(handle, journal, owner, snapshotBase, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>Empties the generation file the active one superseded, which a rewrite or a crash may have left holding a full copy of the log.</summary>
        /// <param name="journal">The storage surface.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        internal static async Task EmptySupersededLogAsync(FollowerLogJournal journal, CancellationToken cancellationToken)
        {
            var path = journal.Paths.GetLogSlotPath(1 - journal.LogSlot);
            if (!File.Exists(path) || new FileInfo(path).Length == 0)
                return;

            await WorkPool.RunAsync(new RecoveryTruncateWork(path, 0), TaskCreationOptions.None, cancellationToken).ConfigureAwait(false);
        }

        private static async Task RecoverOpenedFileAsync(
            SafeFileHandle handle,
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            ulong snapshotBase,
            CancellationToken cancellationToken)
        {
            if (await TryResetShortFileAsync(handle, journal, owner, snapshotBase, cancellationToken).ConfigureAwait(false))
                return;

            journal.ClearEntries();

            // A populated log starts at index one (full history), right after the compacted snapshot
            // base, or — when a newer snapshot was published after the last compaction — inside the
            // snapshot-covered prefix. In the third shape the discarded head is backed by the published
            // snapshot, so the walk resumes from the last journaled index below the first frame. Trusting
            // the snapshot base against a full log would misread its first frame as a committed gap, so
            // the starting index is derived from the first frame.
            var firstIndex = await PeekFirstFrameIndexAsync(handle, cancellationToken).ConfigureAwait(false);
            if (await TryResetTornFirstFrameAsync(journal, owner, firstIndex, snapshotBase, cancellationToken).ConfigureAwait(false))
                return;

            var walkBase = DeriveWalkBaseIndex(firstIndex, snapshotBase);
            owner.LastLogIndex = walkBase;

            var result = await WalkFramesAsync(journal, owner, handle, cancellationToken).ConfigureAwait(false);
            await FinishRecoveredWalkAsync(handle, journal, owner, snapshotBase, walkBase, result, cancellationToken).ConfigureAwait(false);
        }

        private static async Task<bool> TryResetShortFileAsync(
            SafeFileHandle handle,
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            ulong snapshotBase,
            CancellationToken cancellationToken)
        {
            // A header-only file carries no frames, so only the snapshot baseline (if any) is durable.
            if (RandomAccess.GetLength(handle) != GroupLogSlots.HeaderSize)
                return false;

            await SnapshotBoundaryWatermarks.ResetAndEnsureAsync(journal, owner, snapshotBase, cancellationToken).ConfigureAwait(false);
            return true;
        }

        private static async Task<bool> TryResetTornFirstFrameAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            ulong firstIndex,
            ulong snapshotBase,
            CancellationToken cancellationToken)
        {
            if (firstIndex != 0 || snapshotBase == 0)
                return false;

            // The first suffix frame after the validated snapshot base is torn; the entire suffix is
            // unrecoverable. Truncate the file to the header and restart from the snapshot baseline,
            // but only while the snapshot covers the whole committed prefix: destroying the file below
            // a higher durable commit watermark would turn a repairable shortfall into a permanent one.
            if (snapshotBase < owner.Meta.CommitIndex)
            {
                owner.Readiness = FollowerLogReadiness.Failed;
                throw new InvalidDataException($"Replica group '{owner.GroupId}' first journal frame above snapshot base '{snapshotBase}' is unreadable while the commit watermark '{owner.Meta.CommitIndex}' exceeds it.");
            }

            await TruncateAndResetAsync(journal, owner, GroupLogSlots.HeaderSize, snapshotBase, cancellationToken).ConfigureAwait(false);
            return true;
        }

        private static async Task FinishRecoveredWalkAsync(
            SafeFileHandle handle,
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            ulong snapshotBase,
            ulong walkBase,
            WalkResult result,
            CancellationToken cancellationToken)
        {
            // A crash between snapshot publication and journal truncation leaves the old journal in place; a
            // surviving frame at the restored snapshot's included index carrying a different term means the
            // suffix above it belongs to a divergent history and is discarded, restarting from the snapshot.
            if (result.BoundaryDivergent)
            {
                await TruncateAndResetAsync(journal, owner, GroupLogSlots.HeaderSize, snapshotBase, cancellationToken).ConfigureAwait(false);
                return;
            }

            // A torn trailing frame is truncated back to the last valid boundary on disk,
            // since a CRC mismatch alone cannot tell an appended tail from corruption.
            if (result.Truncated && result.LastValidEnd < RandomAccess.GetLength(handle))
                await ScheduleTruncateAsync(journal, result.LastValidEnd, cancellationToken).ConfigureAwait(false);

            await SnapshotBoundaryWatermarks.CompleteWalkAsync(journal, owner, snapshotBase, (walkBase, result.LastValidEnd), cancellationToken).ConfigureAwait(false);
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
            owner.Readiness = FollowerLogReadiness.Failed;
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
            return true switch
            {
                _ when firstIndex == 1 || snapshotBase == 0 => 0UL,
                _ when firstIndex <= snapshotBase => firstIndex - 1UL,
                _ when firstIndex == snapshotBase + 1 => snapshotBase,
                _ => 0UL,
            };
        }

        /// <summary>Detects a surviving journal frame whose term diverges from the restored snapshot at its included index.</summary>
        /// <param name="journal">The paired in-memory journal state.</param>
        /// <param name="logIndex">The validated frame's log index.</param>
        /// <param name="term">The validated frame's term.</param>
        /// <returns>The divergent-boundary result, or <see langword="null" /> when the frame agrees with the snapshot.</returns>
        private static WalkResult? DivergentBoundary(FollowerLogJournal journal, ulong logIndex, ulong term)
        {
            var baseline = journal.SnapshotBaseline;
            return baseline.LastIncludedIndex != logIndex || baseline.LastIncludedTerm == term ? null : new WalkResult(GroupLogSlots.HeaderSize, true, true);
        }

        /// <summary>Reads the log index of the first frame from the fixed header boundary.</summary>
        /// <param name="handle">The open log file handle.</param>
        /// <param name="cancellationToken">A token to observe while reading.</param>
        /// <returns>The first frame's log index, or <c language="csharp">0</c> when no valid frame opens the file.</returns>
        private static async Task<ulong> PeekFirstFrameIndexAsync(SafeFileHandle handle, CancellationToken cancellationToken)
        {
            const int start = GroupLogSlots.HeaderSize;
            var length = RandomAccess.GetLength(handle);
            if (length - start < FrameHeaderByteCount)
                return 0UL;

            var frameHeader = ArrayPool<byte>.Shared.Rent(FrameHeaderByteCount);
            try
            {
                var headerEnd = await HandleEx.ReadExactAsync(handle, frameHeader.AsMemory(0, FrameHeaderByteCount), start, cancellationToken).ConfigureAwait(false);
                if (headerEnd == null || !GroupLogCodec.TryReadFrameHeaderLength(frameHeader.AsSpan(0, FrameHeaderByteCount), out var frameLength))
                    return 0UL;

                if (frameLength > length - start)
                    return 0UL;

                var frame = ArrayPool<byte>.Shared.Rent(frameLength);
                try
                {
                    frameHeader.AsSpan(0, FrameHeaderByteCount).CopyTo(frame);
                    var frameEnd = await HandleEx.ReadExactAsync(
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

            var headerEnd = await HandleEx.ReadExactAsync(handle, frameHeader.AsMemory(0, FrameHeaderByteCount), frameStart, cancellationToken).ConfigureAwait(false);
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
            var payloadEnd = await HandleEx.ReadExactAsync(handle, frame.AsMemory(FrameHeaderByteCount, frameLength - FrameHeaderByteCount), payloadOffset, cancellationToken)
                                           .ConfigureAwait(false);

            // The frame content is incomplete: hand ownership back to the caller's pooling path.
            return payloadEnd == null
                ? new FrameReadOutcome(frame, exhausted, 0, nextLogIndex, UncommittedTail(owner, frameStart, nextLogIndex))
                : new FrameReadOutcome(frame, exhausted, frameLength, nextLogIndex, null);
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

            // A snapshot is authoritative for the covered prefix: a higher included term advances the durable
            // term and clears any vote cast in the older term before readiness, mirroring InstallAsync.
            var termRaised = snapshot.LastIncludedTerm > candidate.CurrentTerm;
            if (termRaised)
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

            // A raised term is mirrored into both slots, like every term step; other repairs go to one slot.
            var persist = termRaised
                ? FollowerLogAppend.PersistMirroredMetaOrFailReadinessAsync(journal, owner, candidate, cancellationToken)
                : FollowerLogAppend.PersistMetaOrFailReadinessAsync(journal, owner, candidate, cancellationToken);
            await persist.ConfigureAwait(false);
            owner.Meta = candidate;
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
                owner.LastLogIndex = logIndex;
                return null;
            }

            if (!GroupLogCodec.TryReadFrame(frame, out var entry))
                return UncommittedTail(owner, lastValidEnd, nextLogIndex);

            if (entry.LogIndex != nextLogIndex)
                return CommittedGap(owner, lastValidEnd, nextLogIndex);

            if (DivergentBoundary(journal, entry.LogIndex, entry.Term) is { } divergentBoundary)
                return divergentBoundary;

            journal.AddEntry(in entry, lastValidEnd, entry.Term);
            owner.LastLogIndex = entry.LogIndex;
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
                    owner.Readiness = FollowerLogReadiness.Failed;
                    throw new InvalidDataException($"Replica group '{owner.GroupId}' snapshot belongs to group '{snapshot.Value.GroupId}'.");
                }

                if (FollowerLogSnapshot.SnapshotTopologyMismatch(owner, snapshot.Value) != null)
                {
                    owner.Readiness = FollowerLogReadiness.Failed;
                    throw new InvalidDataException($"Replica group '{owner.GroupId}' snapshot topology or configuration generation conflicts with durable metadata.");
                }

                // A snapshot whose commit index falls below its included index is malformed: the committed prefix it
                // claims to cover is internally inconsistent, and adopting its boundary would let LastAppliedIndex
                // exceed CommitIndex. ReconcileSnapshotMetadataAsync assumes the decoder already rejected this; guard
                // here so recovery fails readiness before persisting an incoherent watermark, matching
                // ValidateInstallEligibility on the installation path.
                if (snapshot.Value.CommitIndex < snapshot.Value.LastIncludedIndex)
                {
                    owner.Readiness = FollowerLogReadiness.Failed;
                    throw new InvalidDataException(
                        $"Replica group '{owner.GroupId}' snapshot commit index '{snapshot.Value.CommitIndex}' is below its included index '{snapshot.Value.LastIncludedIndex}'.");
                }

                // Terms start at 1. A zero-included term collides with the unverifiable-term sentinel used by
                // TermAtApplied and would make DivergentBoundary discard the entire durable suffix.
                if (snapshot.Value is { LastIncludedIndex: > 0UL, LastIncludedTerm: 0UL })
                {
                    owner.Readiness = FollowerLogReadiness.Failed;
                    throw new InvalidDataException($"Replica group '{owner.GroupId}' snapshot included term is zero at index '{snapshot.Value.LastIncludedIndex}'.");
                }

                // Reconcile and durably persist the snapshot metadata BEFORE mutating in-memory recovery state. If the
                // persisting is canceled (OperationCanceledException, preserved as retryable by PersistMetaOrFailReadinessAsync)
                // or fails, the instance is left without partially restored idempotency/baseline, so a retry of OpenAsync on
                // the same instance cannot operate on inconsistent state. See F37.
                await ReconcileSnapshotMetadataAsync(journal, owner, snapshot.Value, cancellationToken).ConfigureAwait(false);
                owner.Idempotency.RestoreFromSnapshot(snapshot.Value.CommittedOutcomes, snapshot.Value.CapturedUtc, []);

                // Recovery rebuilds both indexes from the durable log after this point; the baseline is
                // restored without pruning because the walk owns the index lifecycle.
                owner.RestoreBaseline(new SnapshotBaseline(snapshot.Value.LastIncludedIndex, snapshot.Value.LastIncludedTerm));
                return snapshot.Value.LastIncludedIndex;
            }
            catch (InvalidDataException)
            {
                owner.Readiness = FollowerLogReadiness.Failed;
                throw;
            }
        }

        private static Task ScheduleTruncateAsync(FollowerLogJournal journal, long truncateLength, CancellationToken cancellationToken)
        {
            var work = new RecoveryTruncateWork(journal.LogPath, truncateLength);
            return WorkPool.RunAsync(work, TaskCreationOptions.None, cancellationToken);
        }

        private static async Task TruncateAndResetAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            long truncateLength,
            ulong snapshotBase,
            CancellationToken cancellationToken)
        {
            var work = new RecoveryTruncateWork(journal.LogPath, truncateLength);
            await WorkPool.RunAsync(work, TaskCreationOptions.None, cancellationToken).ConfigureAwait(false);
            journal.ClearEntries();

            // The truncation dropped every frame at or below the snapshot base, so nothing can apply them any more: the applied watermark
            // moves to the base, as a snapshot installation moves it. Keeping it below would point the applier at entries no frame holds,
            // and its catch-up would refuse the gap forever. The commit index already reaches the base, which the snapshot reconciliation
            // raised it to, so the watermark never passes the commit index.
            await SnapshotBoundaryWatermarks.ResetAndEnsureAsync(journal, owner, snapshotBase, cancellationToken).ConfigureAwait(false);
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
            owner.Readiness = FollowerLogReadiness.Failed;
            throw new InvalidDataException($"Replica group '{owner.GroupId}' committed log frame at index '{nextLogIndex}' is corrupt.");
        }

        private static async Task<WalkResult> WalkFramesAsync(FollowerLogJournal journal, IFollowerLogContext owner, SafeFileHandle handle, CancellationToken cancellationToken)
        {
            long lastValidEnd = GroupLogSlots.HeaderSize;

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
                using var handle = File.OpenHandle(_path, FileMode.Open, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete, FileOptions.None);
                RandomAccess.SetLength(handle, _length);
                RandomAccess.FlushToDisk(handle);
            }
        }
    }

    /// <summary>Watermark rules that tie startup recovery to the restored snapshot boundary.</summary>
    private static class SnapshotBoundaryWatermarks
    {
        /// <summary>Completes recovery of a populated log after its frames were walked.</summary>
        /// <param name="journal">The paired in-memory journal state.</param>
        /// <param name="owner">The log being recovered.</param>
        /// <param name="snapshotBase">The restored snapshot boundary, or zero when no snapshot is published.</param>
        /// <param name="walk">The index the walk started from and the byte offset after the last valid frame.</param>
        /// <param name="cancellationToken">A token to observe while persisting metadata.</param>
        /// <returns>A task that completes when the log state, the applied watermark, and the retained payloads are consistent.</returns>
        internal static async Task CompleteWalkAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            ulong snapshotBase,
            (ulong BaseIndex, long LastValidEnd) walk,
            CancellationToken cancellationToken)
        {
            owner.LogLength = walk.LastValidEnd;
            EnsureCommittedPrefixCovered(owner, snapshotBase);

            // Only a frame the walk found above its base counts as held by the log.
            await RaiseAppliedToBoundaryAsync(journal, owner, snapshotBase, owner.LastLogIndex > walk.BaseIndex ? owner.LastLogIndex : 0UL, cancellationToken)
               .ConfigureAwait(false);

            // Releases the applied entry payloads from memory, retaining their durable frame offsets.
            journal.ReleaseAppliedEntries(owner.Meta.LastAppliedIndex);
        }

        /// <summary>Resets the log state to the snapshot boundary, checks the committed prefix, and raises applied as for a log without frames.</summary>
        /// <param name="journal">The paired in-memory journal state.</param>
        /// <param name="owner">The log being recovered.</param>
        /// <param name="snapshotBase">The restored snapshot boundary, or zero when no snapshot is published.</param>
        /// <param name="cancellationToken">A token to observe while persisting metadata.</param>
        /// <returns>A task that completes when the state is reset.</returns>
        internal static async Task ResetAndEnsureAsync(FollowerLogJournal journal, IFollowerLogContext owner, ulong snapshotBase, CancellationToken cancellationToken)
        {
            ResetLogState(owner, snapshotBase);
            EnsureCommittedPrefixCovered(owner, snapshotBase);

            // The log holds no frame, so nothing above the snapshot base is left to apply.
            await RaiseAppliedToBoundaryAsync(journal, owner, snapshotBase, 0UL, cancellationToken).ConfigureAwait(false);
        }

        private static void EnsureCommittedPrefixCovered(IFollowerLogContext owner, ulong snapshotBase)
        {
            // Indexes at or below the restored snapshot baseline stay covered by the published snapshot even when
            // the walked journal ends below them: a crash between snapshot publication and the installation log
            // rewrite leaves the previous, shorter log in place under already-advanced watermarks. The next
            // replication round rebuilds the discarded span from the leader.
            if (owner.Meta.CommitIndex <= Math.Max(owner.LastLogIndex, snapshotBase))
                return;
            owner.Readiness = FollowerLogReadiness.Failed;
            throw new InvalidDataException($"Replica group '{owner.GroupId}' commit index exceeds the durable log.");
        }

        /// <summary>Raises the applied watermark to the snapshot boundary unless the log holds a frame at or above the boundary.</summary>
        /// <remarks>
        /// The applied watermark is owned by AdvanceAppliedAsync, not by the snapshot boundary. A snapshot carries only idempotency and commit
        /// state, not the application payload, so a frame the log holds at or above the boundary keeps the watermark where it is and is applied
        /// on catch-up. Frames below the boundary are covered by the snapshot and are skipped. The boundary is compared against the highest
        /// frame found in the log file. A retried open reads the metadata from the
        /// disk again, so this step is safe to repeat.
        /// </remarks>
        /// <param name="journal">The paired in-memory journal state.</param>
        /// <param name="owner">The log being recovered.</param>
        /// <param name="snapshotBase">The restored snapshot boundary, or zero when no snapshot is published.</param>
        /// <param name="highestFrameIndex">The highest frame index found in the log file, or zero when it holds no frame.</param>
        /// <param name="cancellationToken">A token to observe while persisting metadata.</param>
        private static async Task RaiseAppliedToBoundaryAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            ulong snapshotBase,
            ulong highestFrameIndex,
            CancellationToken cancellationToken)
        {
            if (snapshotBase <= owner.Meta.LastAppliedIndex || snapshotBase <= highestFrameIndex)
                return;

            var candidate = owner.Meta with { LastAppliedIndex = snapshotBase };
            await FollowerLogAppend.PersistMetaOrFailReadinessAsync(journal, owner, candidate, cancellationToken).ConfigureAwait(false);
            owner.Meta = candidate;
        }

        private static void ResetLogState(IFollowerLogContext owner, ulong snapshotBase)
        {
            owner.LogLength = GroupLogSlots.HeaderSize;
            owner.LastLogIndex = snapshotBase;
        }
    }

    /// <summary>Snapshot creation, installation, and compaction for a follower log.</summary>
    private static class FollowerLogSnapshot
    {
        /// <summary>Publishes a snapshot, failing the readiness of the log when the store cannot tell whether the new snapshot became durable.</summary>
        /// <param name="journal">The paired in-memory journal state.</param>
        /// <param name="owner">The owning log, whose readiness fails.</param>
        /// <param name="snapshot">The snapshot to publish.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>A task that completes when the snapshot is durably published.</returns>
        /// <remarks>
        /// A snapshot too large, or a cancellation, leaves the previous snapshot in force and is rethrown as is. Any other failure may have left the new
        /// snapshot durable while the in-memory baseline trails it, so the log is never surfaced as ready afterwards.
        /// </remarks>
        internal static async Task PublishOrFailReadinessAsync(FollowerLogJournal journal, IFollowerLogContext owner, GroupSnapshot snapshot, CancellationToken cancellationToken)
        {
            try
            {
                await journal.Snapshot.PublishAsync(snapshot, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not (GroupSnapshotTooLargeException or OperationCanceledException))
            {
                owner.Readiness = FollowerLogReadiness.Failed;
                throw;
            }
        }

        /// <summary>Publishes a snapshot through <paramref name="index" /> and compacts the log prefix it covers.</summary>
        /// <param name="journal">The paired in-memory journal state.</param>
        /// <param name="owner">The log being compacted.</param>
        /// <param name="index">The index to compact through.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The compaction outcome.</returns>
        /// <remarks>
        /// Runs under the log gate. Every refusal happens before anything durable changes and leaves readiness untouched: only a prefix
        /// that is committed and applied is compacted, and never while an entry of it still has an unresolved idempotency outcome, which
        /// the snapshot cannot carry. The index is the durable applied index; the commit index may be higher, and every frame above the
        /// index, committed or not, stays in the log as the retained tail, which must hold every index through the last one. The snapshot
        /// is published before the compaction core rewrites the log, so a crash between the two recovers from the snapshot plus the old log.
        /// </remarks>
        internal static async Task<GroupCompactionOutcome> CompactThroughAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            ulong index,
            CancellationToken cancellationToken)
        {
            var eligible = index != 0UL && index == owner.Meta.LastAppliedIndex && index <= owner.Meta.CommitIndex && index <= owner.LastLogIndex &&
                           index >= journal.SnapshotBaseline.LastIncludedIndex;
            if (!eligible)
                return GroupCompactionOutcome.NotReady;

            if (owner.Idempotency.HasUnresolvedThrough(index))
                return GroupCompactionOutcome.UnresolvedOutcome;

            // A retained tail missing an index cannot be rewritten without leaving a gap, so it is refused before the snapshot is published.
            var tail = CollectRetainedTail(journal, index);
            if (ulong.CreateChecked(tail.Count) != owner.LastLogIndex - index)
                return GroupCompactionOutcome.NotReady;

            var snapshot = BuildSnapshot(journal, owner, index);

            try
            {
                await PublishOrFailReadinessAsync(journal, owner, snapshot, cancellationToken).ConfigureAwait(false);
            }
            catch (GroupSnapshotTooLargeException)
            {
                return GroupCompactionOutcome.SnapshotTooLarge;
            }

            owner.RestoreBaseline(new SnapshotBaseline(snapshot.LastIncludedIndex, snapshot.LastIncludedTerm));
            var compacted = await CompactAsync(journal, owner, cancellationToken).ConfigureAwait(false);
            return compacted.Success ? GroupCompactionOutcome.Compacted : GroupCompactionOutcome.NotReady;
        }

        /// <summary>
        /// Returns the topology-mismatch refusal when the snapshot's topology fingerprint or configuration generation
        /// conflicts with the durable metadata; an empty durable fingerprint adopts the snapshot's fingerprint and never
        /// conflicts. Returns <see langword="null" /> when the snapshot is compatible.
        /// </summary>
        /// <param name="owner">They log the snapshot targets.</param>
        /// <param name="snapshot">The snapshot to validate.</param>
        /// <returns>The <see cref="FollowerLogRefusal.TopologyMismatch" /> marker, or <see langword="null" />.</returns>
        internal static string? SnapshotTopologyMismatch(IFollowerLogContext owner, in GroupSnapshot snapshot)
        {
            var memory = owner.Meta.TopologyFingerprint;
            return (!memory.IsEmpty && !memory.Span.SequenceEqual(snapshot.TopologyFingerprint.Span)) || snapshot.ConfigurationGeneration < owner.Meta.ConfigurationGeneration
                ? FollowerLogRefusal.TopologyMismatch
                : null;
        }

        internal static GroupSnapshot BuildSnapshot(FollowerLogJournal journal, IFollowerLogContext owner, ulong lastIncludedIndex)
        {
            if (!journal.EntryOffsets.TryGetValue(lastIncludedIndex, out var location))
                return BuildSnapshotFromBaseline(journal, owner, lastIncludedIndex);

            // The two adjacent ulong arguments are LastIncludedIndex and CommitIndex, in declaration order; both
            // equal the snapshot boundary for a freshly created snapshot.
            var outcomes = ExportCoveredOutcomes(owner, lastIncludedIndex, out var capturedUtc);
            return new GroupSnapshot(
                owner.GroupId,
                owner.Meta.TopologyFingerprint,
                owner.Meta.ConfigurationGeneration,
                location.Term,
                lastIncludedIndex,
                lastIncludedIndex,
                outcomes,
                capturedUtc);
        }

        private static GroupSnapshot BuildSnapshotFromBaseline(FollowerLogJournal journal, IFollowerLogContext owner, ulong lastIncludedIndex)
        {
            // The covered index may be the snapshot base itself, whose frame was already compacted away.
            if (journal.SnapshotBaseline.LastIncludedIndex == 0UL || lastIncludedIndex != journal.SnapshotBaseline.LastIncludedIndex)
                throw new InvalidOperationException($"Replica group '{owner.GroupId}' cannot snapshot from a missing index '{lastIncludedIndex}'.");

            var outcomes = ExportCoveredOutcomes(owner, lastIncludedIndex, out var capturedUtc);
            return new GroupSnapshot(
                owner.GroupId,
                owner.Meta.TopologyFingerprint,
                owner.Meta.ConfigurationGeneration,
                journal.SnapshotBaseline.LastIncludedTerm,
                lastIncludedIndex,
                lastIncludedIndex,
                outcomes,
                capturedUtc);
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

        private static async Task<GroupCompactionResult> CompactAsync(FollowerLogJournal journal, IFollowerLogContext owner, CancellationToken cancellationToken)
        {
            if (!journal.Snapshot.SnapshotExists)
                return new GroupCompactionResult(false, null, FollowerLogRefusal.NotReady);

            var snapshot = await journal.Snapshot.ReadPublishedAsync(cancellationToken).ConfigureAwait(false);
            if (snapshot == null)
                return new GroupCompactionResult(false, null, FollowerLogRefusal.NotReady);

            var snapshotValue = snapshot.Value;
            if (RefuseCompactionSnapshot(owner, journal, in snapshotValue) is { } refusal)
                return new GroupCompactionResult(false, null, refusal);

            var included = snapshotValue.LastIncludedIndex;
            var tail = CollectRetainedTail(journal, included);

            // Every frame above the boundary must be rewritten: a missing index would leave a gap, committed entries included, that
            // recovery refuses. Nothing durable changed yet beyond the published snapshot, which recovers on its own.
            if (ulong.CreateChecked(tail.Count) != owner.LastLogIndex - included)
                return new GroupCompactionResult(false, null, FollowerLogRefusal.NotReady);

            var retainedLogIndexes = CollectRetainedLogIndexes(tail);
            var (length, offsets) = await ReplaceRewrittenTailAsync(journal, owner, tail, cancellationToken).ConfigureAwait(false);
            ReindexTail(journal, tail, offsets);

            owner.LastLogIndex = tail.Count == 0 ? included : tail[^1].LogIndex;
            owner.LogLength = length;

            // ReindexTail already left only entries above the boundary, so the paired advance prunes nothing.
            journal.AdvanceBaseline(new SnapshotBaseline(snapshotValue.LastIncludedIndex, snapshotValue.LastIncludedTerm));

            RestoreCompactionIdempotency(owner, in snapshotValue, retainedLogIndexes);
            return new GroupCompactionResult(true, journal.Snapshot.SnapshotPath, string.Empty);
        }

        private static string? RefuseCompactionSnapshot(IFollowerLogContext owner, FollowerLogJournal journal, in GroupSnapshot snapshot)
        {
            return RefuseWatermarkMismatch(owner, snapshot.LastIncludedIndex)
                ?? RefuseIdentityMismatch(owner, in snapshot)
                ?? RefuseTermMismatch(journal, snapshot.LastIncludedIndex, in snapshot);
        }

        private static string? RefuseWatermarkMismatch(IFollowerLogContext owner, ulong included)
        {
            // The durable prefix is dropped only through a committed index the published snapshot covers; committed entries above it stay
            // in the retained tail.
            // Frames in (included, LastAppliedIndex] were released from `Entries` by PruneAppliedEntries, so the
            // retained tail cannot reproduce them. Compacting here would drop committed frames the snapshot does
            // not cover and leave a committed gap for recovery.
            return included == 0UL || included > owner.Meta.CommitIndex || included > owner.LastLogIndex || included > owner.Meta.LastAppliedIndex || included < owner.Meta.LastAppliedIndex
                ? FollowerLogRefusal.NotReady
                : null;
        }

        private static string? RefuseIdentityMismatch(IFollowerLogContext owner, in GroupSnapshot snapshot)
        {
            // Validate the published snapshot's identity and topology against the durable local state before any
            // destructive rewrite. A snapshot that diverges from the replica must not be used to truncate the durable
            // prefix; the suffix must instead survive as a replicable tail.
            return !string.Equals(snapshot.GroupId, owner.GroupId, StringComparison.Ordinal)
                ? FollowerLogRefusal.NotMember
                : SnapshotTopologyMismatch(owner, in snapshot);
        }

        private static string? RefuseTermMismatch(FollowerLogJournal journal, ulong included, in GroupSnapshot snapshot)
        {
            return journal.EntryOffsets.TryGetValue(included, out var boundary) && boundary.Term != snapshot.LastIncludedTerm
                ? FollowerLogRefusal.LogMismatch
                : null;
        }

        private static async Task<(int Length, List<long> Offsets)> ReplaceRewrittenTailAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            List<FollowerLogEntry> tail,
            CancellationToken cancellationToken)
        {
            try
            {
                return await FollowerLogDurable.ReplaceLogAsync(journal, owner, tail, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                owner.Readiness = FollowerLogReadiness.Failed;
                throw;
            }
        }

        private static void RestoreCompactionIdempotency(IFollowerLogContext owner, in GroupSnapshot snapshot, List<ulong> retainedLogIndexes)
        {
            // The discarded prefix is now owned by the snapshot, which exports only resolved outcomes; any remaining
            // record at or below the included index has lost its durable journal frame and must be released, while records
            // carried by the retained tail stay authoritative. The restore is never refused for the idempotency capacity,
            // so outcomes recorded while the snapshot was published cannot fail the log here.
            try
            {
                owner.Idempotency.RestoreFromSnapshot(snapshot.CommittedOutcomes, snapshot.CapturedUtc, retainedLogIndexes);
            }
            catch
            {
                // A failed idempotency restore leaves the in-memory map holding discarded-prefix records whose
                // journal frames no longer exist; mark the log failed so it is never surfaced as Ready.
                owner.Readiness = FollowerLogReadiness.Failed;
                throw;
            }
        }

        private static List<GroupIdempotencyRecord> ExportCoveredOutcomes(IFollowerLogContext owner, ulong lastIncludedIndex, out DateTime capturedUtc)
        {
            var outcomes = new List<GroupIdempotencyRecord>();
            foreach (var record in owner.Idempotency.ExportResolved(out capturedUtc))
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
    }

    /// <summary>Operations of a state transfer: marks a copy incomplete, completes it at a snapshot, and captures the snapshot a leader sends.</summary>
    /// <remarks>
    /// <para>
    /// The caller holds the log gate. Every metadata record of a transfer is published to both slots through the mirrored path, like the member
    /// state changes of the first start, so damage to one slot cannot roll the mark back. Recovery never writes the member state: it keeps whatever
    /// the newest valid record holds.
    /// </para>
    /// <para>
    /// An unseeded copy, which lost its disk, takes a transfer only from a leader that checked the term ceiling (#1126). It stays unseeded, and
    /// votes for nobody, for the whole transfer: the begin writes nothing. The completion writes, in its one complete record, the leader's term
    /// and a vote for the leader, so the copy cannot vote a second time in a term it may have voted in before the loss. A transfer cut short
    /// leaves the copy unseeded.
    /// </para>
    /// </remarks>
    private static class FollowerLogStateInstall
    {
        internal static async Task<string?> BeginAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            bool open,
            bool topologyMismatch,
            (StateInstallLeader Leader, ulong Index, GroupSnapshot Snapshot) transfer,
            CancellationToken cancellationToken)
        {
            // The snapshot is checked before the copy is marked, so one the completion would refuse costs the copy nothing.
            var meta = owner.Meta;
            var refusal = (open, topologyMismatch) switch
            {
                (false, _) => FollowerLogRefusal.NotReady,
                (true, true) => FollowerLogRefusal.TopologyMismatch,
                _ => RefuseTransfer(in meta, in transfer.Leader, transfer.Index) ?? RefuseSnapshot(journal, owner, in transfer.Snapshot),
            };
            if (refusal != null || meta.MemberState == GroupMemberState.Unseeded)
                return refusal;

            // A higher leader term is adopted with the vote cleared, any other term keeps the term and the vote.
            var leaderTerm = transfer.Leader.Term;
            var candidate = leaderTerm > meta.CurrentTerm ? meta with { CurrentTerm = leaderTerm, VotedFor = string.Empty } : meta;
            await StoreAsync(journal, owner, candidate with { MemberState = GroupMemberState.Incomplete }, cancellationToken).ConfigureAwait(false);
            return null;
        }

        internal static GroupSnapshotCapture Capture(FollowerLogJournal journal, IFollowerLogContext owner, bool open)
        {
            var index = owner.Meta.LastAppliedIndex;
            var outcome = (open, owner.Meta.MemberState == GroupMemberState.Complete, index == 0UL) switch
            {
                (false, _, _) => GroupSnapshotCaptureOutcome.NotReady,
                (true, false, _) => GroupSnapshotCaptureOutcome.NotComplete,
                (true, true, true) => GroupSnapshotCaptureOutcome.NothingApplied,
                _ when owner.Idempotency.HasUnresolvedThrough(index) => GroupSnapshotCaptureOutcome.UnresolvedOutcome,
                _ => GroupSnapshotCaptureOutcome.Captured,
            };
            return outcome == GroupSnapshotCaptureOutcome.Captured
                ? new GroupSnapshotCapture(outcome, FollowerLogSnapshot.BuildSnapshot(journal, owner, index))
                : GroupSnapshotCapture.Refused(outcome);
        }

        internal static async Task<GroupSnapshotInstallResult> CompleteAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            bool open,
            GroupSnapshot snapshot,
            StateInstallLeader leader,
            CancellationToken cancellationToken)
        {
            var refusal = open ? RefuseCompletion(owner, in snapshot, in leader) : FollowerLogRefusal.NotReady;
            if (refusal != null)
                return GroupSnapshotInstallResult.Refused(refusal);

            // The snapshot is published first. If that fails before its first slot is flushed, the old snapshot, metadata and log remain authoritative;
            // a failure after that point may leave the new snapshot published, and a failure of any later step leaves the copy incomplete, or unseeded,
            // so recovery restores the baseline from the snapshot on the next open.
            await FollowerLogSnapshot.PublishOrFailReadinessAsync(journal, owner, snapshot, cancellationToken).ConfigureAwait(false);
            await ReplaceStateAsync(journal, owner, snapshot, leader).ConfigureAwait(false);
            return GroupSnapshotInstallResult.Installed;
        }

        private static async Task ReplaceStateAsync(FollowerLogJournal journal, IFollowerLogContext owner, GroupSnapshot snapshot, StateInstallLeader leader)
        {
            try
            {
                // Past the publish there is no way back: a caller's cancellation must not fail the log half way through.
                await ReplaceLogAndStoreAsync(journal, owner, snapshot, leader, CancellationToken.None).ConfigureAwait(false);
            }
            catch
            {
                // The in-memory state may trail the durable one, so the log is never surfaced as ready after a failed step.
                owner.Readiness = FollowerLogReadiness.Failed;
                throw;
            }
        }

        private static async Task ReplaceLogAndStoreAsync(
            FollowerLogJournal journal,
            IFollowerLogContext owner,
            GroupSnapshot snapshot,
            StateInstallLeader leader,
            CancellationToken cancellationToken)
        {
            // Both restores precede the log replacement, so a fault during it cannot leave the in-memory map holding entries the transfer
            // discarded. The restore keeps every committed outcome whatever the idempotency capacity, which bounds new reservations only.
            owner.RestoreBaseline(new SnapshotBaseline(snapshot.LastIncludedIndex, snapshot.LastIncludedTerm));
            owner.Idempotency.RestoreFromSnapshot(snapshot.CommittedOutcomes, snapshot.CapturedUtc, []);

            // The log holds nothing above the baseline: the transferred state is the committed prefix, and the leader sends what follows.
            var (length, _) = await FollowerLogDurable.ReplaceLogAsync(journal, owner, [], cancellationToken).ConfigureAwait(false);
            journal.ClearEntries();
            owner.LastLogIndex = snapshot.LastIncludedIndex;
            owner.LogLength = length;

            // The complete record is the last step. A higher leader term is adopted with it, with the vote cleared; a copy that was unseeded
            // takes the leader's term with a vote for the leader, since it may have voted in that term before it lost its disk.
            var meta = owner.Meta;
            var adopted = AdoptedTerm(in meta, in leader);
            var complete = adopted with
            {
                MemberState = GroupMemberState.Complete,
                CommitIndex = snapshot.LastIncludedIndex,
                LastAppliedIndex = snapshot.LastIncludedIndex,
                TopologyFingerprint = meta.TopologyFingerprint.IsEmpty ? snapshot.TopologyFingerprint : meta.TopologyFingerprint,
                ConfigurationGeneration = Math.Max(meta.ConfigurationGeneration, snapshot.ConfigurationGeneration),
            };
            await StoreAsync(journal, owner, complete, cancellationToken).ConfigureAwait(false);
        }

        private static GroupLogMetadata AdoptedTerm(in GroupLogMetadata meta, in StateInstallLeader leader) => true switch
        {
            _ when meta.MemberState == GroupMemberState.Unseeded => meta with { CurrentTerm = leader.Term, VotedFor = leader.NodeId },
            _ when leader.Term > meta.CurrentTerm => meta with { CurrentTerm = leader.Term, VotedFor = string.Empty },
            _ => meta,
        };

        private static string? RefuseTransfer(in GroupLogMetadata meta, in StateInstallLeader leader, ulong index)
        {
            return true switch
            {
                _ when leader.Term < meta.CurrentTerm => FollowerLogRefusal.StaleTerm,
                _ when meta.MemberState == GroupMemberState.Unseeded && !leader.CeilingChecked => FollowerLogRefusal.Unseeded,
                _ when index < meta.CommitIndex => FollowerLogRefusal.NotReady,
                _ => null,
            };
        }

        private static string? RefuseSnapshot(FollowerLogJournal journal, IFollowerLogContext owner, in GroupSnapshot snapshot)
        {
            var topologyMismatch = FollowerLogSnapshot.SnapshotTopologyMismatch(owner, in snapshot);
            return true switch
            {
                _ when !string.Equals(snapshot.GroupId, owner.GroupId, StringComparison.Ordinal) => FollowerLogRefusal.NotMember,
                _ when topologyMismatch != null => topologyMismatch,
                _ when IsMalformed(owner, in snapshot) || !journal.Snapshot.Fits(snapshot) => FollowerLogRefusal.NotReady,
                _ => null,
            };
        }

        private static string? RefuseCompletion(IFollowerLogContext owner, in GroupSnapshot snapshot, in StateInstallLeader leader)
        {
            var topologyMismatch = FollowerLogSnapshot.SnapshotTopologyMismatch(owner, in snapshot);
            var state = owner.Meta.MemberState;
            return true switch
            {
                _ when state == GroupMemberState.Unseeded && !leader.CeilingChecked => FollowerLogRefusal.Unseeded,
                _ when state == GroupMemberState.Complete => FollowerLogRefusal.NotReady,
                _ when !string.Equals(snapshot.GroupId, owner.GroupId, StringComparison.Ordinal) => FollowerLogRefusal.NotMember,
                _ when topologyMismatch != null => topologyMismatch,
                _ when leader.Term < owner.Meta.CurrentTerm => FollowerLogRefusal.StaleTerm,
                _ when IsMalformed(owner, in snapshot) => FollowerLogRefusal.NotReady,
                _ => null,
            };
        }

        private static bool IsMalformed(IFollowerLogContext owner, in GroupSnapshot snapshot)
        {
            // Terms start at 1, and a zero baseline collides with the "unverifiable term" sentinel and would make recovery discard the whole
            // durable suffix. The transferred state is authoritative for the covered prefix, so one below the durable commit index is refused:
            // durable watermarks never move back. A commit index below the included index is internally inconsistent.
            var baselineInvalid = snapshot.LastIncludedIndex == 0UL || snapshot.LastIncludedTerm == 0UL || snapshot.CommitIndex < snapshot.LastIncludedIndex;
            var belowCommit = snapshot.LastIncludedIndex < owner.Meta.CommitIndex;

            // A snapshot with an unresolved outcome would write invalid idempotency state to disk and poison the next recovery, and so would
            // an outcome beyond the boundary, which a direct snapshot bypassing the on-disk decoder can carry. Both are refused before anything
            // durable is written.
            return baselineInvalid || belowCommit || HasUnresolvedOutcome(snapshot.CommittedOutcomes) || HasOutcomeBeyond(snapshot.CommittedOutcomes, snapshot.LastIncludedIndex);
        }

        private static bool HasOutcomeBeyond(IReadOnlyList<GroupIdempotencyRecord> outcomes, ulong lastIncludedIndex)
        {
            for (var i = 0; i < outcomes.Count; i++)
            {
                if (outcomes[i].LogIndex > lastIncludedIndex)
                    return true;
            }

            return false;
        }

        private static bool HasUnresolvedOutcome(IReadOnlyList<GroupIdempotencyRecord> outcomes)
        {
            for (var i = 0; i < outcomes.Count; i++)
            {
                if (outcomes[i].ResolvedUtc == null)
                    return true;
            }

            return false;
        }

        private static async Task StoreAsync(FollowerLogJournal journal, IFollowerLogContext owner, GroupLogMetadata candidate, CancellationToken cancellationToken)
        {
            await FollowerLogAppend.PersistMirroredMetaOrFailReadinessAsync(journal, owner, candidate, cancellationToken).ConfigureAwait(false);
            owner.Meta = candidate;
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
