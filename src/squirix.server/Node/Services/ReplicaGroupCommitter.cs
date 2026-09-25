using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Threading;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Node.Services;

/// <summary>Serialized owner-side replicated commits for the group owned by this node.</summary>
/// <remarks>
/// Commits run one at a time per group under <see cref="AsyncLock" />: log indexes stay dense with no
/// gaps, prepare-time reads stay exact through the ordered applying, and the coordinator never observes
/// admission pressure or turn waits. The coordinator itself is never canceled; a fixed commit budget
/// bounds every attempt up to its durable majority, and idempotent retries recover unknown outcomes.
/// </remarks>
internal sealed class ReplicaGroupCommitter : IAsyncDisposable
{
    private const int IdempotencyCapacity = 1024;
    private const int MaxInFlight = 2;
    private const string PendingApplyRefusalReason = "replica_apply_pending";
    private static readonly TimeSpan CommitTimeout = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan DefaultShutdownBudget = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan ProbeTimeout = TimeSpan.FromSeconds(1);

    private readonly AsyncLock _gate = new();
    private readonly IReplicaRpcGateway _gateway;
    private readonly ulong _generation;
    private readonly ILogicalNamespacedCache<object?> _local;
    private readonly IReplicaGroupLocator _locator;
    private readonly ReplicaGroupRegistry _registry;
    private readonly string _selfId;
    private readonly ReadOnlyMemory<byte> _topologyFingerprint;
    private ReplicaCommitCoordinator? _coordinator;
    private int _disposed;
    private ReplicaMutationFactory? _factory;
    private ulong _nextIndex;
    private bool _started;

    /// <summary>Initializes a new instance of the <see cref="ReplicaGroupCommitter" /> class.</summary>
    /// <param name="registry">Replica group registry of this node.</param>
    /// <param name="locator">Replica group locator resolving the owned group members.</param>
    /// <param name="gateway">Follower replication RPCs.</param>
    /// <param name="local">Local cache pipeline used for prepare reads and memory applies.</param>
    /// <param name="selfId">This node identifier; the node owns the group with this identifier.</param>
    /// <param name="topologyFingerprint">Static topology fingerprint.</param>
    /// <param name="generation">Static configuration generation.</param>
    internal ReplicaGroupCommitter(
        ReplicaGroupRegistry registry,
        IReplicaGroupLocator locator,
        IReplicaRpcGateway gateway,
        ILogicalNamespacedCache<object?> local,
        string selfId,
        ReadOnlyMemory<byte> topologyFingerprint,
        ulong generation)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(locator);
        ArgumentNullException.ThrowIfNull(gateway);
        ArgumentNullException.ThrowIfNull(local);
        ArgumentException.ThrowIfNullOrWhiteSpace(selfId);
        _registry = registry;
        _locator = locator;
        _gateway = gateway;
        _local = local;
        _selfId = selfId;
        _topologyFingerprint = topologyFingerprint.IsEmpty ? throw new ArgumentException("Topology fingerprint must not be empty.", nameof(topologyFingerprint))
            : topologyFingerprint;
        _generation = generation;
        ShutdownBudget = DefaultShutdownBudget;
    }

    /// <summary>Gets the logger for lifecycle failures; the host logger unless set.</summary>
    internal ILogger Log { get; init; } = LogManager.GetLogger<ReplicaGroupCommitter>();

    /// <summary>Gets the longest wait for an in-flight commit on dispose; 30 seconds unless set.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The budget is not positive.</exception>
    internal TimeSpan ShutdownBudget
    {
        get;
        init
        {
            value.ThrowIfNegativeOrZero(nameof(value), "The shutdown budget must be greater than zero.");

            field = value;
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        // Drain in-flight committer operations holding _gate so their AsyncLockHolder can release
        // the gate before it is disposed of. New admissions fail closed via ThrowIfDisposed.
        // Work after a durable majority ignores cancellation and can outlast a stalled disk, so the
        // drain is bounded: on expiry the coordinator and the gate stay with the in-flight commit and
        // are leaked loudly instead of being torn down under it. Not throwing keeps the host disposing
        // the services behind this one (the group logs and the journal).
        AsyncLockHolder drain;
        using (var budget = new CancellationTokenSource(ShutdownBudget))
        {
            try
            {
                drain = await _gate.LockAsync(budget.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                LogManager.ReplicaCommitterLeakedOnShutdownTimeout(Log, ShutdownBudget);
                return;
            }
        }

        drain.Dispose();

        if (_coordinator != null)
            await _coordinator.DisposeAsync().ConfigureAwait(false);
        _gate.Dispose();
    }

    /// <summary>Commits a replicated remove and returns the removed entry, if any.</summary>
    /// <param name="operationId">Client operation identifier.</param>
    /// <param name="cacheName">Target cache name.</param>
    /// <param name="key">Target key.</param>
    /// <param name="cancellationToken">Cancellation token for queueing only; the commit itself is budget-bounded.</param>
    /// <returns>The remove outcome with the previous value when one was observed.</returns>
    internal async Task<CacheRemoveResult<object?>> CommitRemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        using var guard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        var (coordinator, factory) = await EnsureStartedAsync(true, cancellationToken).ConfigureAwait(false);
        var index = PeekNextIndex();
        var mutation = await factory.PrepareRemoveAsync(operationId, cacheName, key, index, cancellationToken).ConfigureAwait(false);
        AdvanceNextIndex();
        var outcome = await CommitWithPreAppendResyncAsync(coordinator, mutation).ConfigureAwait(false);
        return await DecodeRemoveAsync(outcome).ConfigureAwait(false);
    }

    /// <summary>Commits a replicated expiration removal.</summary>
    /// <param name="operationId">Client operation identifier.</param>
    /// <param name="cacheName">Target cache name.</param>
    /// <param name="key">Target key.</param>
    /// <param name="cancellationToken">Cancellation token for queueing only; the commit itself is budget-bounded.</param>
    /// <returns><see langword="true" /> when an expiration was present and cleared.</returns>
    internal async Task<bool> CommitRemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        using var guard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        var (coordinator, factory) = await EnsureStartedAsync(true, cancellationToken).ConfigureAwait(false);
        var index = PeekNextIndex();
        var mutation = await factory.PrepareRemoveExpirationAsync(operationId, cacheName, key, index, cancellationToken).ConfigureAwait(false);
        AdvanceNextIndex();
        var outcome = await CommitWithPreAppendResyncAsync(coordinator, mutation).ConfigureAwait(false);
        return DecodeApplied(outcome);
    }

    /// <summary>Commits a replicated unconditional writing.</summary>
    /// <param name="operationId">Client operation identifier.</param>
    /// <param name="cacheName">Target cache name.</param>
    /// <param name="key">Target key.</param>
    /// <param name="entry">Entry to write.</param>
    /// <param name="cancellationToken">Cancellation token for queueing only; the commit itself is budget-bounded.</param>
    /// <returns>A task that completes after the commit.</returns>
    internal async Task CommitSetAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        using var guard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        var (coordinator, factory) = await EnsureStartedAsync(true, cancellationToken).ConfigureAwait(false);
        var index = PeekNextIndex();
        var mutation = factory.PrepareSet(operationId, cacheName, key, entry, index);
        AdvanceNextIndex();
        _ = await CommitWithPreAppendResyncAsync(coordinator, mutation).ConfigureAwait(false);
    }

    /// <summary>Commits a replicated conditional expiration refresh.</summary>
    /// <param name="operationId">Client operation identifier.</param>
    /// <param name="cacheName">Target cache name.</param>
    /// <param name="key">Target key.</param>
    /// <param name="expiration">New expiration.</param>
    /// <param name="cancellationToken">Cancellation token for queueing only; the commit itself is budget-bounded.</param>
    /// <returns><see langword="true" /> when the key exists and the expiration was refreshed.</returns>
    internal async Task<bool> CommitTouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        using var guard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        var (coordinator, factory) = await EnsureStartedAsync(true, cancellationToken).ConfigureAwait(false);
        var index = PeekNextIndex();
        var mutation = await factory.PrepareTouchAsync(operationId, cacheName, key, expiration, index, cancellationToken).ConfigureAwait(false);
        AdvanceNextIndex();
        var outcome = await CommitWithPreAppendResyncAsync(coordinator, mutation).ConfigureAwait(false);
        return DecodeApplied(outcome);
    }

    /// <summary>Commits a replicated conditional adding and returns whether the key was absent.</summary>
    /// <param name="operationId">Client operation identifier.</param>
    /// <param name="cacheName">Target cache name.</param>
    /// <param name="key">Target key.</param>
    /// <param name="entry">Entry to add when absent.</param>
    /// <param name="cancellationToken">Cancellation token for queueing only; the commit itself is budget-bounded.</param>
    /// <returns><see langword="true" /> when the key was absent and the entry was added.</returns>
    internal async Task<bool> CommitTryAddAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        using var guard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        var (coordinator, factory) = await EnsureStartedAsync(true, cancellationToken).ConfigureAwait(false);
        var index = PeekNextIndex();
        var mutation = await factory.PrepareTryAddAsync(operationId, cacheName, key, entry, index, cancellationToken).ConfigureAwait(false);
        AdvanceNextIndex();
        var outcome = await CommitWithPreAppendResyncAsync(coordinator, mutation).ConfigureAwait(false);
        return DecodeApplied(outcome);
    }

    /// <summary>Commits a replicated value replacement.</summary>
    /// <param name="operationId">Client operation identifier.</param>
    /// <param name="cacheName">Target cache name.</param>
    /// <param name="key">Target key.</param>
    /// <param name="value">Replacement value.</param>
    /// <param name="cancellationToken">Cancellation token for queueing only; the commit itself is budget-bounded.</param>
    /// <returns><see langword="true" /> when the key exists and the value was replaced.</returns>
    internal async Task<bool> CommitUpdateAsync(string operationId, string cacheName, string key, object? value, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        using var guard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        var (coordinator, factory) = await EnsureStartedAsync(true, cancellationToken).ConfigureAwait(false);
        var index = PeekNextIndex();
        var mutation = await factory.PrepareUpdateAsync(operationId, cacheName, key, value, index, cancellationToken).ConfigureAwait(false);
        AdvanceNextIndex();
        var outcome = await CommitWithPreAppendResyncAsync(coordinator, mutation).ConfigureAwait(false);
        return DecodeApplied(outcome);
    }

    /// <summary>Verifies non-ready replica slots against the leader log so a restarted group regains its write quorum.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The verification state; <see cref="ReplicaVerification.Pending" /> while some follower is not yet verified.</returns>
    /// <remarks>
    /// Unreachable followers are probed without holding the commit gate, so a dead peer never delays writes. Only
    /// when some follower answered does the gate get taken to start the coordinator, re-check that the leader
    /// tail did not move, and admit the verified slots.
    /// </remarks>
    internal async Task<ReplicaVerification> VerifyReplicasAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        if (!_registry.TryGetLog(_selfId, out var log))
            return ReplicaVerification.Blocked;

        var eligibility = _registry.EligibilityFor(_selfId);
        var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        if (status.Readiness != FollowerLogReadiness.Ready || status.LastLogIndex != status.CommitIndex)
            return ReplicaVerification.Blocked;

        if (eligibility.AllCanCountInWriteQuorum())
            return ReplicaVerification.AllReady;

        var (members, header) = BuildMembership(Math.Max(1UL, status.CurrentTerm));
        var probed = await ReplicaReadinessProbe.ProbeAllAsync(_gateway, ReplicaReadinessProbe.NonReadyFollowers(eligibility), members, header, status, ProbeTimeout, cancellationToken).ConfigureAwait(false);
        var answered = new bool[probed.Length];
        var anyAnswered = false;
        for (var i = 1; i < probed.Length; i++)
        {
            answered[i] = probed[i].Kind == ReplicaProbeKind.Accepted || probed[i].Kind == ReplicaProbeKind.LogMismatch;
            anyAnswered |= answered[i];
        }

        if (!anyAnswered)
            return ReplicaVerification.Pending;

        using var guard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);

        // Entries still unapplied refuse writes and a resync: report that as blocked, like an uncommitted tail, instead of
        // failing the verification loop with the write-path refusal from the coordinator start.
        if (!await TryApplyPendingAsync().ConfigureAwait(false))
            return ReplicaVerification.Blocked;

        var (coordinator, _) = await EnsureStartedAsync(false, cancellationToken).ConfigureAwait(false);
        var current = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        if (current.LastLogIndex != current.CommitIndex)
            return ReplicaVerification.Blocked;

        // A commit may have moved the tail between the unguarded probe and the gate: the verdicts then describe
        // an older tail, so the slots that answered are probed again against the current one.
        if (current.LastLogIndex != status.LastLogIndex || current.LastLogTerm != status.LastLogTerm)
            probed = await ReplicaReadinessProbe.ProbeAllAsync(_gateway, answered, members, header, current, ProbeTimeout, cancellationToken).ConfigureAwait(false);

        // StartAsync may have verified some of these slots while this call waited for the gate: an older verdict
        // must not demote them.
        for (var i = 1; i < probed.Length; i++)
        {
            if (eligibility.CanCountInWriteQuorum(i))
                probed[i] = default;
        }

        ReplicaReadinessProbe.ApplyAll(eligibility, probed, in current, _topologyFingerprint, _generation, coordinator);
        return eligibility.AllCanCountInWriteQuorum() ? ReplicaVerification.AllReady : ReplicaVerification.Pending;
    }

    private static bool DecodeApplied(ReadOnlyMemory<byte> outcome)
    {
        const string message = "Committed outcome payload is malformed.";
        return !ReplicaOutcomeCodec.TryDecode(outcome, out var applied, out _) ? throw new InvalidOperationException(message) : applied;
    }

    private static async Task<CacheRemoveResult<object?>> DecodeRemoveAsync(ReadOnlyMemory<byte> outcome)
    {
        if (!ReplicaOutcomeCodec.TryDecode(outcome, out var removed, out var previous) || (removed && previous.IsEmpty))
            throw new InvalidOperationException("Committed remove outcome payload is malformed.");

        if (!removed)
            return new CacheRemoveResult<object?>(false, null);

        var entry = CacheEntryWire.Parser.ParseFrom(new ReadOnlySequence<byte>(previous));
        var mapped = await entry.MapFromProtoAsync<object?>().ConfigureAwait(false);
        return new CacheRemoveResult<object?>(true, mapped.Value);
    }

    private static bool IsPostAppendOutcome(Exception error) =>
        error is InvalidOperationException && error.Message.StartsWith(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);

    /// <summary>Consumes the next group log index after a mutation prepared successfully.</summary>
    /// <remarks>
    /// The index is advanced only once preparation succeeds, so a cancelled or failed prepare leaves the
    /// reservation for the retry and the durable log stays dense regardless of how the caller observed it.
    /// </remarks>
    private void AdvanceNextIndex() => _nextIndex++;

    private async ValueTask<ReadOnlyMemory<byte>> CommitWithPreAppendResyncAsync(ReplicaCommitCoordinator coordinator, PreparedReplicaMutation mutation)
    {
        try
        {
            return await coordinator.CommitAsync(mutation, CommitTimeout, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception error) when (IsPostAppendOutcome(error))
        {
            // A durable majority may hold the entry: keep the reservation and sequencing untouched and
            // report the stable contract (gRPC Unavailable with COMMIT_OUTCOME_UNKNOWN), so callers stop
            // instead of retrying under a new identity. The original cause is logged before it is dropped.
            LogManager.ReplicaCommitOutcomeUnknown(Log, error);
            throw ServerOpContract.CommitOutcomeUnknown();
        }
        catch
        {
            // The local appending was refused before anything was marked appended: the reserved
            // _nextIndex no longer matches the durable log, so drop the started state and rebuild
            // from status.LastLogIndex on the next attempt.
            _started = false;
            throw;
        }
    }

    private async Task<(ReplicaCommitCoordinator Coordinator, ReplicaMutationFactory Factory)> EnsureStartedAsync(bool requireWriteMajority, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        if (!_started)
            await StartAsync(cancellationToken).ConfigureAwait(false);

        if (requireWriteMajority && !HasWriteMajority())
        {
            // Refused before anything is appended: a write that cannot reach a majority would leave an uncommitted
            // local tail, which blocks verification and the coordinator start until it is reconciled. Dropping the
            // started state re-probes the followers on the next write.
            _started = false;
            throw new InvalidOperationException("Replica group has no verified write majority; the write was refused before the local append.");
        }

        // Outcomes are prepared from live memory, so an entry that is appended but not yet applied would make the prepared
        // outcome disagree with the log-order apply. Refused before the prepare and the local append, the write fails
        // definitely and may be retried; only this gate appends, so the check cannot go stale before the prepare.
        var applied = !requireWriteMajority || await TryApplyPendingAsync().ConfigureAwait(false);
        return (applied, _coordinator, _factory) switch
        {
            (false, _, _) => throw ServerOpContract.TooManyRequests(PendingApplyRefusalReason),
            (true, { } coordinator, { } factory) => (coordinator, factory),
            _ => throw new InvalidOperationException("Replica group committer is not started."),
        };
    }

    private bool HasWriteMajority()
    {
        var eligibility = _registry.EligibilityFor(_selfId);
        var ready = 0;
        for (var i = 0; i < eligibility.ReplicaCount; i++)
        {
            if (eligibility.CanCountInWriteQuorum(i))
                ready++;
        }

        return ready >= (eligibility.ReplicaCount / 2) + 1;
    }

    /// <summary>Returns the next group log index to prepare with, without consuming it.</summary>
    private ulong PeekNextIndex() => _nextIndex;

    private async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_registry.TryGetLog(_selfId, out var log))
            throw new InvalidOperationException($"This node does not serve its owned replica group '{_selfId}'.");

        if (_coordinator != null)
        {
            // The old coordinator's retained entries are not recovered by the new one (it starts from the durable log status, with
            // an empty apply queue), so disposing it with entries still unapplied would lose them from memory. Apply the committed
            // ones first; while any stays pending (the apply keeps failing, or no majority covers it yet) refuse the resync and
            // keep the old coordinator, whose late-majority path and the next attempt can still apply them.
            if (!await TryApplyPendingAsync().ConfigureAwait(false))
                throw ServerOpContract.TooManyRequests(PendingApplyRefusalReason);

            await _coordinator.DisposeAsync().ConfigureAwait(false);
        }

        var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        var term = Math.Max(1UL, status.CurrentTerm);
        var (members, header) = BuildMembership(term);

        // A restart with durable progress leaves every slot recovering. Verify the leader's own tail and every
        // follower against it before the first commit, so the quorum is built from verified slots only. A tail
        // that is not fully committed cannot start the coordinator below and is left untouched here.
        var eligibility = _registry.EligibilityFor(_selfId);
        ReplicaReadinessProbe.MarkLeaderReady(eligibility, in status, _topologyFingerprint, _generation);
        if (eligibility.CanCountInWriteQuorum(0))
        {
            var candidates = ReplicaReadinessProbe.NonReadyFollowers(eligibility);
            var results = await ReplicaReadinessProbe.ProbeAllAsync(_gateway, candidates, members, header, status, ProbeTimeout, cancellationToken).ConfigureAwait(false);
            ReplicaReadinessProbe.ApplyAll(eligibility, results, in status, _topologyFingerprint, _generation, null);
        }

        var pipeline = new ReplicaGroupCommitPipeline(_local, log, _gateway, members, _selfId, status, header);
        var idempotency = new GroupIdempotencyState(IdempotencyCapacity, GroupIdempotencyState.DefaultRetention);
        _coordinator = new ReplicaCommitCoordinator(
            new ReplicaCommitCoordinatorOptions(_locator.ReplicaCount, status.LastLogIndex, status.CommitIndex, MaxInFlight),
            pipeline,
            NoOpCommitHooks.Instance,
            idempotency,
            eligibility);
        _factory = new ReplicaMutationFactory(_local, _selfId, term);
        _nextIndex = status.LastLogIndex + 1;
        _started = true;
    }

    private (string[] Members, ReplicaRpcHeader Header) BuildMembership(ulong term)
    {
        var members = new string[_locator.ReplicaCount];
        _locator.GetReplicaGroup(_selfId, members);
        return (members, new ReplicaRpcHeader(_selfId, _topologyFingerprint, _generation, term, _selfId, _selfId));
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

    /// <summary>Applies the committed entries the current coordinator still retains.</summary>
    /// <returns><see langword="true" /> when no coordinator retains an unapplied entry.</returns>
    /// <remarks>
    /// An apply failure is logged and reported as <see langword="false" />: the entry stays retained and the caller refuses
    /// definitely, before anything of its own is appended.
    /// </remarks>
    private async Task<bool> TryApplyPendingAsync()
    {
        if (_coordinator == null)
            return true;

        try
        {
            return await _coordinator.ApplyCommittedAsync().ConfigureAwait(false);
        }
        catch (Exception error) when (error is not ObjectDisposedException)
        {
            LogManager.ReplicaPendingApplyFailed(Log, error);
            return false;
        }
    }

    /// <summary>No-op fault hooks for production commits outside fault-injection tests.</summary>
    [Immutable]
    private sealed class NoOpCommitHooks : IReplicaCommitFaultHooks
    {
        internal static NoOpCommitHooks Instance { get; } = new();

        public ValueTask OnStageAsync(ReplicaCommitStage stage, PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }

    /// <summary>Owner-side commit pipeline: local durable append, follower fan-out, and memory apply.</summary>
    /// <remarks>
    /// All calls originate from the single owning coordinator under its commit gate (ordered commit bodies, and
    /// the commit-and-apply of entries a late majority or a later drive covers), except
    /// <see cref="RecordLaggingReplica" />, which the coordinator also invokes from background follower
    /// observation. The coordinator serializes whole commit bodies under its commit gate, and the committer
    /// drives one commit at a time, so the fan-out for mutation N always runs between the local appending
    /// of N and N+1: the fan-out snapshot below always describes the batch it accompanies. Only the commit
    /// watermark is shared across the background path and stays monotonic; every other field is written
    /// solely by the serialized body.
    /// </remarks>
    private sealed class ReplicaGroupCommitPipeline : IReplicaCommitPipeline
    {
        private readonly ReplicaRpcHeader _header;
        private readonly ILogicalNamespacedCache<object?> _local;
        private readonly IFollowerLog _log;
        private readonly string[] _members;
        private readonly IReplicaRpcGateway _rpc;
        private readonly string _selfId;
        private ulong _commitIndex;
        private ulong _fanoutPrevIndex;
        private ulong _fanoutPrevTerm;
        private ulong _prevLogIndex;
        private ulong _prevLogTerm;

        /// <summary>Initializes a new instance of the <see cref="ReplicaGroupCommitPipeline" /> class.</summary>
        /// <param name="local">Local cache pipeline used for memory applies.</param>
        /// <param name="log">Owned group log for local durable appending.</param>
        /// <param name="rpc">Follower replication RPCs.</param>
        /// <param name="members">Ordered group members; index zero is this node.</param>
        /// <param name="selfId">This node identifier, matching <paramref name="members" /> index zero.</param>
        /// <param name="status">Durable log status seeding previous and commit positions.</param>
        /// <param name="header">Replication envelope identity for follower calls.</param>
        internal ReplicaGroupCommitPipeline(
            ILogicalNamespacedCache<object?> local,
            IFollowerLog log,
            IReplicaRpcGateway rpc,
            string[] members,
            string selfId,
            FollowerLogStatus status,
            ReplicaRpcHeader header)
        {
            ArgumentNullException.ThrowIfNull(local);
            ArgumentNullException.ThrowIfNull(log);
            ArgumentNullException.ThrowIfNull(rpc);
            ArgumentNullException.ThrowIfNull(members);
            ArgumentException.ThrowIfNullOrWhiteSpace(selfId);
            if (members.Length == 0 || !string.Equals(members[0], selfId, StringComparison.Ordinal))
                throw new ArgumentException("Group members must start with this node.", nameof(members));

            _local = local;
            _log = log;
            _rpc = rpc;
            _members = members;
            _selfId = selfId;
            _header = header;
            _prevLogIndex = status.LastLogIndex;
            _prevLogTerm = status.LastLogTerm;
            _commitIndex = status.CommitIndex;
        }

        /// <inheritdoc />
        public async ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken)
        {
            var result = await _log.AdvanceCommitAsync(commitIndex, cancellationToken).ConfigureAwait(false);
            if (!result.Success)
                throw new InvalidOperationException($"Local group commit advance was refused: {result.RefusalCode}.");

            _commitIndex = result.CommitIndex;
        }

        /// <inheritdoc />
        public async ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            var nodeId = _members[replicaIndex];
            var decoded = ReplicaLogCodec.Decode(mutation.CanonicalPayload);
            if (decoded is not { } append)
                throw new InvalidOperationException("Prepared mutation carries an undecodable canonical payload.");

            var batch = new FollowerBatch(new[] { append }, _selfId, mutation.Term, _fanoutPrevIndex, _fanoutPrevTerm, _commitIndex);
            var result = await _rpc.AppendEntriesAsync(nodeId, _header, batch, cancellationToken).ConfigureAwait(false);
            var follr = new ReplicaDurableAcknowledgement(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true);
            return !result.Success ? throw new InvalidOperationException($"Follower '{nodeId}' refused append: {result.RefusalCode}.") : follr;
        }

        /// <inheritdoc />
        public async ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            var entries = new FollowerLogEntry[1];
            entries[0] = new FollowerLogEntry(mutation.LogIndex, mutation.Term, mutation.CanonicalPayload);
            var request = new FollowerLogAppendRequest(_selfId, mutation.Term, _prevLogIndex, _prevLogTerm, _commitIndex, new ReadOnlyMemory<FollowerLogEntry>(entries));
            var result = await _log.AppendAsync(request, cancellationToken).ConfigureAwait(false);
            if (!result.Success)
                throw new InvalidOperationException($"Local group append was refused: {result.RefusalCode}.");

            // Snapshot the pre-appended positions for this mutation's fan-out, then advance. The fan-out
            // carries this very entry, so it must name the predecessor, not the entry itself.
            _fanoutPrevIndex = _prevLogIndex;
            _fanoutPrevTerm = _prevLogTerm;
            _prevLogIndex = mutation.LogIndex;
            _prevLogTerm = mutation.Term;
        }

        /// <inheritdoc />
        public async ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            var decoded = ReplicaLogCodec.Decode(mutation.CanonicalPayload);
            if (decoded is not { } apply)
                throw new InvalidOperationException("Prepared mutation carries an undecodable canonical payload.");

            _ = await ReplicaCacheApplier.ApplyAsync(_local, apply, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
            // Repair driving lands in a later milestone; the coordinator already observes stragglers
            // in the background, and a lagging replica simply stops counting toward the majority.
        }
    }
}
