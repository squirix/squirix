using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Threading;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Node.Services;

/// <summary>Serialized owner-side replicated commits for the group owned by this node.</summary>
/// <remarks>
/// Commits run one at a time per group under <see cref="AsyncLock" />: log indexes stay dense with no
/// gaps, prepare-time reads stay exact through the ordered apply, and the coordinator never observes
/// admission pressure or turn waits. The coordinator itself is never cancelled; a fixed commit budget
/// bounds every attempt and idempotent retries recover unknown outcomes.
/// </remarks>
internal sealed class ReplicaGroupCommitter : IAsyncDisposable
{
    private const int IdempotencyCapacity = 1024;
    private const int MaxInFlight = 2;
    private static readonly TimeSpan CommitTimeout = TimeSpan.FromSeconds(5);

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
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        // Drain in-flight committer operations holding _gate so their AsyncLockHolder can release
        // the semaphore before it is disposed. New admissions fail closed via ThrowIfDisposed.
        var drain = await _gate.LockAsync(CancellationToken.None).ConfigureAwait(false);
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
        var (coordinator, factory) = await EnsureStartedAsync(cancellationToken).ConfigureAwait(false);
        var mutation = await factory.PrepareRemoveAsync(operationId, cacheName, key, TakeNextIndex(), cancellationToken).ConfigureAwait(false);
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
        var (coordinator, factory) = await EnsureStartedAsync(cancellationToken).ConfigureAwait(false);
        var mutation = await factory.PrepareRemoveExpirationAsync(operationId, cacheName, key, TakeNextIndex(), cancellationToken).ConfigureAwait(false);
        var outcome = await CommitWithPreAppendResyncAsync(coordinator, mutation).ConfigureAwait(false);
        return DecodeApplied(outcome);
    }

    /// <summary>Commits a replicated unconditional write.</summary>
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
        var (coordinator, factory) = await EnsureStartedAsync(cancellationToken).ConfigureAwait(false);
        var mutation = factory.PrepareSet(operationId, cacheName, key, entry, TakeNextIndex());
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
        var (coordinator, factory) = await EnsureStartedAsync(cancellationToken).ConfigureAwait(false);
        var mutation = await factory.PrepareTouchAsync(operationId, cacheName, key, expiration, TakeNextIndex(), cancellationToken).ConfigureAwait(false);
        var outcome = await CommitWithPreAppendResyncAsync(coordinator, mutation).ConfigureAwait(false);
        return DecodeApplied(outcome);
    }

    /// <summary>Commits a replicated conditional add and returns whether the key was absent.</summary>
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
        var (coordinator, factory) = await EnsureStartedAsync(cancellationToken).ConfigureAwait(false);
        var mutation = await factory.PrepareTryAddAsync(operationId, cacheName, key, entry, TakeNextIndex(), cancellationToken).ConfigureAwait(false);
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
        var (coordinator, factory) = await EnsureStartedAsync(cancellationToken).ConfigureAwait(false);
        var mutation = await factory.PrepareUpdateAsync(operationId, cacheName, key, value, TakeNextIndex(), cancellationToken).ConfigureAwait(false);
        var outcome = await CommitWithPreAppendResyncAsync(coordinator, mutation).ConfigureAwait(false);
        return DecodeApplied(outcome);
    }

    private static bool DecodeApplied(ReadOnlyMemory<byte> outcome)
    {
        if (!ReplicaOutcomeCodec.TryDecode(outcome, out var applied, out _))
            throw new InvalidOperationException("Committed outcome payload is malformed.");

        return applied;
    }

    private static async Task<CacheRemoveResult<object?>> DecodeRemoveAsync(ReadOnlyMemory<byte> outcome)
    {
        if (!ReplicaOutcomeCodec.TryDecode(outcome, out var removed, out var previous) || (removed && previous.IsEmpty))
            throw new InvalidOperationException("Committed remove outcome payload is malformed.");

        if (!removed)
            return new CacheRemoveResult<object?>(false, default);

        var entry = CacheEntryWire.Parser.ParseFrom(new ReadOnlySequence<byte>(previous));
        var mapped = await entry.MapFromProtoAsync<object?>().ConfigureAwait(false);
        return new CacheRemoveResult<object?>(true, mapped.Value);
    }

    private static bool IsPostAppendOutcome(Exception error) =>
        error is InvalidOperationException && error.Message.StartsWith(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);

    private async ValueTask<ReadOnlyMemory<byte>> CommitWithPreAppendResyncAsync(ReplicaCommitCoordinator coordinator, PreparedReplicaMutation mutation)
    {
        try
        {
            return await coordinator.CommitAsync(mutation, CommitTimeout, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception error) when (!IsPostAppendOutcome(error))
        {
            // The local append was refused before anything was marked appended: the reserved
            // _nextIndex no longer matches the durable log, so drop the started state and rebuild
            // from status.LastLogIndex on the next attempt. Post-append outcomes (a durable
            // majority may exist) keep the reservation and sequencing untouched.
            _started = false;
            throw;
        }
    }

    private async Task<(ReplicaCommitCoordinator Coordinator, ReplicaMutationFactory Factory)> EnsureStartedAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        if (!_started)
            await StartAsync(cancellationToken).ConfigureAwait(false);

        if (_coordinator == null || _factory == null)
            throw new InvalidOperationException("Replica group committer is not started.");

        return (_coordinator, _factory);
    }

    private async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_registry.TryGetLog(_selfId, out var log) || log == null)
            throw new InvalidOperationException($"This node does not serve its owned replica group '{_selfId}'.");

        if (_coordinator != null)
            await _coordinator.DisposeAsync().ConfigureAwait(false);

        var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        var term = Math.Max(1UL, status.CurrentTerm);
        var members = new string[_locator.ReplicaCount];
        _locator.GetReplicaGroup(_selfId, members);
        var header = new ReplicaRpcHeader(_selfId, _topologyFingerprint, _generation, term, _selfId, _selfId);
        var pipeline = new ReplicaGroupCommitPipeline(_local, log, _gateway, members, _selfId, status, header);
        var idempotency = new GroupIdempotencyState(IdempotencyCapacity, GroupIdempotencyState.DefaultRetention);
        _coordinator = new ReplicaCommitCoordinator(
            new ReplicaCommitCoordinatorOptions(_locator.ReplicaCount, status.LastLogIndex, status.CommitIndex, MaxInFlight),
            pipeline,
            NoOpCommitHooks.Instance,
            idempotency,
            _registry.EligibilityFor(_selfId));
        _factory = new ReplicaMutationFactory(_local, _selfId, term);
        _nextIndex = status.LastLogIndex + 1;
        _started = true;
    }

    private ulong TakeNextIndex() => _nextIndex++;

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

    /// <summary>No-op fault hooks for production commits outside fault-injection tests.</summary>
    [Immutable]
    private sealed class NoOpCommitHooks : IReplicaCommitFaultHooks
    {
        internal static NoOpCommitHooks Instance { get; } = new();

        public ValueTask OnStageAsync(ReplicaCommitStage stage, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _ = stage;
            _ = mutation;
            _ = cancellationToken;
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>Owner-side commit pipeline: local durable append, follower fan-out, and memory apply.</summary>
    /// <remarks>
    /// All calls originate from the single owning coordinator's serialized ordered body, except
    /// <see cref="RecordLaggingReplica" />, which the coordinator also invokes from background follower
    /// observation. The coordinator serializes whole commit bodies under its commit gate and the committer
    /// drives one commit at a time, so the fan-out for mutation N always runs between the local appends
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
        /// <param name="log">Owned group log for local durable appends.</param>
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

            var header = _header;
            var batch = new FollowerBatch(new[] { append }, _selfId, mutation.Term, _fanoutPrevIndex, _fanoutPrevTerm, _commitIndex);
            var result = await _rpc.AppendEntriesAsync(nodeId, header, batch, cancellationToken).ConfigureAwait(false);
            if (!result.Success)
                throw new InvalidOperationException($"Follower '{nodeId}' refused append: {result.RefusalCode}.");

            return new ReplicaDurableAcknowledgement(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true);
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

            // Snapshot the pre-append positions for this mutation's fan-out, then advance. The fan-out
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
            _ = replicaIndex;
            _ = logIndex;

            // Repair driving lands in a later milestone; the coordinator already observes stragglers
            // in the background and a lagging replica simply stops counting toward the majority.
        }
    }
}
