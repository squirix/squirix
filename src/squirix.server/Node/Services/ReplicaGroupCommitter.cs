using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Serialized leader-side replicated commits for one replica group this node leads.</summary>
/// <remarks>
/// Commits run one at a time per group under <see cref="AsyncLock" />: log indexes stay dense with no
/// gaps, prepare-time reads stay exact through the ordered applying, and the coordinator never observes
/// admission pressure or turn waits. The coordinator itself is never canceled; a fixed commit budget
/// bounds every attempt up to its durable majority, and idempotent retries recover unknown outcomes.
/// </remarks>
internal sealed class ReplicaGroupCommitter : IAsyncDisposable
{
    internal const int MaxInFlight = 2;
    internal const string CommitBudgetRefusalReason = "replica_commit_budget";
    private const string PendingApplyRefusalReason = "replica_apply_pending";
    private static readonly TimeSpan DefaultCommitBudget = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan DefaultShutdownBudget = TimeSpan.FromSeconds(30);

    private readonly Lazy<ReplicaExpirationCoordinator<NodeCacheEntry<object?>>> _expiration;
    private readonly AsyncLock _gate = new();
    private readonly IReplicaRpcGateway _gateway;
    private readonly ILogicalNamespacedCache<object?> _local;
    private readonly IReplicaGroupLocator _locator;
    private readonly ReplicaGroupRegistry _registry;
    private readonly Lazy<ReplicaVerificationProbe> _probe;
    private readonly ReplicaTopologyStamp _topology;
    private ReplicaCommitCoordinator? _coordinator;
    private int _disposed;
    private ReplicaMutationFactory? _factory;
    private ReplicaGroupCommitPipeline? _pipeline;

    private bool _started;

    /// <summary>The leadership of the group by election, or <see langword="null" /> while there is none; written by the driver's calls only.</summary>
    private ReplicaLeaderTenure? _tenure;
    private volatile bool _recovered;

    /// <summary>Initializes a new instance of the <see cref="ReplicaGroupCommitter" /> class.</summary>
    /// <param name="registry">Replica group registry of this node.</param>
    /// <param name="locator">Replica group locator resolving the owned group members.</param>
    /// <param name="gateway">Follower replication RPCs.</param>
    /// <param name="local">Local cache pipeline used for prepare reads and memory applies.</param>
    /// <param name="identity">The identifier of the group and the identifier of this node, which leads it and is a member of it.</param>
    /// <param name="topology">Static topology fingerprint and configuration generation.</param>
    /// <param name="log">Logger for lifecycle failures.</param>
    internal ReplicaGroupCommitter(
        ReplicaGroupRegistry registry,
        IReplicaGroupLocator locator,
        IReplicaRpcGateway gateway,
        ILogicalNamespacedCache<object?> local,
        (string GroupId, string SelfId) identity,
        in ReplicaTopologyStamp topology,
        ILogger<ReplicaGroupCommitter> log)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(locator);
        ArgumentNullException.ThrowIfNull(gateway);
        ArgumentNullException.ThrowIfNull(local);
        ArgumentException.ThrowIfNullOrWhiteSpace(identity.GroupId);
        ArgumentException.ThrowIfNullOrWhiteSpace(identity.SelfId);
        ArgumentNullException.ThrowIfNull(log);
        Log = log;
        _registry = registry;
        _locator = locator;
        _gateway = gateway;
        _local = local;
        GroupId = identity.GroupId;
        _topology = topology.Fingerprint.IsEmpty ? throw new ArgumentException("Topology fingerprint must not be empty.", nameof(topology)) : topology;
        _probe = new Lazy<ReplicaVerificationProbe>(
            () => new ReplicaVerificationProbe(registry, locator, gateway, identity, _topology.Fingerprint, _topology.Generation, Log),
            LazyThreadSafetyMode.ExecutionAndPublication);
        CommitBudget = DefaultCommitBudget;
        ShutdownBudget = DefaultShutdownBudget;

        // Created on first use, after the init-only budgets are set. One run of the expiry serves every caller of the key, so it takes no
        // caller token: the commit budget bounds the wait before the append, and the commit itself is budget-bounded.
        _expiration = new Lazy<ReplicaExpirationCoordinator<NodeCacheEntry<object?>>>(
            () => new ReplicaExpirationCoordinator<NodeCacheEntry<object?>>(async (cacheName, key) =>
            {
                ThrowIfDisposed();
                using var budget = new CancellationTokenSource(CommitBudget, BudgetTimeProvider);
                await WaitForLocalRecoveryAsync(budget.Token).ConfigureAwait(false);
                ThrowIfDisposed();
                using var guard = await _gate.LockAsync(budget.Token).ConfigureAwait(false);
                var (coordinator, factory) = await EnsureStartedAsync(true, budget.Token).ConfigureAwait(false);
                var (tombstone, current) = await factory.PrepareExpireAsync(cacheName, key, PeekNextIndex(), budget.Token).ConfigureAwait(false);
                if (tombstone == null)
                    return current;

                _ = await this.CommitWithPreAppendResyncAsync(coordinator, tombstone).ConfigureAwait(false);
                return null;
            })
            {
                ShutdownBudget = ShutdownBudget,
                ShutdownTimeProvider = ShutdownTimeProvider,
                ShutdownLeakReporter = budget => ServerLog.ReplicaExpirationLeakedOnShutdown(Log, budget),
            },
            LazyThreadSafetyMode.ExecutionAndPublication);
    }

    /// <summary>Gets the budget of one commit attempt up to its durable majority; 5 seconds unless set.</summary>
    /// <remarks>
    /// It bounds queueing, the local appending and the majority wait; an attempt that runs out of it after the local appending
    /// ends with an unknown outcome.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">The budget is not positive.</exception>
    internal TimeSpan CommitBudget
    {
        get;
        init
        {
            value.ThrowIfNegativeOrZero(nameof(value), "The commit budget must be greater than zero.");

            field = value;
        }
    }

    /// <summary>Gets or initializes the longest wait of a compaction step for the followers and the commit gate; 10 seconds unless set.</summary>
    /// <remarks>A step that runs out of it is skipped until the next pass; the compaction itself, once it holds the gate, is never bounded by it.</remarks>
    /// <exception cref="ArgumentOutOfRangeException">The budget is not positive.</exception>
    internal TimeSpan CompactionWaitBudget
    {
        get;
        init
        {
            value.ThrowIfNegativeOrZero(nameof(value), "The compaction wait budget must be greater than zero.");

            field = value;
        }
    } = TimeSpan.FromSeconds(10);

    /// <summary>Gets or initializes the time source of the commit budget and of the follower request timeouts; the system clock unless set.</summary>
    /// <remarks>Test seam: production committers keep the system clock.</remarks>
    internal TimeProvider BudgetTimeProvider { get; init; } = TimeProvider.System;

    /// <summary>Initializes the time source of the shutdown budget that bounds the dispose drain; the system clock unless set.</summary>
    /// <remarks>Test seam: production committers keep the system clock.</remarks>
    internal TimeProvider ShutdownTimeProvider { private get; init; } = TimeProvider.System;

    /// <summary>Gets or initializes the time source that pins the expiration deadlines of prepared records and decides expiry; the system clock unless set.</summary>
    /// <remarks>The prepare of a mutation and the expiry check of a read use it. Applying a record never does.</remarks>
    internal TimeProvider Clock { get; init; } = TimeProvider.System;

    /// <summary>Initializes the journal lifecycle whose startup gate opens once local recovery has replayed the journal into memory.</summary>
    /// <remarks>Commits and verifications wait for the gate before they read memory, so no decision is prepared against a partly recovered cache.</remarks>
    internal required IJournalCoordinatorLifecycle Recovery { private get; init; }

    /// <summary>Gets the identifier of the replica group this committer leads.</summary>
    internal string GroupId { get; }

    /// <summary>Initializes the election state of the group; when set, this committer leads only a term an election hands it.</summary>
    /// <remarks>
    /// Without it the committer leads the own group statically, in the term of its log, as long as the node runs. With it nothing starts
    /// before <see cref="PromoteAsync" />: a promotion appends a leader-term entry of the won term, every follower reply is posted to the
    /// state, and the committer stops leading at <see cref="RetireAsync" />.
    /// </remarks>
    internal ReplicaGroupState? Election { private get; init; }

    /// <summary>Gets the pipeline of the running coordinator, whose senders carry the heartbeats of an elected leader.</summary>
    internal ReplicaGroupCommitPipeline? RunningPipeline => Volatile.Read(ref _pipeline);

    /// <summary>Gets the current leadership of the group by election, or <see langword="null" /> while there is none.</summary>
    internal ReplicaLeaderTenure? Tenure => Volatile.Read(ref _tenure);

    /// <summary>Initializes the applier of the led group, which this committer drives for as long as it leads the group.</summary>
    /// <remarks>
    /// The applier lives as long as the node, so its applied index survives a replaced coordinator; while this committer leads the group
    /// it is the only caller of the applier's catch-up and applies.
    /// </remarks>
    /// <exception cref="ArgumentException">The applier serves another group.</exception>
    internal required ReplicaGroupApplier Applier
    {
        private get;
        init
        {
            ArgumentNullException.ThrowIfNull(value);
            if (!string.Equals(value.GroupId, GroupId, StringComparison.Ordinal))
                throw new ArgumentException($"The applier of group '{value.GroupId}' cannot serve the committer of group '{GroupId}'.", nameof(value));

            field = value;
        }
    }

    /// <summary>Gets or initializes the longest wait for an in-flight commit on dispose, which also caps the coordinator's own teardown wait; 30 seconds unless set.</summary>
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

    /// <summary>Gets the expiry of the owned group: one run per key decides it on the leader clock and commits the tombstone of an expired entry.</summary>
    /// <remarks>Disposed first by <see cref="DisposeAsync" />, so it refuses expiries once this committer is disposing.</remarks>
    internal ReplicaExpirationCoordinator<NodeCacheEntry<object?>> Expiration => _expiration.Value;

    /// <summary>Gets the logger for lifecycle failures.</summary>
    internal ILogger Log { get; }

    /// <summary>Gets the follower verification, which also hands out the followers to catch up.</summary>
    internal ReplicaVerificationProbe Probe => _probe.Value;

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        // One shutdown budget bounds the whole drain: the expiries first, then the gate, so dispose never waits longer than the budget.
        using var budget = new CancellationTokenSource(ShutdownBudget, ShutdownTimeProvider);

        // Expiries in flight go first: they queue on the gate the drain below takes. An expiry still running when the budget ends is
        // reported by the expiration coordinator and faulted by the gate disposal below.
        try
        {
            await _expiration.Value.DisposeAsync().AsTask().WaitAsync(budget.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (budget.IsCancellationRequested)
        {
            // The gate drain below finds the budget spent as well and leaks the gate loudly.
        }

        // Drain in-flight committer operations holding _gate so their AsyncLockHolder can release
        // the gate before it is disposed of. New admissions fail closed via ThrowIfDisposed. The drain stays held until the
        // coordinator and the gate are disposed, so no caller queued behind it runs a body against a coordinator being torn down:
        // disposing the gate faults every queued caller with ObjectDisposedException.
        // Work after a durable majority ignores cancellation and can outlast a stalled disk, so the
        // drain is bounded: on expiry the coordinator and the gate stay with the in-flight commit and
        // are leaked loudly instead of being torn down under it. Not throwing keeps the host disposing
        // the services behind this one (the group logs and the journal). Callers queued behind the stuck holder are still faulted,
        // by disposing the gate: the holder keeps exclusion and can still release.
        AsyncLockHolder drain;
        try
        {
            drain = await _gate.LockAsync(budget.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            ServerLog.ReplicaCommitterLeakedOnShutdownTimeout(Log, ShutdownBudget);
            _gate.Dispose();
            return;
        }

        using (drain)
        {
            try
            {
                // The senders close first: their pending and in-flight follower appends end at once, so the coordinator's teardown
                // does not wait for a follower that is slow or gone.
                if (_pipeline != null)
                    await this.CloseSendersAsync(_pipeline).ConfigureAwait(false);

                if (_coordinator != null)
                    await _coordinator.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                _tenure?.Dispose();
                _gate.Dispose();
            }
        }
    }

    /// <summary>Commits one write under the commit gate: prepares it at the next log index, commits it, and decodes its outcome.</summary>
    /// <typeparam name="TState">The arguments of the write.</typeparam>
    /// <typeparam name="TResult">The decoded outcome.</typeparam>
    /// <param name="write">The cache scope and the client operation identifier of the write.</param>
    /// <param name="state">The arguments of the write, handed to <paramref name="prepare" />.</param>
    /// <param name="fingerprint">
    /// Computes the operation fingerprint of the write from its arguments; called only when the write is refused before it is
    /// prepared and an entry with its identity is retained.
    /// </param>
    /// <param name="prepare">Prepares the mutation from the running factory, the arguments, and the log index it is appended at.</param>
    /// <param name="decode">Decodes the committed outcome.</param>
    /// <param name="cancellationToken">Cancellation token for queueing only; the commit itself is budget-bounded.</param>
    /// <returns>The decoded outcome of the committed write.</returns>
    /// <exception cref="ServerOpIdMismatchException">The operation identifier is reused with another request.</exception>
    /// <exception cref="SquirixException">The outcome of the operation is unknown, or the write is refused retryably.</exception>
    /// <remarks>The typed writes built on this method are in <see cref="ReplicaGroupCommitterWrites" />.</remarks>
    internal async Task<TResult> CommitAsync<TState, TResult>(
        (string Scope, string OperationId) write,
        TState state,
        Func<TState, byte[]> fingerprint,
        Func<ReplicaMutationFactory, TState, ulong, CancellationToken, ValueTask<PreparedReplicaMutation>> prepare,
        Func<ReadOnlyMemory<byte>, ValueTask<TResult>> decode,
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        await WaitForLocalRecoveryAsync(cancellationToken).ConfigureAwait(false);
        ThrowIfDisposed();
        using var guard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        var starting = EnsureStartedAsync(true, cancellationToken);
        if (await starting.CaptureFailureAsync().ConfigureAwait(false) != null)
        {
            // Whatever refused the start, a retained entry of this operation decides the answer. A retry of a committed operation replays
            // its outcome: it needs no majority and no apply. A retry of an operation whose entry is appended but not yet committed
            // (possibly by the process before a restart) must neither re-execute nor be told it failed: its outcome stays unknown until a
            // commit resolves the entry. The same identifier with another request is a reuse, whatever the state of the entry. Without a
            // retained entry the refusal stands and is rethrown by the await below.
            var retained = ReplicaGroupCommitterCommits.LookupRetained(_registry.TryGetLog(GroupId, out var log) ? log : null, write, state, fingerprint, out var recorded);
            if (retained == GroupIdempotencyLookup.Found)
                return await decode(recorded.OutcomePayload).ConfigureAwait(false);
            if (retained == GroupIdempotencyLookup.Mismatch)
                throw new ServerOpIdMismatchException();
            if (retained == GroupIdempotencyLookup.Unresolved)
                throw ServerOpContract.CommitOutcomeUnknown();
        }

        var (coordinator, factory) = await starting.ConfigureAwait(false);
        var index = PeekNextIndex();
        var mutation = await prepare(factory, state, index, cancellationToken).ConfigureAwait(false);
        var outcome = await this.CommitWithPreAppendResyncAsync(coordinator, mutation).ConfigureAwait(false);
        return await decode(outcome).ConfigureAwait(false);
    }

    /// <summary>Persists the in-memory applied index of the owned group log once the cache journal holds every applied entry durably.</summary>
    /// <param name="durability">The node cache journal whose frames the applies appended.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the durable applied index is at least the in-memory one read at the start.</returns>
    /// <exception cref="InvalidOperationException">The owned group log refused the applied advance.</exception>
    /// <remarks>
    /// Runs outside the commit gate. Every entry at or below the applied index read here returned from its apply, which appends its
    /// cache journal frame first, so the durability barrier awaited next covers all of them; only then does the log advance its applied
    /// index and release the applied payloads, so a crash never leaves the log claiming an apply the cache journal lost. Nothing is
    /// done while the durable applied index is already there.
    /// </remarks>
    internal async Task FlushAppliedAsync(IJournalDurabilityCoordinator durability, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(durability);
        ThrowIfDisposed();
        if (!_registry.TryGetLog(GroupId, out var log))
            return;

        await Applier.FlushAsync(log, durability, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Compacts the owned group log through its commit index once it reaches a threshold and nothing still needs its entries.</summary>
    /// <param name="policy">The compaction thresholds.</param>
    /// <param name="durability">The node cache journal whose frames the applies appended.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The step outcome; only <see cref="ReplicaLogCompactionOutcome.Compacted" /> changes the log.</returns>
    /// <remarks>
    /// The thresholds, and advisorily the followers, are checked without the commit gate. Everything else is one step under it: no
    /// write can append, commit, or apply between the checks and the compaction, so a steady write load cannot keep moving the commit
    /// index past the applied one. A write arriving meanwhile waits for the step and then appends after the compacted log.
    /// </remarks>
    internal async Task<ReplicaLogCompactionOutcome> CompactOwnedLogAsync(
        ReplicaLogCompactionPolicy policy,
        IJournalDurabilityCoordinator durability,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(durability);
        ThrowIfDisposed();
        if (!_registry.TryGetLog(GroupId, out var log))
            return ReplicaLogCompactionOutcome.NotReady;

        var retention = await log.GetRetentionAsync(cancellationToken).ConfigureAwait(false);
        if (!policy.IsReachedBy(in retention))
            return ReplicaLogCompactionOutcome.BelowThreshold;

        // An advisory check first, without the gate: a follower that is down or lagging then refuses the step without holding the
        // gate for the whole follower wait on every pass. The decisive check runs again under the gate. The wait budget bounds only these
        // waits: once the gate is held, the compaction runs to its end, so a budget never cancels a durable rewrite of the log.
        var eligibility = _registry.EligibilityFor(GroupId);
        using var budget = new CancellationTokenSource(CompactionWaitBudget, BudgetTimeProvider);
        using var waiting = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, budget.Token);
        AsyncLockHolder guard;
        try
        {
            if (Volatile.Read(ref _coordinator) is { } running)
            {
                var observed = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
                if (await ReplicaLogCompactionStep.AwaitFollowersAsync(running, eligibility, observed.CommitIndex, Clock, waiting.Token).ConfigureAwait(false) is { } refused)
                    return refused;
            }

            guard = await _gate.LockAsync(waiting.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (budget.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            return ReplicaLogCompactionOutcome.NotReady;
        }

        using var held = guard;
        ThrowIfDisposed();

        // An elected leader compacts only once its leader-term entry is committed: the authority check reads the term of that entry.
        return _started && _coordinator is { } coordinator && (Election == null || _tenure is { Authorized: true })
            ? await ReplicaLogCompactionStep.RunAsync(log, coordinator, eligibility, Applier.AppliedIndex, durability, Clock, cancellationToken).ConfigureAwait(false)
            : ReplicaLogCompactionOutcome.NotReady;
    }

    /// <summary>Verifies non-ready replica slots against the leader log so a restarted group regains its write quorum.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// The verification state: <see cref="ReplicaVerification.Pending" /> while some follower is not yet verified or an uncommitted
    /// tail is not yet committed; <see cref="ReplicaVerification.Blocked" /> while the log is not ready or its uncommitted tail holds
    /// no entry of the current term.
    /// </returns>
    /// <remarks>
    /// Followers are probed without holding the commit gate, so a dead or slow peer never delays writes; a follower that lacks entries,
    /// the uncommitted tail included, is caught up afterwards by the readiness service through its sender. Only when some follower answered does the gate get taken to start the coordinator (which
    /// recovers the tail), re-check that the leader tail did not move, admit the verified slots, and commit what they now cover.
    /// </remarks>
    internal async Task<ReplicaVerification> VerifyReplicasAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        await WaitForLocalRecoveryAsync(cancellationToken).ConfigureAwait(false);
        ThrowIfDisposed();
        if (!_registry.TryGetLog(GroupId, out var log))
            return ReplicaVerification.Blocked;

        // A committer that leads by election verifies nothing once it no longer leads, and verifies in its led term while it does.
        var tenure = Volatile.Read(ref _tenure);
        if (Election != null && tenure == null)
            return ReplicaVerification.Blocked;

        var snapshot = await Probe.ProbeAsync(log, tenure?.Term ?? 0UL, cancellationToken).ConfigureAwait(false);
        if (snapshot.Verdict is { } verdict)
            return verdict;

        using var guard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        ThrowIfDisposed();

        // A retirement may have run while the probe was out: the snapshot then belongs to a leadership that is over.
        return Election != null && !ReferenceEquals(_tenure, tenure) ? ReplicaVerification.Blocked
            : await AdmitVerifiedAsync(log, snapshot, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Admits a follower slot a catch-up session verified, under the commit gate, and commits what the slot now covers.</summary>
    /// <param name="replicaIndex">Zero-based follower slot.</param>
    /// <param name="result">The session result.</param>
    /// <param name="pipeline">The pipeline whose sender the session ran on; a slot is never admitted into a newer one.</param>
    /// <param name="cancellationToken">Cancellation token for queueing on the gate.</param>
    /// <returns><see langword="true" /> when the slot became ready.</returns>
    /// <remarks>
    /// Called while the session's lease still holds the sender, so no live entry past the held index reaches the follower, and no
    /// acknowledgement of one is lost to a slot that does not count yet, before the slot's match index is raised.
    /// </remarks>
    internal async Task<bool> AdmitCaughtUpFollowerAsync(int replicaIndex, ReplicaCatchUpResult result, IReplicaCommitPipeline pipeline, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        using var guard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        ThrowIfDisposed();
        if (!ReferenceEquals(_pipeline, pipeline) || _coordinator is not { } coordinator || !_registry.TryGetLog(GroupId, out var log))
            return false;

        var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        var eligibility = _registry.EligibilityFor(GroupId);
        var wasReady = eligibility.CanCountInWriteQuorum(replicaIndex);
        ReplicaReadinessProbe.AdmitCaughtUp(eligibility, replicaIndex, in result, status.CommitIndex, _topology.Fingerprint, _topology.Generation, coordinator);
        if (wasReady || !eligibility.CanCountInWriteQuorum(replicaIndex))
            return false;

        _ = await TryApplyPendingAsync().ConfigureAwait(false);
        return true;
    }

    /// <summary>Tells whether the owned group log holds the entry of a prepared mutation.</summary>
    /// <param name="mutation">The prepared mutation.</param>
    /// <returns><see langword="true" /> when the log holds the entry.</returns>
    internal ValueTask<bool> HoldsEntryAsync(PreparedReplicaMutation mutation) => _registry.HoldsEntryAsync(GroupId, mutation);

    /// <summary>Drops the started state, so the next attempt rebuilds the pipeline positions from the durable log status.</summary>
    /// <remarks>Runs under the commit gate.</remarks>
    internal void DropStartedState() => _started = false;

    /// <summary>Leads the group in a won term and reports whether its leader-term entry is committed.</summary>
    /// <param name="term">The won term.</param>
    /// <param name="cancellationToken">Cancellation token; it ends the wait for the applier lease, local recovery, or the commit gate.</param>
    /// <returns>
    /// <see langword="true" /> once the leader-term entry this promotion appended is committed by a coordinator of this promotion and the
    /// log still holds it in <paramref name="term" />; <see langword="false" /> while it is not, or when the start failed and is retried.
    /// </returns>
    /// <exception cref="InvalidOperationException">The committer leads statically, or still leads another term.</exception>
    /// <remarks>
    /// The first call takes the lease of the applier, waiting for a running apply pass, and keeps it until <see cref="RetireAsync" />, so
    /// the apply loop of the group never runs meanwhile. The entry is appended once per promotion, at a new index with an identity of its
    /// own, so no entry of an earlier leadership, committed or not, stands in for it. It is committed without the write majority check of
    /// client writes: every follower starts unverified, and verification needs an entry of the term in the tail.
    /// </remarks>
    internal async Task<bool> PromoteAsync(ulong term, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfZero(term);
        ThrowIfDisposed();
        if (Election == null)
            throw new InvalidOperationException($"Replica group '{GroupId}' is led statically and is never promoted.");

        await WaitForLocalRecoveryAsync(cancellationToken).ConfigureAwait(false);
        var tenure = Volatile.Read(ref _tenure);
        if (tenure == null)
        {
            await Applier.DriverLease.LeadAsync(cancellationToken).ConfigureAwait(false);
            _tenure = new ReplicaLeaderTenure(term, Applier.DriverLease);
            tenure = _tenure;
        }
        else if (tenure.Term != term)
        {
            throw new InvalidOperationException($"Replica group '{GroupId}' still leads term {tenure.Term}; it cannot be promoted to term {term}.");
        }

        using var guard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        ThrowIfDisposed();
        try
        {
            if (!_started)
                await StartAsync(cancellationToken).ConfigureAwait(false);

            _ = await TryApplyPendingAsync().ConfigureAwait(false);
            return _registry.TryGetLog(GroupId, out var log) && await tenure.IsAuthorizedAsync(_coordinator, log, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is IOException or InvalidDataException or InvalidOperationException or SquirixException)
        {
            // The start is retried on the next call: a storage fault, an inconsistent record, a log that moved past the term, or committed
            // entries of a replaced coordinator still to apply.
            ServerLog.ReplicaPromotionRetry(Log, GroupId, term, exception);
            return false;
        }
    }

    /// <summary>Stops leading the group by election: closes its followers, disposes its coordinator, and releases the applier lease.</summary>
    /// <param name="cancellationToken">Cancellation token for the wait on the commit gate.</param>
    /// <returns>
    /// <see langword="true" /> once the group is retired, or when it was not led; <see langword="false" /> while a committed entry is still
    /// to be applied, when the coordinator and the lease are kept and the call is to be retried.
    /// </returns>
    /// <remarks>
    /// The caller revokes the authority first, so no write is admitted meanwhile. Entries a majority never acknowledged are left in the log
    /// for the next leader to commit or truncate; the apply loop of the group takes over from the applied index.
    /// </remarks>
    internal async Task<bool> RetireAsync(CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _tenure) is not { } tenure)
            return true;

        using var guard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        if (!await TryApplyPendingAsync().ConfigureAwait(false) && _coordinator is { } retained && Applier.AppliedIndex < retained.CommitIndex)
            return false;

        if (_pipeline != null)
            await this.CloseSendersAsync(_pipeline).ConfigureAwait(false);

        if (_coordinator != null)
            await _coordinator.DisposeAsync().ConfigureAwait(false);

        _coordinator = null;
        Volatile.Write(ref _pipeline, null);
        _factory = null;
        _started = false;
        Volatile.Write(ref _tenure, null);
        await tenure.EndAsync().ConfigureAwait(false);
        return true;
    }

    /// <summary>Waits until local recovery has replayed the journal into memory, so no decision is prepared against a partly recovered cache.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes once the journal startup gate is open.</returns>
    /// <remarks>Must run before the commit gate is taken: a commit queued on the gate never waits for recovery while holding it.</remarks>
    private async ValueTask WaitForLocalRecoveryAsync(CancellationToken cancellationToken)
    {
        if (_recovered)
            return;

        await Recovery.WaitForStartupAsync(cancellationToken).ConfigureAwait(false);
        _recovered = true;
    }

    /// <summary>Admits the followers verified outside the gate and commits what the verified slots now cover.</summary>
    /// <param name="log">The owned group log.</param>
    /// <param name="snapshot">The follower probing taken outside the gate.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The verification state.</returns>
    /// <remarks>Runs under the commit gate.</remarks>
    private async Task<ReplicaVerification> AdmitVerifiedAsync(
        IFollowerLog log,
        ReplicaVerificationSnapshot snapshot,
        CancellationToken cancellationToken)
    {
        // A coordinator that still retains entries is never replaced (its restart refuses): the verified slots are admitted into it,
        // and its resolver commits and applies what they now cover. Otherwise the coordinator starts here, recovering the log tail.
        var coordinator = !await TryApplyPendingAsync().ConfigureAwait(false) && _coordinator is { } retained ? retained
            : (await EnsureStartedAsync(false, cancellationToken).ConfigureAwait(false)).Coordinator;
        var eligibility = await Probe.AdmitVerifiedSlotsAsync(log, snapshot, coordinator, cancellationToken).ConfigureAwait(false);
        if (_pipeline is { } pipeline)
            Probe.OfferCatchUp(snapshot.Answered, pipeline.CatchUpTargetFor);

        var applied = await TryApplyPendingAsync().ConfigureAwait(false);
        return applied && eligibility.AllCanCountInWriteQuorum() ? ReplicaVerification.AllReady : ReplicaVerification.Pending;
    }

    /// <summary>Starts the coordinator when needed and, for a write, checks that it may be prepared and appended now.</summary>
    /// <param name="write">Whether a write is to be prepared next; <see langword="false" /> for verification.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The running coordinator and mutation factory.</returns>
    /// <exception cref="Grpc.Core.RpcException">The write has no verified majority: Unavailable, nothing was written.</exception>
    /// <exception cref="SquirixException">An appended entry is not yet applied (too many requests).</exception>
    /// <exception cref="InvalidOperationException">The committer is not started.</exception>
    private async Task<(ReplicaCommitCoordinator Coordinator, ReplicaMutationFactory Factory)> EnsureStartedAsync(bool write, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        if (!_started)
            await StartAsync(cancellationToken).ConfigureAwait(false);

        // Refused before anything is appended: a write that cannot reach a majority would leave an uncommitted local tail.
        // Dropping the started state re-probes the followers on the next write. Decisions are prepared from live memory, so an
        // entry that is appended but not yet applied would leave the decision blind to its effect: such a
        // write fails definitely and may be retried; only this gate appends, so the check cannot go stale before the prepare.
        var majority = !write || _registry.EligibilityFor(GroupId).HasWriteMajority();
        var applied = !write || (majority && await TryApplyPendingAsync().ConfigureAwait(false));
        if (!majority)
            _started = false;

        return (majority, applied, _coordinator, _factory) switch
        {
            (false, _, _, _) => throw ServerOpContract.NoWriteMajority(),
            (true, false, _, _) => throw ServerOpContract.TooManyRequests(PendingApplyRefusalReason),
            (true, true, { } coordinator, { } factory) => (coordinator, factory),
            _ => throw new InvalidOperationException("Replica group committer is not started."),
        };
    }

    /// <summary>Returns the next group log index to prepare with: the one after the last entry appended to the local log.</summary>
    /// <remarks>
    /// The index follows the local appends of the running pipeline, so it moves only when an entry is appended: a prepare that fails, a
    /// refusal before the append, and a retry the coordinator answers from the idempotency state without appending all leave it for the
    /// next write, and the durable log stays dense.
    /// </remarks>
    /// <exception cref="InvalidOperationException">The committer is not started.</exception>
    private ulong PeekNextIndex() => ThrowHelper.Required(_pipeline, "Replica group committer is not started.").NextLogIndex;

    private async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_registry.TryGetLog(GroupId, out var log))
            throw new InvalidOperationException($"This node does not serve the replica group '{GroupId}' it leads.");

        // A start after retirement refuses like the write gate: no leader with authority here, nothing written.
        var tenure = Election == null || _tenure != null ? _tenure : throw ServerOpContract.NoLeaderAuthority();
        var replacing = _coordinator != null;
        await RetireCoordinatorAsync().ConfigureAwait(false);

        // One read pairs the status with its tail: a commit left running by the disposed coordinator may still advance the log.
        var read = await log.GetLeaderTailAsync(cancellationToken).ConfigureAwait(false);
        var status = read.Status;

        // Memory must hold every committed entry before anything new is prepared.
        await Applier.CatchUpAsync(log, status.LastAppliedIndex, status.CommitIndex, cancellationToken).ConfigureAwait(false);

        // A static leader leads in the term of its log. An elected one leads its won term and appends its leader-term entry before any
        // follower is probed, so verification can admit the followers that hold it; the entry commits like a recovered tail.
        var term = tenure?.TermFor(in status, GroupId) ?? Math.Max(1UL, status.CurrentTerm);
        var factory = new ReplicaMutationFactory(_local, GroupId, term, Clock, Log);
        if (tenure != null)
        {
            read = await tenure.AppendNoopAsync(log, read, factory, Probe.SelfId, cancellationToken).ConfigureAwait(false);
            status = read.Status;
        }

        var (members, header) = Probe.BuildMembership(term);
        var leaderIndex = Probe.LeaderReplicaIndex;
        var tail = ReplicaLeaderTail.From(read);

        // A restart with durable progress leaves every slot recovering. Verify the leader's own log and every follower against its
        // last entry before the first commit, so the quorum is built from verified slots only. A follower still ready under a replaced
        // coordinator is verified again: it may hold less than the commit index the new coordinator starts it at. An uncommitted tail is
        // recovered by the coordinator and commits once verified slots hold it; followers lacking it are caught up through their senders,
        // outside this gate.
        var eligibility = _registry.EligibilityFor(GroupId);
        if (replacing || tenure != null)
            ReplicaReadinessProbe.UnverifyFollowers(eligibility, leaderIndex);

        ReplicaReadinessProbe.MarkLeaderReady(eligibility, leaderIndex, in status, _topology.Fingerprint, _topology.Generation);
        var results = eligibility.CanCountInWriteQuorum(leaderIndex)
            ? await ReplicaReadinessProbe.ProbeAllAsync(_gateway, ReplicaReadinessProbe.NonReadyFollowers(eligibility, leaderIndex), members, header, status, ReplicaVerificationProbe.ProbeTimeout, cancellationToken)
                                         .ConfigureAwait(false)
            : [];

        // The coordinator pins the tail in the log's idempotency state, which durable truncation releases pins from.
        var lagging = new ReplicaLaggingFollowers(GroupId, eligibility, Probe.Repairs, Log);
        var pipeline = new ReplicaGroupCommitPipeline(Applier, log, CreateSenders(members, leaderIndex, in status, in header), (header.LeaderNodeId, leaderIndex), lagging, in status, term);
        _pipeline = pipeline;
        _coordinator = this.CreateCoordinator((_locator.ReplicaCount, leaderIndex), pipeline, log, in status, eligibility, Applier.RecoverTail(tail, term, factory));

        // Before a restart the outcomes of the committed entries above the snapshot lived only in memory; their records carry them, so
        // a retry of an operation committed before the restart replays its outcome. The recovered tail is pinned first and keeps its pins.
        if (!log.Idempotency.OutcomesRebuilt)
            await RestoreOutcomesAsync(log, pipeline, cancellationToken).ConfigureAwait(false);

        // Verified slots are admitted at the leader's last index before they count, so they cover the recovered tail.
        ReplicaReadinessProbe.ApplyAll(eligibility, leaderIndex, results, in status, _topology.Fingerprint, _topology.Generation, _coordinator);
        _factory = factory;
        _started = true;
    }

    /// <summary>Creates the sender of every follower slot, seeded with the last entry of the leader log.</summary>
    /// <param name="members">Group members in slot order.</param>
    /// <param name="leaderIndex">The slot of this node, which gets no sender.</param>
    /// <param name="status">Durable log status of the leader.</param>
    /// <param name="header">Replication envelope identity for follower calls.</param>
    /// <returns>The senders of the follower slots, in slot order.</returns>
    /// <remarks>The commit budget bounds one request, and the shutdown budget bounds waiting for one that ignores its cancellation on dispose.</remarks>
    private ReplicaFollowerSender[] CreateSenders(string[] members, int leaderIndex, in FollowerLogStatus status, in ReplicaRpcHeader header)
    {
        return ReplicaFollowerSenders.Create(
            _gateway,
            members,
            leaderIndex,
            in status,
            in header,
            new ReplicaFollowerSenders.SenderTiming(CommitBudget, ShutdownBudget, BudgetTimeProvider)
            {
                Replies = Election is { } election ? (slot, reply) => election.RecordFollowerReply(slot, in reply) : null,
            },
            budget => ServerLog.ReplicaFollowerSenderLeakedOnShutdown(Log, budget));
    }

    /// <summary>Rebuilds the outcomes of the committed log entries once, after the coordinator of the first start pinned the recovered tail.</summary>
    /// <param name="log">The owned group log.</param>
    /// <param name="pipeline">The pipeline of the new coordinator, closed together with it on failure.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <remarks>
    /// A failure drops the new coordinator and closes its pipeline before it serves anything: it would keep the recovered tail with no
    /// follower progress recorded, refusing the next writes. Its tail is durable, so the next start pins it again and retries the rebuild.
    /// </remarks>
    private async Task RestoreOutcomesAsync(IFollowerLog log, ReplicaGroupCommitPipeline pipeline, CancellationToken cancellationToken)
    {
        try
        {
            var restored = await ReplicaOutcomeRecovery.RestoreAsync(log, Clock, cancellationToken).ConfigureAwait(false);
            ServerLog.ReplicaOutcomesRestored(Log, GroupId, restored);
        }
        catch
        {
            var coordinator = _coordinator;
            _coordinator = null;
            _pipeline = null;
            await this.CloseSendersAsync(pipeline).ConfigureAwait(false);
            if (coordinator != null)
                await coordinator.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Disposes the coordinator of the previous start, if any, once its committed entries are applied.</summary>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="Errors.SquirixException">A committed entry of the previous coordinator stays unapplied; the resync is refused.</exception>
    private async Task RetireCoordinatorAsync()
    {
        if (_coordinator == null)
            return;

        // The old coordinator's retained entries are not recovered by the new one (it starts from the durable log status, with
        // an empty apply queue), so disposing it with entries still unapplied would lose them from memory. Apply the committed
        // ones first; while any stays pending (the apply keeps failing, or no majority covers it yet) refuse the resync and
        // keep the old coordinator, whose late-majority path and the next attempt can still apply them.
        if (!await TryApplyPendingAsync().ConfigureAwait(false))
            throw ServerOpContract.TooManyRequests(PendingApplyRefusalReason);

        // The old pipeline stops admitting and delivers what it queued to its followers, within the commit budget, before it closes, so
        // the new start verifies them as holding its log instead of leaving them to catch up. It also finishes before the new pipeline
        // exists, so its appends cannot reach a follower after the new pipeline's.
        if (_pipeline != null)
        {
            await _pipeline.DrainAsync(CommitBudget).ConfigureAwait(false);
            await this.CloseSendersAsync(_pipeline).ConfigureAwait(false);
        }

        await _coordinator.DisposeAsync().ConfigureAwait(false);
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
            ServerLog.ReplicaPendingApplyFailed(Log, error);
            return false;
        }
    }

    /// <summary>No-op fault hooks for production commits outside fault-injection tests.</summary>
    [Immutable]
    internal sealed class NoOpCommitHooks : IReplicaCommitFaultHooks
    {
        internal static NoOpCommitHooks Instance { get; } = new();

        public ValueTask OnStageAsync(ReplicaCommitStage stage, PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }
}
