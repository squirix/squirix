using System;
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
/// <para>
/// Commits run one at a time per group under <see cref="AsyncLock" />: log indexes stay dense with no
/// gaps, prepare-time reads stay exact through the ordered applying, and the coordinator never observes
/// admission pressure or turn waits. The coordinator itself is never canceled; a fixed commit budget
/// bounds every attempt up to its durable majority, and idempotent retries recover unknown outcomes.
/// </para>
/// <para>
/// This type owns the state of the running coordinator and its lifecycle. The operations built on it are in
/// <see cref="ReplicaGroupCommitterCommits" />, <see cref="ReplicaGroupCommitterStarts" />, <see cref="ReplicaGroupCommitterVerification" />,
/// <see cref="ReplicaGroupCommitterRepair" />, <see cref="ReplicaGroupCommitterLeadership" />, <see cref="ReplicaGroupCommitterCompaction" />,
/// and <see cref="ReplicaGroupCommitterWrites" />.
/// </para>
/// </remarks>
internal sealed class ReplicaGroupCommitter : IAsyncDisposable
{
    internal const int MaxInFlight = 2;
    internal const string CommitBudgetRefusalReason = "replica_commit_budget";
    private const string PendingApplyRefusalReason = "replica_apply_pending";
    private static readonly TimeSpan DefaultCommitBudget = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan DefaultShutdownBudget = TimeSpan.FromSeconds(30);

    private readonly Lazy<ReplicaExpirationCoordinator<NodeCacheEntry<object?>>> _expiration;
    private readonly IReplicaGroupLocator _locator;
    private readonly Lazy<ReplicaVerificationProbe> _probe;
    private ReplicaCommitCoordinator? _coordinator;
    private int _disposed;
    private ReplicaMutationFactory? _factory;
    private ReplicaGroupCommitPipeline? _pipeline;

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
        Registry = registry;
        _locator = locator;
        Gateway = gateway;
        Local = local;
        GroupId = identity.GroupId;
        Topology = topology.Fingerprint.IsEmpty ? throw new ArgumentException("Topology fingerprint must not be empty.", nameof(topology)) : topology;
        _probe = new Lazy<ReplicaVerificationProbe>(
            () => new ReplicaVerificationProbe(registry, locator, gateway, identity, Topology.Fingerprint, Topology.Generation, Log),
            LazyThreadSafetyMode.ExecutionAndPublication);
        CommitBudget = DefaultCommitBudget;
        ShutdownBudget = DefaultShutdownBudget;

        // Created on first use, after the init-only budgets are set. One run of the expiry serves every caller of the key, so it takes no
        // caller token: the commit budget bounds the wait before the append, and the commit itself is budget-bounded.
        _expiration = new Lazy<ReplicaExpirationCoordinator<NodeCacheEntry<object?>>>(
            () => new ReplicaExpirationCoordinator<NodeCacheEntry<object?>>((cacheName, key) => this.CommitExpiryAsync(cacheName, key))
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

    /// <summary>Gets or initializes the election state of the group; when set, this committer leads only a term an election hands it.</summary>
    /// <remarks>
    /// Without it the committer leads the own group statically, in the term of its log, as long as the node runs. With it nothing starts
    /// before <see cref="ReplicaGroupCommitterLeadership.PromoteAsync" />: a promotion appends a leader-term entry of the won term, every
    /// follower reply is posted to the state, and the committer stops leading at <see cref="RetireAsync" />.
    /// </remarks>
    internal ReplicaGroupState? Election { get; init; }

    /// <summary>Gets the pipeline of the running coordinator, whose senders carry the heartbeats of an elected leader.</summary>
    internal ReplicaGroupCommitPipeline? RunningPipeline => Volatile.Read(ref _pipeline);

    /// <summary>Gets the current leadership of the group by election, or <see langword="null" /> while there is none.</summary>
    internal ReplicaLeaderTenure? Tenure => Volatile.Read(ref _tenure);

    /// <summary>Gets or initializes the applier of the led group, which this committer drives for as long as it leads the group.</summary>
    /// <remarks>
    /// The applier lives as long as the node, so its applied index survives a replaced coordinator; while this committer leads the group
    /// it is the only caller of the applier's catch-up and applies.
    /// </remarks>
    /// <exception cref="ArgumentException">The applier serves another group.</exception>
    internal required ReplicaGroupApplier Applier
    {
        get;
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

    /// <summary>Gets the commit gate: appends, commits, applies, starts, and retirements of the group run one at a time under it.</summary>
    internal AsyncLock Gate { get; } = new();

    /// <summary>Gets the follower replication RPCs.</summary>
    internal IReplicaRpcGateway Gateway { get; }

    /// <summary>Gets the local cache pipeline used for prepare reads and memory applies.</summary>
    internal ILogicalNamespacedCache<object?> Local { get; }

    /// <summary>Gets the replica group registry of this node.</summary>
    internal ReplicaGroupRegistry Registry { get; }

    /// <summary>Gets the static topology fingerprint and configuration generation.</summary>
    internal ReplicaTopologyStamp Topology { get; }

    /// <summary>Gets the running coordinator, or <see langword="null" /> before the first start and after a retirement.</summary>
    /// <remarks>Read under the commit gate; <see cref="ReadCoordinator" /> reads it outside the gate.</remarks>
    internal ReplicaCommitCoordinator? Coordinator => _coordinator;

    /// <summary>Gets a value indicating whether the coordinator is started and its pipeline positions follow the durable log.</summary>
    /// <remarks>Read and written under the commit gate.</remarks>
    internal bool IsStarted { get; private set; }

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

        // Drain in-flight committer operations holding the gate so their AsyncLockHolder can release
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
            drain = await Gate.LockAsync(budget.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            ServerLog.ReplicaCommitterLeakedOnShutdownTimeout(Log, ShutdownBudget);
            Gate.Dispose();
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
                Gate.Dispose();
            }
        }
    }

    /// <summary>Drops the started state, so the next attempt rebuilds the pipeline positions from the durable log status.</summary>
    /// <remarks>Runs under the commit gate.</remarks>
    internal void DropStartedState() => IsStarted = false;

    /// <summary>Reads the running coordinator outside the commit gate.</summary>
    /// <returns>The running coordinator, or <see langword="null" /> when there is none.</returns>
    internal ReplicaCommitCoordinator? ReadCoordinator() => Volatile.Read(ref _coordinator);

    /// <summary>Takes the leadership of a won term: the applier lease first, then the tenure; a leadership already taken is kept.</summary>
    /// <param name="term">The won term.</param>
    /// <param name="cancellationToken">Cancellation token; it ends the wait for the applier lease.</param>
    /// <returns>The leadership of <paramref name="term" />.</returns>
    /// <exception cref="InvalidOperationException">The committer still leads another term.</exception>
    /// <remarks>Called by the promotion only, which the election driver serializes.</remarks>
    internal async Task<ReplicaLeaderTenure> TakeTenureAsync(ulong term, CancellationToken cancellationToken)
    {
        if (Tenure is { } tenure)
        {
            return tenure.Term == term
                ? tenure
                : throw new InvalidOperationException($"Replica group '{GroupId}' still leads term {tenure.Term}; it cannot be promoted to term {term}.");
        }

        await Applier.DriverLease.LeadAsync(cancellationToken).ConfigureAwait(false);
        _tenure = new ReplicaLeaderTenure(term, Applier.DriverLease);
        return _tenure;
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
        if (Tenure is not { } tenure)
            return true;

        using var guard = await Gate.LockAsync(cancellationToken).ConfigureAwait(false);
        if (!await TryApplyPendingAsync().ConfigureAwait(false) && _coordinator is { } retained && Applier.AppliedIndex < retained.CommitIndex)
            return false;

        if (_pipeline != null)
            await this.CloseSendersAsync(_pipeline).ConfigureAwait(false);

        if (_coordinator != null)
            await _coordinator.DisposeAsync().ConfigureAwait(false);

        ClearRun();
        Volatile.Write(ref _tenure, null);
        await tenure.EndAsync().ConfigureAwait(false);
        return true;
    }

    /// <summary>Throws once this committer is disposing or disposed.</summary>
    /// <exception cref="ObjectDisposedException">The committer is disposing or disposed.</exception>
    internal void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

    /// <summary>Waits until local recovery has replayed the journal into memory, so no decision is prepared against a partly recovered cache.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes once the journal startup gate is open.</returns>
    /// <remarks>Must run before the commit gate is taken: a commit queued on the gate never waits for recovery while holding it.</remarks>
    internal async ValueTask WaitForLocalRecoveryAsync(CancellationToken cancellationToken)
    {
        if (_recovered)
            return;

        await Recovery.WaitForStartupAsync(cancellationToken).ConfigureAwait(false);
        _recovered = true;
    }

    /// <summary>Starts the coordinator when needed and, for a write, checks that it may be prepared and appended now.</summary>
    /// <param name="write">Whether a write is to be prepared next; <see langword="false" /> for verification.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The running coordinator and mutation factory.</returns>
    /// <exception cref="Grpc.Core.RpcException">
    /// The write has no verified majority (Unavailable) or no authorized leadership in the led term (stale-term, stale-owner or Unavailable):
    /// nothing was written.
    /// </exception>
    /// <exception cref="SquirixException">An appended entry is not yet applied (too many requests).</exception>
    /// <exception cref="InvalidOperationException">The committer is not started.</exception>
    /// <remarks>Runs under the commit gate.</remarks>
    internal async Task<(ReplicaCommitCoordinator Coordinator, ReplicaMutationFactory Factory)> EnsureStartedAsync(bool write, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();

        // A write that passed the write gate may reach a leadership that lost its authority, or a later one whose leader-term entry is not
        // committed yet: refused the same way.
        if (write)
            this.ThrowIfNoWriteAuthority();
        if (!IsStarted)
            await (write ? this.StartForWriteAsync(cancellationToken) : StartAsync(cancellationToken)).ConfigureAwait(false);

        // Refused before anything is appended: a write that cannot reach a majority would leave an uncommitted local tail.
        // Dropping the started state re-probes the followers on the next write. Decisions are prepared from live memory, so an
        // entry that is appended but not yet applied would leave the decision blind to its effect: such a
        // write fails definitely and may be retried; only this gate appends, so the check cannot go stale before the prepare.
        var majority = !write || Registry.EligibilityFor(GroupId).HasWriteMajority();
        var applied = !write || (majority && await TryApplyPendingAsync().ConfigureAwait(false));
        if (!majority)
            DropStartedState();

        // The start and the apply may wait: the authority is checked again last, right before the prepare and the local append.
        if (write && majority && applied)
            this.ThrowIfNoWriteAuthority();

        return (majority, applied, _coordinator, _factory) switch
        {
            (false, _, _, _) => throw ServerOpContract.NoWriteMajority(),
            (true, false, _, _) => throw ServerOpContract.TooManyRequests(PendingApplyRefusalReason),
            (true, true, { } coordinator, { } factory) => (coordinator, factory),
            _ => throw new InvalidOperationException("Replica group committer is not started."),
        };
    }

    /// <summary>Returns the next group log index to prepare with: the one after the last entry appended to the local log.</summary>
    /// <returns>The next log index.</returns>
    /// <remarks>
    /// The index follows the local appends of the running pipeline, so it moves only when an entry is appended: a prepare that fails, a
    /// refusal before the append, and a retry the coordinator answers from the idempotency state without appending all leave it for the
    /// next write, and the durable log stays dense.
    /// </remarks>
    /// <exception cref="InvalidOperationException">The committer is not started.</exception>
    internal ulong PeekNextIndex() => ThrowHelper.Required(_pipeline, "Replica group committer is not started.").NextLogIndex;

    /// <summary>Starts a coordinator over the durable log, replacing the one of the previous start once its committed entries are applied.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes once the coordinator is started.</returns>
    /// <exception cref="InvalidOperationException">This node does not serve the group, or the log moved past the led term.</exception>
    /// <exception cref="Grpc.Core.RpcException">The leadership by election is retired: Unavailable, nothing was written.</exception>
    /// <remarks>Runs under the commit gate, and only while the committer is not started.</remarks>
    internal async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!Registry.TryGetLog(GroupId, out var log))
            throw new InvalidOperationException($"This node does not serve the replica group '{GroupId}' it leads.");

        // A start after retirement refuses like the write gate: no leader with authority here, nothing written.
        var tenure = Election == null || _tenure != null ? _tenure : throw ServerOpContract.NoLeaderAuthority();
        var replacing = _coordinator != null;
        await RetireCoordinatorAsync().ConfigureAwait(false);

        var (pipeline, factory, read, term, eligibility, results) = await this.LaunchAsync(log, tenure, replacing, cancellationToken).ConfigureAwait(false);
        var status = read.Status;
        var leaderIndex = Probe.LeaderReplicaIndex;
        _pipeline = pipeline;
        _coordinator = this.CreateCoordinator((_locator.ReplicaCount, leaderIndex), pipeline, log, in status, eligibility, Applier.RecoverTail(ReplicaLeaderTail.From(read), term, factory));

        // Before a restart the outcomes of the committed entries above the snapshot lived only in memory; their records carry them, so
        // a retry of an operation committed before the restart replays its outcome. The recovered tail is pinned first and keeps its pins.
        if (!log.Idempotency.OutcomesRebuilt)
            await RestoreOutcomesAsync(log, pipeline, cancellationToken).ConfigureAwait(false);

        // Verified slots are admitted at the leader's last index before they count, so they cover the recovered tail.
        ReplicaReadinessProbe.ApplyAll(eligibility, leaderIndex, results, in status, Topology.Fingerprint, Topology.Generation, _coordinator);
        _factory = factory;
        IsStarted = true;
    }

    /// <summary>Applies the committed entries the current coordinator still retains.</summary>
    /// <returns><see langword="true" /> when no coordinator retains an unapplied entry.</returns>
    /// <remarks>
    /// An apply failure is logged and reported as <see langword="false" />: the entry stays retained and the caller refuses
    /// definitely, before anything of its own is appended.
    /// </remarks>
    internal async Task<bool> TryApplyPendingAsync()
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

    /// <summary>Forgets the coordinator, the pipeline, and the mutation factory of the last start, and drops the started state.</summary>
    /// <remarks>Runs under the commit gate, after the pipeline and the coordinator are closed or are being closed.</remarks>
    private void ClearRun()
    {
        _coordinator = null;
        Volatile.Write(ref _pipeline, null);
        _factory = null;
        DropStartedState();
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
            ClearRun();
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

    /// <summary>No-op fault hooks for production commits outside fault-injection tests.</summary>
    [Immutable]
    internal sealed class NoOpCommitHooks : IReplicaCommitFaultHooks
    {
        internal static NoOpCommitHooks Instance { get; } = new();

        public ValueTask OnStageAsync(ReplicaCommitStage stage, PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }
}
