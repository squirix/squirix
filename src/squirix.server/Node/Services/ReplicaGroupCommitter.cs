using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

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
    private const int MaxInFlight = 2;
    private const string CommitBudgetRefusalReason = "replica_commit_budget";
    private const string PendingApplyRefusalReason = "replica_apply_pending";
    private static readonly TimeSpan DefaultCommitBudget = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan DefaultShutdownBudget = TimeSpan.FromSeconds(30);

    private readonly Lazy<ReplicaLeaderApplier> _applier;
    private readonly AsyncLock _gate = new();
    private readonly IReplicaRpcGateway _gateway;
    private readonly ulong _generation;
    private readonly ILogicalNamespacedCache<object?> _local;
    private readonly IReplicaGroupLocator _locator;
    private readonly ReplicaGroupRegistry _registry;
    private readonly Lazy<ReplicaVerificationProbe> _probe;
    private readonly ReadOnlyMemory<byte> _topologyFingerprint;
    private ReplicaCommitCoordinator? _coordinator;
    private int _disposed;
    private ReplicaMutationFactory? _factory;
    private ReplicaGroupCommitPipeline? _pipeline;

    /// <summary>Whether the outcomes of the committed log entries were rebuilt; set once, after a start that rebuilt them completes it.</summary>
    private bool _outcomesRestored;

    private bool _started;

    /// <summary>Initializes a new instance of the <see cref="ReplicaGroupCommitter" /> class.</summary>
    /// <param name="registry">Replica group registry of this node.</param>
    /// <param name="locator">Replica group locator resolving the owned group members.</param>
    /// <param name="gateway">Follower replication RPCs.</param>
    /// <param name="local">Local cache pipeline used for prepare reads and memory applies.</param>
    /// <param name="selfId">This node identifier; the node owns the group with this identifier.</param>
    /// <param name="topology">Static topology fingerprint and configuration generation.</param>
    /// <param name="log">Logger for lifecycle failures.</param>
    internal ReplicaGroupCommitter(
        ReplicaGroupRegistry registry,
        IReplicaGroupLocator locator,
        IReplicaRpcGateway gateway,
        ILogicalNamespacedCache<object?> local,
        string selfId,
        in ReplicaTopologyStamp topology,
        ILogger<ReplicaGroupCommitter> log)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(locator);
        ArgumentNullException.ThrowIfNull(gateway);
        ArgumentNullException.ThrowIfNull(local);
        ArgumentException.ThrowIfNullOrWhiteSpace(selfId);
        ArgumentNullException.ThrowIfNull(log);
        Log = log;
        _registry = registry;
        _locator = locator;
        _gateway = gateway;
        _local = local;
        _applier = new Lazy<ReplicaLeaderApplier>(() => new ReplicaLeaderApplier(local, Log, selfId, selfId, Metrics), LazyThreadSafetyMode.ExecutionAndPublication);
        GroupId = selfId;
        _topologyFingerprint = topology.Fingerprint.IsEmpty ? throw new ArgumentException("Topology fingerprint must not be empty.", nameof(topology))
            : topology.Fingerprint;
        _generation = topology.Generation;
        _probe = new Lazy<ReplicaVerificationProbe>(
            () => new ReplicaVerificationProbe(registry, locator, gateway, selfId, _topologyFingerprint, _generation, Log),
            LazyThreadSafetyMode.ExecutionAndPublication);
        CommitBudget = DefaultCommitBudget;
        ShutdownBudget = DefaultShutdownBudget;
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

    /// <summary>Gets the time source that pins the expiration deadlines of prepared records; the system clock unless set.</summary>
    /// <remarks>Only the prepare of a mutation reads it. Applying a record never does.</remarks>
    internal TimeProvider Clock { private get; init; } = TimeProvider.System;

    /// <summary>Gets the identifier of the owned replica group, which is this node's identifier.</summary>
    internal string GroupId { get; }

    /// <summary>Gets the replication metrics counting the inconsistent log records the committer refuses to apply; none are counted unless set.</summary>
    internal ReplicationMetrics? Metrics { private get; init; }

    /// <summary>Gets the longest wait for an in-flight commit on dispose; 30 seconds unless set.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The budget is not positive.</exception>
    internal TimeSpan ShutdownBudget
    {
        private get;
        init
        {
            value.ThrowIfNegativeOrZero(nameof(value), "The shutdown budget must be greater than zero.");

            field = value;
        }
    }

    /// <summary>Gets the logger for lifecycle failures.</summary>
    private ILogger Log { get; }

    private ReplicaLeaderApplier Applier => _applier.Value;

    private ReplicaVerificationProbe Probe => _probe.Value;

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
                ServerLog.ReplicaCommitterLeakedOnShutdownTimeout(Log, ShutdownBudget);
                return;
            }
        }

        drain.Dispose();

        if (_coordinator != null)
            await _coordinator.DisposeAsync().ConfigureAwait(false);
        _gate.Dispose();
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
        using var guard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        ReplicaCommitCoordinator coordinator;
        ReplicaMutationFactory factory;
        try
        {
            (coordinator, factory) = await EnsureStartedAsync(true, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception) when (LookupRetained(write, state, fingerprint) is var retained && retained is GroupIdempotencyLookup.Unresolved or GroupIdempotencyLookup.Mismatch)
        {
            // A retry of an operation whose entry is appended but not yet committed (possibly by the process before a restart) must
            // neither re-execute nor be told it failed: its outcome stays unknown until a commit resolves the entry, which the retry
            // then replays. Whatever refused this attempt, only the unknown outcome is true for the operation. The same identifier
            // with another request is a reuse, reported as such whatever the state of the entry.
            throw retained == GroupIdempotencyLookup.Mismatch ? new ServerOpIdMismatchException() : ServerOpContract.CommitOutcomeUnknown();
        }

        var index = PeekNextIndex();
        var mutation = await prepare(factory, state, index, cancellationToken).ConfigureAwait(false);
        var outcome = await CommitWithPreAppendResyncAsync(coordinator, mutation).ConfigureAwait(false);
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

        var applied = Applier.AppliedIndex;
        var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        if (applied <= status.LastAppliedIndex)
            return;

        await durability.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
        var result = await log.AdvanceAppliedAsync(applied, cancellationToken).ConfigureAwait(false);
        if (!result.Success)
            throw new InvalidOperationException($"Local group applied advance was refused: {result.RefusalCode}.");
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
        // gate for the whole follower wait on every pass. The decisive check runs again under the gate.
        var eligibility = _registry.EligibilityFor(GroupId);
        if (Volatile.Read(ref _coordinator) is { } running)
        {
            var observed = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
            if (await ReplicaLogCompactionStep.AwaitFollowersAsync(running, eligibility, observed.CommitIndex, Clock, cancellationToken).ConfigureAwait(false) is { } refused)
                return refused;
        }

        using var guard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        return _started && _coordinator is { } coordinator
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
    /// Followers are probed, and re-sent the leader's uncommitted tail when they lack it, without holding the commit gate, so a dead
    /// or slow peer never delays writes. Only when some follower answered does the gate get taken to start the coordinator (which
    /// recovers the tail), re-check that the leader tail did not move, admit the verified slots, and commit what they now cover.
    /// </remarks>
    internal async Task<ReplicaVerification> VerifyReplicasAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        if (!_registry.TryGetLog(GroupId, out var log))
            return ReplicaVerification.Blocked;

        var snapshot = await Probe.ProbeAsync(log, cancellationToken).ConfigureAwait(false);
        if (snapshot.Verdict is { } verdict)
            return verdict;

        using var guard = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        return await AdmitVerifiedAsync(log, snapshot, cancellationToken).ConfigureAwait(false);
    }

    private static bool IsPostAppendOutcome(Exception error) =>
        error is InvalidOperationException && error.Message.StartsWith(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);

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
        var status = snapshot.Status;
        var probed = snapshot.Probed;

        // A coordinator that still retains entries is never replaced (its restart refuses): the verified slots are admitted into it,
        // and its resolver commits and applies what they now cover. Otherwise the coordinator starts here, recovering the log tail.
        var coordinator = !await TryApplyPendingAsync().ConfigureAwait(false) && _coordinator is { } retained ? retained
            : (await EnsureStartedAsync(false, cancellationToken).ConfigureAwait(false)).Coordinator;
        var current = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);

        // A commit may have moved the tail between the unguarded probe and the gate: the verdicts then describe
        // an older tail, so the slots that answered are probed again against the current one.
        if (current.LastLogIndex != status.LastLogIndex || current.LastLogTerm != status.LastLogTerm)
            probed = await ReplicaReadinessProbe.ProbeAllAsync(_gateway, snapshot.Answered, snapshot.Members, snapshot.Header, current, ReplicaVerificationProbe.ProbeTimeout, cancellationToken).ConfigureAwait(false);

        // StartAsync may have verified some of these slots while this call waited for the gate: an older verdict
        // must not demote them.
        var eligibility = _registry.EligibilityFor(GroupId);
        for (var i = 1; i < probed.Length; i++)
        {
            if (eligibility.CanCountInWriteQuorum(i))
                probed[i] = default;
        }

        ReplicaReadinessProbe.ApplyAll(eligibility, probed, in current, _topologyFingerprint, _generation, coordinator);
        var applied = await TryApplyPendingAsync().ConfigureAwait(false);
        return applied && eligibility.AllCanCountInWriteQuorum() ? ReplicaVerification.AllReady : ReplicaVerification.Pending;
    }

    private async ValueTask<ReadOnlyMemory<byte>> CommitWithPreAppendResyncAsync(ReplicaCommitCoordinator coordinator, PreparedReplicaMutation mutation)
    {
        try
        {
            return await coordinator.CommitAsync(mutation, CommitBudget, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception error) when (IsPostAppendOutcome(error))
        {
            // A durable majority may hold the entry: keep the reservation and sequencing untouched and report the stable contract
            // (gRPC Unavailable with COMMIT_OUTCOME_UNKNOWN), so callers stop instead of retrying under a new identity.
            ServerLog.ReplicaCommitOutcomeUnknown(Log, error);
            throw ServerOpContract.CommitOutcomeUnknown();
        }
        catch (InvalidOperationException error) when (error.Message.StartsWith(ReplicaCommitCoordinator.IdempotencyCapacityCode, StringComparison.Ordinal))
        {
            // Refused when the identity was reserved, before anything was appended: the pipeline positions stand, so the started state
            // is kept, and the caller gets a retryable refusal while older outcomes age out.
            throw ServerOpContract.TooManyRequests(ReplicaCommitCoordinator.IdempotencyCapacityCode);
        }
        catch (InvalidOperationException error) when (error.Message.StartsWith(ReplicaCommitCoordinator.FingerprintMismatchCode, StringComparison.Ordinal))
        {
            // Refused the same way at the lookup, as a reuse of the identifier.
            throw new ServerOpIdMismatchException();
        }
        catch (Exception error)
        {
            // The local appending was refused before anything was marked appended: an interrupted
            // append may leave the durable log ahead of the pipeline positions, so drop the started
            // state and rebuild from status.LastLogIndex on the next attempt.
            _started = false;

            // The log may still hold the entry (its frames were durable when the write behind them failed): the next start recovers and
            // pins it as the tail, and a majority may commit it, so the caller must not be told the write was refused.
            if (await HoldsEntryAsync(_registry, GroupId, mutation).ConfigureAwait(false))
            {
                ServerLog.ReplicaCommitOutcomeUnknown(Log, error);
                throw ServerOpContract.CommitOutcomeUnknown();
            }

            // The commit runs on the budget only, so a cancellation here is the budget expiring before the append: a definite refusal.
            if (error is OperationCanceledException)
                throw ServerOpContract.TooManyRequests(CommitBudgetRefusalReason);
            throw;
        }

        static async ValueTask<bool> HoldsEntryAsync(ReplicaGroupRegistry registry, string groupId, PreparedReplicaMutation mutation)
        {
            if (!registry.TryGetLog(groupId, out var log))
                return false;

            try
            {
                var status = await log.GetStatusAsync(CancellationToken.None).ConfigureAwait(false);
                return status.LastLogIndex >= mutation.LogIndex && await log.GetTermAtAsync(mutation.LogIndex, CancellationToken.None).ConfigureAwait(false) == mutation.Term;
            }
            catch (ObjectDisposedException)
            {
                // A closed log cannot tell; the original failure is reported as it is.
                return false;
            }
        }
    }

    /// <summary>Starts the coordinator when needed and, for a write, checks that it may be prepared and appended now.</summary>
    /// <param name="write">Whether a write is to be prepared next; <see langword="false" /> for verification.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The running coordinator and mutation factory.</returns>
    /// <exception cref="SquirixException">An appended entry is not yet applied (too many requests).</exception>
    /// <exception cref="InvalidOperationException">The write has no verified majority, or the committer is not started.</exception>
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
            (false, _, _, _) => throw new InvalidOperationException("Replica group has no verified write majority; the write was refused before the local append."),
            (true, false, _, _) => throw ServerOpContract.TooManyRequests(PendingApplyRefusalReason),
            (true, true, { } coordinator, { } factory) => (coordinator, factory),
            _ => throw new InvalidOperationException("Replica group committer is not started."),
        };
    }

    /// <summary>Looks up the entry retained in the owned group log for the identity of a write.</summary>
    /// <typeparam name="TState">The type of the write arguments.</typeparam>
    /// <param name="write">The cache scope and the client operation identifier of the write.</param>
    /// <param name="state">The arguments of the write.</param>
    /// <param name="fingerprint">Computes the operation fingerprint of the write.</param>
    /// <returns>The lookup; a miss when the owned group log is not open.</returns>
    /// <remarks>The fingerprint, which encodes and hashes the request, is computed only once an entry with the identity is found.</remarks>
    private GroupIdempotencyLookup LookupRetained<TState>((string Scope, string OperationId) write, TState state, Func<TState, byte[]> fingerprint)
    {
        return !_registry.TryGetLog(GroupId, out var log) || log.Idempotency.Lookup(write.Scope, write.OperationId, [], out _) == GroupIdempotencyLookup.Miss
            ? GroupIdempotencyLookup.Miss
            : log.Idempotency.Lookup(write.Scope, write.OperationId, fingerprint(state), out _);
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
            throw new InvalidOperationException($"This node does not serve its owned replica group '{GroupId}'.");

        await RetireCoordinatorAsync().ConfigureAwait(false);

        // One read pairs the status with its tail: a commit left running by the disposed coordinator may still advance the log.
        var read = await log.GetLeaderTailAsync(cancellationToken).ConfigureAwait(false);
        var status = read.Status;

        // Memory must hold every committed entry before anything new is prepared.
        await Applier.CatchUpAsync(log, status.LastAppliedIndex, status.CommitIndex, cancellationToken).ConfigureAwait(false);

        var term = Math.Max(1UL, status.CurrentTerm);
        var (members, header) = Probe.BuildMembership(term);
        var tail = ReplicaLeaderTail.From(read);

        // A restart with durable progress leaves every slot recovering. Verify the leader's own log and every follower against its
        // last entry before the first commit, so the quorum is built from verified slots only. An uncommitted tail is recovered by the
        // coordinator and commits once verified slots hold it; followers lacking it are re-sent it by verification, outside this gate.
        var eligibility = _registry.EligibilityFor(GroupId);
        ReplicaReadinessProbe.MarkLeaderReady(eligibility, in status, _topologyFingerprint, _generation);
        var results = eligibility.CanCountInWriteQuorum(0)
            ? await ReplicaReadinessProbe.ProbeAllAsync(_gateway, ReplicaReadinessProbe.NonReadyFollowers(eligibility), members, header, status, ReplicaVerificationProbe.ProbeTimeout, cancellationToken)
                                         .ConfigureAwait(false)
            : [];

        // The coordinator pins the tail in the log's idempotency state, which durable truncation releases pins from.
        var pipeline = new ReplicaGroupCommitPipeline(Applier, log, _gateway, members, GroupId, in status, in header);
        var factory = new ReplicaMutationFactory(_local, GroupId, term, Clock, Log);
        _coordinator = new ReplicaCommitCoordinator(
            new ReplicaCommitCoordinatorOptions(_locator.ReplicaCount, status.LastLogIndex, status.CommitIndex, MaxInFlight),
            pipeline,
            NoOpCommitHooks.Instance,
            log.Idempotency,
            eligibility,
            Applier.RecoverTail(tail, term, factory))
        {
            ShutdownLeakReporter = budget => ServerLog.ReplicaCoordinatorLeakedOnShutdown(Log, budget),
        };

        // Before a restart the outcomes of the committed entries above the snapshot lived only in memory; their records carry them, so
        // a retry of an operation committed before the restart replays its outcome. The recovered tail is pinned first and keeps its pins.
        if (!_outcomesRestored)
            await RestoreOutcomesAsync(log, cancellationToken).ConfigureAwait(false);

        // Verified slots are admitted at the leader's last index before they count, so they cover the recovered tail.
        ReplicaReadinessProbe.ApplyAll(eligibility, results, in status, _topologyFingerprint, _generation, _coordinator);
        _factory = factory;
        _pipeline = pipeline;
        _started = true;
    }

    /// <summary>Rebuilds the outcomes of the committed log entries once, after the coordinator of the first start pinned the recovered tail.</summary>
    /// <param name="log">The owned group log.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <remarks>
    /// A failure drops the new coordinator before it serves anything: it would keep the recovered tail with no follower progress recorded,
    /// refusing the next writes. Its tail is durable, so the next start pins it again and retries the rebuild.
    /// </remarks>
    private async Task RestoreOutcomesAsync(IFollowerLog log, CancellationToken cancellationToken)
    {
        try
        {
            var restored = await ReplicaOutcomeRecovery.RestoreAsync(log, Clock, cancellationToken).ConfigureAwait(false);
            _outcomesRestored = true;
            ServerLog.ReplicaOutcomesRestored(Log, GroupId, restored);
        }
        catch
        {
            var coordinator = _coordinator;
            _coordinator = null;
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
        private readonly ReplicaLeaderApplier _applier;
        private readonly ReplicaRpcHeader _header;
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
        /// <param name="applier">The committer's applier, which applies committed entries to memory in log order.</param>
        /// <param name="log">Owned group log for local durable appending.</param>
        /// <param name="rpc">Follower replication RPCs.</param>
        /// <param name="members">Ordered group members; index zero is this node.</param>
        /// <param name="selfId">This node identifier, matching <paramref name="members" /> index zero.</param>
        /// <param name="status">Durable log status seeding previous and commit positions.</param>
        /// <param name="header">Replication envelope identity for follower calls.</param>
        internal ReplicaGroupCommitPipeline(
            ReplicaLeaderApplier applier,
            IFollowerLog log,
            IReplicaRpcGateway rpc,
            string[] members,
            string selfId,
            in FollowerLogStatus status,
            in ReplicaRpcHeader header)
        {
            ArgumentNullException.ThrowIfNull(applier);
            ArgumentNullException.ThrowIfNull(log);
            ArgumentNullException.ThrowIfNull(rpc);
            ArgumentNullException.ThrowIfNull(members);
            ArgumentException.ThrowIfNullOrWhiteSpace(selfId);
            if (members.Length == 0 || !string.Equals(members[0], selfId, StringComparison.Ordinal))
                throw new ArgumentException("Group members must start with this node.", nameof(members));

            _applier = applier;
            _log = log;
            _rpc = rpc;
            _members = members;
            _selfId = selfId;
            _header = header;
            _prevLogIndex = status.LastLogIndex;
            _prevLogTerm = status.LastLogTerm;
            _commitIndex = status.CommitIndex;
        }

        /// <summary>Gets the group log index after the last entry this pipeline appended locally, or after the seeded status.</summary>
        /// <remarks>Read by the committer under its gate, between commits, once the commit that last appended has completed.</remarks>
        internal ulong NextLogIndex => _prevLogIndex + 1;

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
        /// <remarks>Every entry the coordinator applies, its own and those a late majority commits, advances the committer's applied index.</remarks>
        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) =>
            _applier.ApplyAsync(mutation.LogIndex, mutation.CanonicalPayload, cancellationToken);

        /// <inheritdoc />
        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
            // Repair driving lands in a later milestone; the coordinator already observes stragglers
            // in the background, and a lagging replica simply stops counting toward the majority.
        }
    }
}
