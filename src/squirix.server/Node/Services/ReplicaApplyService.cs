using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Applies the committed entries of every served group this node does not lead to local memory, densely in log order.</summary>
/// <remarks>
/// Each follower group runs its own loop, the single caller of the group's applier: it catches memory up through the durable commit
/// index, then waits until the follower path signals new entries, or until a fallback interval elapses. Nothing is applied before local
/// recovery has replayed the cache journal into memory. A committed record that cannot be applied stops only its group: it is logged as
/// an error and every later entry of that group stays unapplied until a state transfer reseeds it, while the other groups go on. Storage and journal faults, a full
/// journal, and memory that refuses the write are logged when they change and retried on the next pass; anything else faults the service. The service runs on the host lifetime and stops
/// with it; a restart applies again what memory lacks. While committed entries are pending and such a fault repeats, or a committed
/// record stopped the group, the group is reported blocked to the election driver, see <see cref="ReplicaApplyBlock" />.
/// </remarks>
internal sealed class ReplicaApplyService : BackgroundService
{
    /// <summary>The longest wait between two passes of a group when no signal arrives.</summary>
    private static readonly TimeSpan FallbackInterval = TimeSpan.FromSeconds(1);

    private readonly ReplicaGroupAppliers _appliers;
    private readonly ReplicaGroupCommitters _committers;
    private readonly ILogger<ReplicaApplyService> _log;
    private readonly IJournalCoordinatorLifecycle _recovery;
    private readonly ReplicaGroupRegistry _registry;
    private readonly TimeProvider _timeProvider;

    /// <summary>Initializes a new instance of the <see cref="ReplicaApplyService" /> class.</summary>
    /// <param name="registry">Replica group registry holding the follower group logs and their apply signals.</param>
    /// <param name="appliers">The appliers of the served groups.</param>
    /// <param name="committers">The committers of the led groups, whose appliers they drive themselves.</param>
    /// <param name="recovery">The journal lifecycle whose startup gate opens once local recovery has replayed the journal into memory.</param>
    /// <param name="log">Logger reporting stopped groups and retried passes.</param>
    /// <param name="timeProvider">Time source of the fallback interval.</param>
    internal ReplicaApplyService(
        ReplicaGroupRegistry registry,
        ReplicaGroupAppliers appliers,
        ReplicaGroupCommitters committers,
        IJournalCoordinatorLifecycle recovery,
        ILogger<ReplicaApplyService> log,
        TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(appliers);
        ArgumentNullException.ThrowIfNull(committers);
        ArgumentNullException.ThrowIfNull(recovery);
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(timeProvider);
        _registry = registry;
        _appliers = appliers;
        _committers = committers;
        _recovery = recovery;
        _log = log;
        _timeProvider = timeProvider;
    }

    /// <inheritdoc />
    /// <remarks>
    /// The service ends only once every loop has ended, so no loop still applies a batch after the host stopped the node journal. A loop
    /// that faults cancels the others, and the service ends with its fault once they have ended, so the host stops. A loop that a
    /// committed record stopped keeps waiting until a state transfer replaces the copy or the host stops, and leaves the others running.
    /// </remarks>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await _recovery.WaitForStartupAsync(stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Host shutdown before recovery finished: nothing was applied.
            return;
        }

        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var groupIds = _appliers.GroupIds;
        var loops = new List<Task>(groupIds.Count);
        for (var i = 0; i < groupIds.Count; i++)
        {
            // A group led statically from the start gets no loop: its committer drives its applier for the node lifetime. A group the
            // election may hand this node keeps its loop, which skips its passes while a committer leads the group.
            if (!_committers.Leads(groupIds[i]))
                loops.Add(ApplyLoopAsync(groupIds[i], stopping.Token));
            else
                _registry.StateFor(groupIds[i]).ApplyBlock.Assess();
        }

        // Without a failed loop, every loop ended with the host: memory keeps what was applied, and a restart applies the committed entries
        // above the durable applied index again.
        await ReplicaGroupLoops.AwaitAllAsync(loops, stopping).ConfigureAwait(false);
    }

    /// <summary>Makes the commit index the leader announced durable, so the pass applies only entries the log holds as committed.</summary>
    /// <param name="log">The group log.</param>
    /// <param name="status">The status the pass read.</param>
    /// <param name="stoppingToken">The host stopping token.</param>
    /// <returns>The status the pass applies through: the one read again after the commit index was made durable, or <paramref name="status" /> when there was nothing to make durable.</returns>
    /// <remarks>
    /// One metadata write covers every append since the previous pass. The write precedes the apply, so memory never holds an entry the durable commit index
    /// does not cover. The announced index is read again after the write, since appends go on while the pass runs, and what a pass leaves is made durable by the next one.
    /// </remarks>
    private static async Task<FollowerLogStatus> MakeCommitDurableAsync(IFollowerLog log, FollowerLogStatus status, CancellationToken stoppingToken)
    {
        if (status.CommitHint <= status.CommitIndex)
            return status;

        _ = await log.AdvanceCommitAsync(status.CommitHint, stoppingToken).ConfigureAwait(false);
        return await log.GetStatusAsync(stoppingToken).ConfigureAwait(false);
    }

    /// <summary>Applies the committed entries of one follower group until the host stops.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="stoppingToken">The host stopping token.</param>
    /// <returns>A task that completes when the host stops, or at once when the group has no log.</returns>
    /// <remarks>
    /// A committed record that cannot be applied stops the group: no pass runs, and the group stays blocked, until a state transfer
    /// resets its applied index. The loop then runs passes again, and the first one that applies everything committed clears the block.
    /// </remarks>
    private async Task ApplyLoopAsync(string groupId, CancellationToken stoppingToken)
    {
        if (!_registry.TryGetLog(groupId, out var log))
        {
            ServerLog.ReplicaFollowerApplyNoLog(_log, groupId);
            return;
        }

        var applier = _appliers.For(groupId);
        var signal = _registry.ApplySignalFor(groupId);
        var block = _registry.StateFor(groupId).ApplyBlock;
        (Type Type, ulong AppliedIndex)? reported = null;
        long? stoppedAt = null;
        var skipping = false;
        while (true)
        {
            // A stopped group waits for a state transfer to reseed it; a pass would only meet the same record again.
            if (stoppedAt is { } reseeds && applier.Reseeds == reseeds)
            {
                // Nothing to do until the signal of the transfer.
            }
            else if (!applier.DriverLease.TryEnterPass())
            {
                // A committer leading the group by election drives the applier itself, and a state transfer replaces it: the pass is
                // skipped until the lease is handed back. A committer applies the group and the leader deadlines judge it, so a block left
                // by an earlier pass is stale; a stopped group stays blocked until a pass applies it.
                if (stoppedAt == null)
                    block.Clear();

                if (!skipping)
                    ServerLog.ReplicaPassSkippedWhileLeading(_log, groupId, "apply");

                skipping = true;
            }
            else
            {
                skipping = false;
                (stoppedAt, reported) = await RunPassAsync(groupId, (applier, log, block), reported, stoppingToken).ConfigureAwait(false);
            }

            if (await signal.WaitAsync(FallbackInterval, _timeProvider, stoppingToken).ConfigureAwait(false) == TimedWaitResult.Canceled)
                return;
        }
    }

    /// <summary>Runs one apply pass the caller entered, and tells whether it stopped the group.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="group">The group applier, the group log, and the flag that tells the election driver whether the apply is blocked.</param>
    /// <param name="reported">The fault last logged, if any.</param>
    /// <param name="stoppingToken">The host stopping token.</param>
    /// <returns>The number of reseeds when a committed record stopped the group, otherwise <see langword="null" />; and the fault logged last.</returns>
    private async Task<(long? StoppedAt, (Type Type, ulong AppliedIndex)? Reported)> RunPassAsync(
        string groupId,
        (ReplicaGroupApplier Applier, IFollowerLog Log, ReplicaApplyBlock Block) group,
        (Type Type, ulong AppliedIndex)? reported,
        CancellationToken stoppingToken)
    {
        // Read under the lease: a reseed runs under it too, so none is missed between the pass and this read.
        var seen = group.Applier.Reseeds;
        var (stopped, last) = await ApplyPassAsync(groupId, group, reported, stoppingToken).ConfigureAwait(false);
        return (stopped ? seen : null, last);
    }

    /// <summary>Runs one apply pass the caller entered, and ends it.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="group">The group applier, the group log, and the flag that tells the election driver whether the apply is blocked.</param>
    /// <param name="reported">The fault last logged, if any.</param>
    /// <param name="stoppingToken">The host stopping token.</param>
    /// <returns>Whether a committed record stopped the group, and the fault logged last.</returns>
    private async Task<(bool Stopped, (Type Type, ulong AppliedIndex)? Reported)> ApplyPassAsync(
        string groupId,
        (ReplicaGroupApplier Applier, IFollowerLog Log, ReplicaApplyBlock Block) group,
        (Type Type, ulong AppliedIndex)? reported,
        CancellationToken stoppingToken)
    {
        try
        {
            await CatchUpAsync(groupId, group, stoppingToken).ConfigureAwait(false);
            return (false, null);
        }
        catch (InvalidDataException exception)
        {
            // The record never reached memory, and every later entry depends on it: the group stops here instead of skipping it, and
            // a win of it could never apply its backlog either.
            group.Block.Block();
            ServerLog.ReplicaFollowerApplyStopped(_log, groupId, exception);
            return (true, reported);
        }
        catch (Exception exception) when (ReplicaRetryableFault.Is(exception) && !stoppingToken.IsCancellationRequested)
        {
            // The same fault repeats on every pass until it clears, so it is logged when its kind or the entry it stops at changes, not on
            // every retry; its message may name the commit index, which moves on while the fault stays.
            var fault = (exception.GetType(), group.Applier.AppliedIndex);
            if (reported != fault)
                ServerLog.ReplicaFollowerApplyRetry(_log, groupId, exception);

            return (false, fault);
        }
        finally
        {
            group.Applier.DriverLease.ExitPass();
        }
    }

    /// <summary>Applies the committed entries of a ready group log that memory lacks; a copy that is not complete applies nothing.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="group">The group applier, the group log, and the flag that tells the election driver whether the apply is blocked.</param>
    /// <param name="stoppingToken">The host stopping token.</param>
    /// <returns>A task that completes when memory holds every entry through the commit index read at the start.</returns>
    /// <remarks>
    /// The first pass over a ready log rebuilds the outcomes of its committed entries first, as the leader committer does for its own
    /// group: the applier records the outcome of each entry it applies, and a retry of an operation committed before the restart then
    /// replays its outcome on this node too. A rebuild that fails is run again on the next pass.
    /// The block follows the pass: a retryable fault with a committed entry still unapplied, or one while the outcomes are rebuilt (a
    /// leader start would fail on it too), marks the apply blocked; a pass that ends with nothing left to apply clears the mark, also
    /// after a retryable fault with nothing pending. A log that is not ready or a copy that is not complete leaves the mark, and the
    /// assessment, as they are: the election gate denies such a node anyway, and the pass after the copy becomes complete decides before
    /// the node can campaign. A failure to read the log status leaves them as they are too. Every pass that gets past the status, and a
    /// pass the loop skips, marks the group assessed, which the election driver waits for before it campaigns.
    /// </remarks>
    private async Task CatchUpAsync(string groupId, (ReplicaGroupApplier Applier, IFollowerLog Log, ReplicaApplyBlock Block) group, CancellationToken stoppingToken)
    {
        var (applier, log, block) = group;
        var status = await log.GetStatusAsync(stoppingToken).ConfigureAwait(false);
        if (status.Readiness != FollowerLogReadiness.Ready || status.MemberState != GroupMemberState.Complete)
            return;

        var pending = status.EffectiveCommitIndex > Math.Max(status.LastAppliedIndex, applier.AppliedIndex);
        try
        {
            if (!log.Idempotency.OutcomesRebuilt)
            {
                var restored = await ReplicaOutcomeRecovery.RestoreAsync(log, _timeProvider, stoppingToken).ConfigureAwait(false);
                ServerLog.ReplicaOutcomesRestored(_log, groupId, restored);
            }
        }
        catch (Exception exception) when (ReplicaRetryableFault.Is(exception) && !stoppingToken.IsCancellationRequested)
        {
            block.Block();
            throw;
        }

        try
        {
            status = await MakeCommitDurableAsync(log, status, stoppingToken).ConfigureAwait(false);
            await applier.CatchUpAsync(log, status.LastAppliedIndex, status.CommitIndex, stoppingToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (ReplicaRetryableFault.Is(exception) && !stoppingToken.IsCancellationRequested)
        {
            if (pending)
                block.Block();
            else
                block.Clear();

            throw;
        }

        block.Clear();
    }
}
