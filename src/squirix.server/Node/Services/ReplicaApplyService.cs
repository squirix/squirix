using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Errors;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Applies the committed entries of every served group this node does not lead to local memory, densely in log order.</summary>
/// <remarks>
/// Each follower group runs its own loop, the single caller of the group's applier: it catches memory up through the durable commit
/// index, then waits until the follower path signals new entries, or until a fallback interval elapses. Nothing is applied before local
/// recovery has replayed the cache journal into memory. A committed record that cannot be applied stops only its group: it is logged as
/// an error and every later entry of that group stays unapplied, while the other groups go on. Storage and journal faults, a full
/// journal, and memory that refuses the write are logged when they change and retried on the next pass; anything else faults the service. The service runs on the host lifetime and stops
/// with it; a restart applies again what memory lacks.
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
    /// committed record stopped ends normally and leaves the others running.
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
            // A led group's committer is the one driver of its applier; the led set is fixed for the node lifetime.
            if (!_committers.Leads(groupIds[i]))
                loops.Add(ApplyLoopAsync(groupIds[i], stopping.Token));
        }

        // Without a failed loop, every loop ended with the host: memory keeps what was applied, and a restart applies the committed entries
        // above the durable applied index again.
        await ReplicaGroupLoops.AwaitAllAsync(loops, stopping).ConfigureAwait(false);
    }

    /// <summary>Tells whether a fault of a pass leaves the group pending for the next pass instead of faulting the service.</summary>
    /// <param name="exception">The fault of the pass.</param>
    /// <returns><see langword="true" /> for storage and journal faults, and for a full journal or memory that refused the write.</returns>
    private static bool IsRetryable(Exception exception) => exception is IOException or InvalidOperationException or JournalCapacityExceededException or ResourceExhaustedException;

    /// <summary>Applies the committed entries of one follower group until the host stops or a committed record stops the group.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="stoppingToken">The host stopping token.</param>
    /// <returns>A task that completes when a committed record stopped the group.</returns>
    private async Task ApplyLoopAsync(string groupId, CancellationToken stoppingToken)
    {
        if (!_registry.TryGetLog(groupId, out var log))
        {
            ServerLog.ReplicaFollowerApplyNoLog(_log, groupId);
            return;
        }

        var applier = _appliers.For(groupId);
        var signal = _registry.ApplySignalFor(groupId);
        (Type Type, ulong AppliedIndex)? reported = null;
        while (true)
        {
            // A committer leading the group by election holds the lease and drives the applier itself: the pass is skipped until it hands
            // the group back.
            if (applier.DriverLease.TryLock(out var lease))
            {
                try
                {
                    await CatchUpAsync(groupId, applier, log, stoppingToken).ConfigureAwait(false);
                    reported = null;
                }
                catch (InvalidDataException exception)
                {
                    // The record never reached memory, and every later entry depends on it: the group stops here instead of skipping it.
                    ServerLog.ReplicaFollowerApplyStopped(_log, groupId, exception);
                    return;
                }
                catch (Exception exception) when (IsRetryable(exception) && !stoppingToken.IsCancellationRequested)
                {
                    // The same fault repeats on every pass until it clears, so it is logged when its kind or the entry it stops at changes,
                    // not on every retry; its message may name the commit index, which moves on while the fault stays.
                    var fault = (exception.GetType(), applier.AppliedIndex);
                    if (reported != fault)
                    {
                        reported = fault;
                        ServerLog.ReplicaFollowerApplyRetry(_log, groupId, exception);
                    }
                }
                finally
                {
                    lease.Dispose();
                }
            }

            _ = await signal.WaitAsync(FallbackInterval, _timeProvider, stoppingToken).ConfigureAwait(false);
        }
    }

    /// <summary>Applies the committed entries of a ready group log that memory lacks.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="applier">The group applier.</param>
    /// <param name="log">The group log.</param>
    /// <param name="stoppingToken">The host stopping token.</param>
    /// <returns>A task that completes when memory holds every entry through the commit index read at the start.</returns>
    /// <remarks>
    /// The first pass over a ready log rebuilds the outcomes of its committed entries first, as the leader committer does for its own
    /// group: the applier records the outcome of each entry it applies, and a retry of an operation committed before the restart then
    /// replays its outcome on this node too. A rebuild that fails is run again on the next pass.
    /// </remarks>
    private async Task CatchUpAsync(string groupId, ReplicaGroupApplier applier, IFollowerLog log, CancellationToken stoppingToken)
    {
        var status = await log.GetStatusAsync(stoppingToken).ConfigureAwait(false);
        if (status.Readiness != FollowerLogReadiness.Ready)
            return;

        if (!log.Idempotency.OutcomesRebuilt)
        {
            var restored = await ReplicaOutcomeRecovery.RestoreAsync(log, _timeProvider, stoppingToken).ConfigureAwait(false);
            ServerLog.ReplicaOutcomesRestored(_log, groupId, restored);
        }

        await applier.CatchUpAsync(log, status.LastAppliedIndex, status.CommitIndex, stoppingToken).ConfigureAwait(false);
    }
}
