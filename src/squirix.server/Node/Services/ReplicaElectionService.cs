using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Runs the election driver of every replica group this node serves, so a group whose leader goes silent elects another.</summary>
/// <remarks>
/// Registered only with automatic failover on a host whose network replication is activated. Each group of three or more replicas gets
/// one loop: it steps its driver, reports what the step did when it changed, and waits for the next step or for a higher term that wakes
/// the driver. Groups of fewer replicas get no loop: their owner leads them statically. A loop that faults stops the others, and the
/// service ends with its fault once they have ended; host shutdown ends every loop normally and stops every driver, which leaves its
/// group a follower without authority. The loops end at the start of a graceful stop (<see cref="HostStopping" />), before the web server
/// drains the requests in flight: a led group stops heartbeating and loses its authority at once, so its followers elect a new leader
/// while this node finishes those requests (#1092).
/// </remarks>
internal sealed class ReplicaElectionService : BackgroundService
{
    /// <summary>The log of each election event that names only its group and term.</summary>
    private static readonly FrozenDictionary<ElectionEvent, Action<ILogger, string, ulong>> TermEventLogs = new Dictionary<ElectionEvent, Action<ILogger, string, ulong>>
    {
        [ElectionEvent.PreVoteLost] = ServerLog.ReplicaElectionPreVoteLost,
        [ElectionEvent.VoteLost] = ServerLog.ReplicaElectionVoteLost,
        [ElectionEvent.Elected] = ServerLog.ReplicaElectionElected,
        [ElectionEvent.Authorized] = ServerLog.ReplicaElectionAuthorized,
        [ElectionEvent.PromotionPending] = ServerLog.ReplicaElectionPromotionPending,
        [ElectionEvent.RetirePending] = ServerLog.ReplicaElectionRetirePending,
        [ElectionEvent.TermObserved] = ServerLog.ReplicaElectionTermObserved,
        [ElectionEvent.TermNotDurable] = ServerLog.ReplicaElectionTermNotDurable,
    }.ToFrozenDictionary();

    private readonly ReplicaRpcHeader _header;
    private readonly IReplicaLeadership _leadership;
    private readonly IReplicaGroupLocator _locator;
    private readonly ILogger<ReplicaElectionService> _log;
    private readonly ReplicaGroupRegistry _registry;
    private readonly IReplicaVoteGateway _votes;

    /// <summary>Initializes a new instance of the <see cref="ReplicaElectionService" /> class.</summary>
    /// <param name="registry">The registry of the served groups, their logs, and their election states.</param>
    /// <param name="locator">The replica group locator naming the members of each group.</param>
    /// <param name="votes">The vote RPCs to the other members.</param>
    /// <param name="leadership">The leader side the drivers hand won terms to.</param>
    /// <param name="identity">The topology fingerprint and generation of the vote RPCs, and this node as their sender.</param>
    /// <param name="log">Logger of the election events.</param>
    internal ReplicaElectionService(
        ReplicaGroupRegistry registry,
        IReplicaGroupLocator locator,
        IReplicaVoteGateway votes,
        IReplicaLeadership leadership,
        (ReadOnlyMemory<byte> Fingerprint, ulong Generation, string SelfId) identity,
        ILogger<ReplicaElectionService> log)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(locator);
        ArgumentNullException.ThrowIfNull(votes);
        ArgumentNullException.ThrowIfNull(leadership);
        ArgumentException.ThrowIfNullOrWhiteSpace(identity.SelfId);
        ArgumentNullException.ThrowIfNull(log);
        _registry = registry;
        _locator = locator;
        _votes = votes;
        _leadership = leadership;
        _header = new ReplicaRpcHeader(string.Empty, identity.Fingerprint, identity.Generation, 0, string.Empty, identity.SelfId);
        _log = log;
    }

    /// <summary>Gets the token signaled at the start of a graceful host stop, before the hosted services stop; none unless set.</summary>
    internal CancellationToken HostStopping { get; init; }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_locator.ReplicaCount < 3)
            return;

        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, HostStopping);
        var groupIds = _registry.GroupIds;
        var loops = new List<Task>(groupIds.Count);
        for (var i = 0; i < groupIds.Count; i++)
        {
            if (_registry.TryGetLog(groupIds[i], out var log))
                loops.Add(DriveAsync(CreateElection(groupIds[i], log), stopping.Token));
            else
                ServerLog.ReplicaElectionNoLog(_log, groupIds[i]);
        }

        await ReplicaGroupLoops.AwaitAllAsync(loops, stopping).ConfigureAwait(false);
        if (stoppingToken.IsCancellationRequested || HostStopping.IsCancellationRequested)
            ServerLog.ReplicaElectionStopped(_log);
    }

    /// <summary>Returns the stable name of a denial for the election log.</summary>
    /// <param name="denial">The denial.</param>
    /// <returns>The snake-case denial name.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The denial is not a named value.</exception>
    private static string DenialName(FailoverDenial denial) => denial switch
    {
        FailoverDenial.None => "none",
        FailoverDenial.Disabled => "disabled",
        FailoverDenial.SingleNode => "single_node",
        FailoverDenial.ReplicaFactorTooLow => "replica_factor_too_low",
        FailoverDenial.NoMajority => "no_majority",
        FailoverDenial.LogNotCaughtUp => "log_not_caught_up",
        FailoverDenial.StaleTerm => "stale_term",
        FailoverDenial.Unseeded => "unseeded",
        FailoverDenial.Incomplete => "incomplete",
        FailoverDenial.ApplyBlocked => "apply_blocked",
        FailoverDenial.ApplyUnassessed => "apply_unassessed",
        _ => throw new ArgumentOutOfRangeException(nameof(denial), denial, "Unsupported failover denial."),
    };

    private ReplicaGroupElection CreateElection(string groupId, IFollowerLog log)
    {
        var members = new string[_locator.ReplicaCount];
        _locator.GetReplicaGroup(groupId, members);
        return new ReplicaGroupElection(_registry.StateFor(groupId), log, _votes, _leadership, members, _header with { GroupId = groupId });
    }

    /// <summary>Steps the driver of one group until the host stops.</summary>
    /// <param name="election">The driver of the group.</param>
    /// <param name="stoppingToken">The host stopping token, also canceled when the loop of another group failed.</param>
    /// <returns>A task that completes when the host stopped.</returns>
    private async Task DriveAsync(ReplicaGroupElection election, CancellationToken stoppingToken)
    {
        var state = _registry.StateFor(election.GroupId);
        ElectionOutcome? reported = null;
        var blockLogged = 0L;
        try
        {
            var running = true;
            while (running)
            {
                var outcome = await election.StepAsync(stoppingToken).ConfigureAwait(false);

                // A block is reported once per episode of the flag, however many rounds the node then sits out.
                var blocked = outcome is { Event: ElectionEvent.Denied, Denial: FailoverDenial.ApplyBlocked };
                var episodes = state.ApplyBlock.Episodes;
                if (blocked ? episodes != blockLogged : outcome != reported && outcome.Event != ElectionEvent.None)
                {
                    Report(election.GroupId, in outcome);
                    reported = outcome;
                    blockLogged = blocked ? episodes : blockLogged;
                }

                running = await state.WaitAsync(election.NextDelay(), stoppingToken).ConfigureAwait(false) != TimedWaitResult.Canceled;
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Host shutdown, or the loop of another group failed: the service reports the stop once.
        }
        finally
        {
            election.Stop();
        }
    }

    /// <summary>Logs what a step of the election driver did.</summary>
    /// <param name="groupId">The replica group.</param>
    /// <param name="outcome">The outcome of the step.</param>
    /// <exception cref="ArgumentOutOfRangeException">The event is not one of the election events.</exception>
    private void Report(string groupId, in ElectionOutcome outcome)
    {
        if (TermEventLogs.TryGetValue(outcome.Event, out var log))
        {
            log(_log, groupId, outcome.Term);
            return;
        }

        if (outcome is { Event: ElectionEvent.Denied, Denial: FailoverDenial.ApplyBlocked })
        {
            ServerLog.ReplicaElectionApplyBlocked(_log, groupId, outcome.Term);
        }
        else if (outcome.Event == ElectionEvent.Denied)
        {
            var denial = DenialName(outcome.Denial);
            ServerLog.ReplicaElectionDenied(_log, groupId, outcome.Term, denial);
        }
        else if (outcome.Event == ElectionEvent.SteppedDown)
        {
            ReportSteppedDown(groupId, in outcome);
        }
        else if (outcome.Event == ElectionEvent.ApplyTimedOut)
        {
            ServerLog.ReplicaElectionApplyTimedOut(_log, groupId, outcome.Term, outcome.Waited);
        }
        else if (outcome.Event == ElectionEvent.AppendTimedOut)
        {
            ServerLog.ReplicaElectionAppendTimedOut(_log, groupId, outcome.Term, outcome.Waited);
        }
        else if (outcome.Event != ElectionEvent.None)
        {
            throw new ArgumentOutOfRangeException(nameof(outcome), outcome.Event, "Unsupported election event.");
        }
    }

    /// <summary>Logs a step-down: a leadership that got no authority within its deadline names how long it waited.</summary>
    /// <param name="groupId">The replica group.</param>
    /// <param name="outcome">The outcome of the step.</param>
    private void ReportSteppedDown(string groupId, in ElectionOutcome outcome)
    {
        if (outcome.Waited > TimeSpan.Zero)
            ServerLog.ReplicaElectionAuthorityTimedOut(_log, groupId, outcome.Term, outcome.Waited);
        else
            ServerLog.ReplicaElectionSteppedDown(_log, groupId, outcome.Term);
    }
}
