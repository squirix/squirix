using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Composition;
using Squirix.Server.Logging;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Runs the first start of the replica group this node owns until its copy is complete or incomplete.</summary>
/// <remarks>
/// <para>
/// Registered on every node of a replicated host, with or without automatic failover. A copy of a group on a fresh disk is unseeded and
/// takes no part in the group; this service seeds the group the node owns, once every other member answered as an unseeded copy of the same
/// topology and accepted the offer of the owner. The leadership the owner then starts completes the offered members. A group the node does
/// not own is never started here: its copy completes only on a call of a complete member.
/// </para>
/// <para>
/// Each iteration is one <see cref="ReplicaGroupBootstrap.RunOnceAsync" />; the service reports a changed outcome once. The first iteration
/// waits for the application to have started, so the peers it wakes can call this node back. Between iterations the
/// <see cref="ReplicaBootstrapPacer" /> waits for the delay of a fixed backoff, from <see cref="InitialDelay" /> doubling up to
/// <see cref="MaxDelay" />, or until the member the last iteration waited for calls this node; a wake is only a shortcut, so an extra or a
/// lost one is harmless, and owners that wait for each other are paced at no more than one iteration per <see cref="InitialDelay" />. It ends
/// once the copy is complete or incomplete, and with the host.
/// </para>
/// </remarks>
internal sealed class ReplicaGroupBootstrapService : NodeWorkerBase
{
    /// <summary>The first delay between two iterations.</summary>
    internal static readonly TimeSpan InitialDelay = TimeSpan.FromMilliseconds(250);

    /// <summary>The longest delay between two iterations.</summary>
    internal static readonly TimeSpan MaxDelay = TimeSpan.FromSeconds(5);

    private readonly ReplicaApplySignal _applySignal;
    private readonly ReplicaGroupBootstrap _bootstrap;
    private readonly ReplicaGroupCommitters _committers;
    private readonly INodeLifetime _lifetime;
    private readonly IServerLogger<ReplicaGroupBootstrapService> _log;
    private readonly ReplicaBootstrapPacer _pacer;

    /// <summary>Initializes a new instance of the <see cref="ReplicaGroupBootstrapService" /> class.</summary>
    /// <param name="bootstrap">The first start of the group this node owns.</param>
    /// <param name="schedule">
    /// The record of the peers that called this node, which cuts the wait between two iterations short, the application lifetime, whose
    /// started signal the first iteration waits for, the committers whose readiness loop is woken once the own copy is complete, and the apply
    /// signal of the own group, notified at the same moment so its apply loop assesses the seeded copy without waiting out its fallback interval.
    /// </param>
    /// <param name="log">Logger of the first start.</param>
    /// <param name="timeProvider">Time source for the delay between iterations.</param>
    internal ReplicaGroupBootstrapService(
        ReplicaGroupBootstrap bootstrap,
        (ReplicaPeerContact Contact, INodeLifetime Lifetime, ReplicaGroupCommitters Committers, ReplicaApplySignal ApplySignal) schedule,
        IServerLogger<ReplicaGroupBootstrapService> log,
        TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(bootstrap);
        ArgumentNullException.ThrowIfNull(schedule.Contact);
        ArgumentNullException.ThrowIfNull(schedule.Lifetime);
        ArgumentNullException.ThrowIfNull(schedule.Committers);
        ArgumentNullException.ThrowIfNull(schedule.ApplySignal);
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(timeProvider);
        _applySignal = schedule.ApplySignal;
        _bootstrap = bootstrap;
        _committers = schedule.Committers;
        _lifetime = schedule.Lifetime;
        _log = log;
        _pacer = new ReplicaBootstrapPacer(schedule.Contact, timeProvider);
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        ReplicaBootstrapOutcome? reported = null;
        try
        {
            var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await using (_lifetime.ApplicationStarted.Register(static state => Release(state), started).ConfigureAwait(false))
                await started.Task.WaitAsync(stoppingToken).ConfigureAwait(false);

            while (true)
            {
                _pacer.Begin();
                var outcome = await RunOnceAsync(stoppingToken).ConfigureAwait(false);
                if (outcome is { } current)
                {
                    if (current != reported)
                    {
                        Report(in current);
                        reported = current;
                    }

                    if (current.Result == ReplicaBootstrapResult.Completed)
                    {
                        _applySignal.Notify();
                        WakeReadiness();
                    }

                    if (current.IsFinal)
                        return;
                }

                if (await _pacer.WaitAsync(outcome, stoppingToken).ConfigureAwait(false) != TimedWaitResult.Canceled)
                    continue;
                ReplicaBootstrapLog.ReplicaBootstrapStopped(_log, _bootstrap.GroupId);
                return;
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            ReplicaBootstrapLog.ReplicaBootstrapStopped(_log, _bootstrap.GroupId);
        }
    }

    private static void Release(object? state)
    {
        if (state is TaskCompletionSource started)
            _ = started.TrySetResult();
    }

    private void Report(in ReplicaBootstrapOutcome outcome)
    {
        var groupId = _bootstrap.GroupId;
        switch (outcome.Result)
        {
            case ReplicaBootstrapResult.Completed:
                ReplicaBootstrapLog.ReplicaBootstrapCompleted(_log, groupId);
                break;
            case ReplicaBootstrapResult.AlreadyComplete:
                ReplicaBootstrapLog.ReplicaBootstrapAlreadyComplete(_log, groupId);
                break;
            case ReplicaBootstrapResult.Incomplete:
                ReplicaBootstrapLog.ReplicaBootstrapIncomplete(_log, groupId);
                break;
            case ReplicaBootstrapResult.LogNotReady:
                ReplicaBootstrapLog.ReplicaBootstrapLogNotReady(_log, groupId);
                break;
            case ReplicaBootstrapResult.WaitingForMember:
                ReplicaBootstrapLog.ReplicaBootstrapWaiting(_log, groupId, outcome.NodeId);
                break;
            case ReplicaBootstrapResult.MemberHoldsCompleteCopy:
                ReplicaBootstrapLog.ReplicaBootstrapMemberComplete(_log, groupId, outcome.NodeId);
                break;
            case ReplicaBootstrapResult.OfferRefused:
                ReplicaBootstrapLog.ReplicaBootstrapOfferRefused(_log, groupId, outcome.NodeId, outcome.MemberStateName, outcome.RefusalCode);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(outcome), outcome.Result, "Unsupported bootstrap result.");
        }
    }

    private async Task<ReplicaBootstrapOutcome?> RunOnceAsync(CancellationToken stoppingToken)
    {
        try
        {
            return await _bootstrap.RunOnceAsync(stoppingToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (ReplicaRetryableFault.Is(exception) && !stoppingToken.IsCancellationRequested)
        {
            // A storage fault of the own log is retried like an unreachable member; anything unexpected still faults the service.
            ReplicaBootstrapLog.ReplicaBootstrapRetry(_log, _bootstrap.GroupId, exception);
            return null;
        }
    }

    /// <summary>Wakes the readiness loop of the group this node leads statically, so it verifies the slots now that the own copy is complete.</summary>
    /// <remarks>A group led by election gets its loop when its leadership starts, which needs a complete copy.</remarks>
    private void WakeReadiness()
    {
        var led = _committers.LedCommitters;
        for (var i = 0; i < led.Count; i++)
        {
            if (string.Equals(led[i].GroupId, _bootstrap.GroupId, StringComparison.Ordinal))
                led[i].Probe.Repairs.Wake();
        }
    }
}
