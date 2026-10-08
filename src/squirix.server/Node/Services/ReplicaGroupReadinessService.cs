using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Squirix.Server.Node.Observability;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Verifies the slots of every replica group this node leads after a restart so each group regains its write quorum.</summary>
/// <remarks>
/// A restarted node with durable RF&gt;1 data starts with every slot recovering. This service runs one loop per led group, which retries
/// the committer's Log Matching verification with a bounded backoff until every slot is ready, which also lets followers that were not
/// yet up at node start join later. A follower that answered but lacks entries is caught up from the leader log, one follower at a time,
/// and verified again at once when it was admitted. A loop keeps polling at the maximum delay once everything is ready; a follower the
/// commit path demotes is queued in the group's <see cref="ReplicaRepairQueue" />, which wakes that loop to verify and catch it up at
/// once. The groups do not wait for each other. A loop that faults stops the others, and the service ends with its fault once they have
/// ended. It runs on the host lifetime and stops with it.
/// </remarks>
internal sealed class ReplicaGroupReadinessService : BackgroundService
{
    private static readonly TimeSpan InitialDelay = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan MaxDelay = TimeSpan.FromSeconds(5);

    private readonly ReplicaCatchUpMetrics? _catchUpMetrics;
    private readonly ReplicaGroupCommitters _committers;
    private readonly ILogger<ReplicaGroupReadinessService> _log;
    private readonly TimeProvider _timeProvider;

    /// <summary>Initializes a new instance of the <see cref="ReplicaGroupReadinessService" /> class.</summary>
    /// <param name="committers">The committers of the led groups, which perform the verification.</param>
    /// <param name="log">Logger reporting verification state changes.</param>
    /// <param name="timeProvider">Time source for the retry delay.</param>
    /// <param name="catchUpMetrics">Counts the follower catch-up sessions; none are counted when <see langword="null" />.</param>
    internal ReplicaGroupReadinessService(
        ReplicaGroupCommitters committers,
        ILogger<ReplicaGroupReadinessService> log,
        TimeProvider timeProvider,
        ReplicaCatchUpMetrics? catchUpMetrics = null)
    {
        ArgumentNullException.ThrowIfNull(committers);
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(timeProvider);
        _committers = committers;
        _log = log;
        _timeProvider = timeProvider;
        _catchUpMetrics = catchUpMetrics;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var led = _committers.Led;
        var loops = new List<Task>(led.Count);
        for (var i = 0; i < led.Count; i++)
            loops.Add(VerifyLoopAsync(led[i], stopping.Token));

        // Without a failed loop, every loop ended with the host: the verification is retried on the next start.
        await ReplicaGroupLoops.AwaitAllAsync(loops, stopping).ConfigureAwait(false);
    }

    /// <summary>Verifies and repairs the slots of one led group until the host stops.</summary>
    /// <param name="committer">The committer of the group.</param>
    /// <param name="stoppingToken">The host stopping token, also canceled when the loop of another group failed.</param>
    /// <returns>A task that completes when the host stopped.</returns>
    private async Task VerifyLoopAsync(ReplicaGroupCommitter committer, CancellationToken stoppingToken)
    {
        var catchUp = new ReplicaCatchUpReporter(committer.GroupId, _log, _catchUpMetrics);
        var repairs = committer.Probe.Repairs;
        var backoff = InitialDelay;
        ReplicaVerification? reported = null;
        try
        {
            while (true)
            {
                var outcome = await VerifyOnceAsync(committer, stoppingToken).ConfigureAwait(false);
                if (outcome != reported)
                {
                    Report(outcome);
                    reported = outcome;
                }

                // A follower admitted by its catch-up is verified again at once, so the group reports ready without a backoff.
                if (outcome == ReplicaVerification.Pending && await CatchUpOnceAsync(committer, catchUp, stoppingToken).ConfigureAwait(false))
                    continue;

                // Pending backs off exponentially toward the cap; a blocked or fully ready group is only re-checked at the cap.
                var delay = outcome == ReplicaVerification.Pending ? backoff : MaxDelay;
                backoff = outcome == ReplicaVerification.Pending ? TimeSpan.FromTicks(Math.Min(MaxDelay.Ticks, backoff.Ticks * 2)) : InitialDelay;

                // A follower the commit path demoted cuts the wait short: it is verified and caught up at once, with a fresh backoff.
                if (await repairs.WaitAsync(delay, _timeProvider, stoppingToken).ConfigureAwait(false))
                    backoff = InitialDelay;
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Host shutdown, or the loop of another group failed: the verification is retried on the next start.
        }
    }

    private async Task<bool> CatchUpOnceAsync(ReplicaGroupCommitter committer, ReplicaCatchUpReporter catchUp, CancellationToken stoppingToken)
    {
        try
        {
            return await committer.CatchUpFollowersAsync(catchUp, stoppingToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is IOException or InvalidOperationException && !stoppingToken.IsCancellationRequested)
        {
            // The same policy as the verification: storage or gate faults are retried on the next pass, anything else faults the service.
            ServerLog.ReplicaVerificationRetry(_log, exception);
            return false;
        }
    }

    private void Report(ReplicaVerification outcome)
    {
        switch (outcome)
        {
            case ReplicaVerification.AllReady:
                ServerLog.ReplicaVerificationComplete(_log);
                break;
            case ReplicaVerification.Pending:
                ServerLog.ReplicaVerificationPending(_log);
                break;
            case ReplicaVerification.Blocked:
                ServerLog.ReplicaVerificationBlocked(_log);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(outcome), outcome, "Unsupported verification state.");
        }
    }

    private async Task<ReplicaVerification> VerifyOnceAsync(ReplicaGroupCommitter committer, CancellationToken stoppingToken)
    {
        try
        {
            return await committer.VerifyReplicasAsync(stoppingToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is IOException or InvalidOperationException && !stoppingToken.IsCancellationRequested)
        {
            // Storage or commit-gate faults are retried like unreachable peers; unexpected exceptions still fault
            // the service so the host fails fast instead of silently running without a verified quorum.
            ServerLog.ReplicaVerificationRetry(_log, exception);
            return ReplicaVerification.Pending;
        }
    }
}
