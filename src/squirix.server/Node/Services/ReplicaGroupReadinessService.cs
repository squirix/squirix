using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Squirix.Server.Node.Observability;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Verifies the slots of every replica group this node leads after a restart so each group regains its write quorum.</summary>
/// <remarks>
/// A restarted node with durable RF&gt;1 data starts with every slot recovering. This service runs one loop per led group, which retries
/// the committer's Log Matching verification with a bounded backoff until every slot is ready, which also lets followers that were not
/// yet up at node start join later. A follower that answered but lacks entries is caught up from the leader log, one follower at a time,
/// and verified again at once when it was admitted. A loop keeps polling at the maximum delay once everything is ready; a follower the
/// commit path demotes is queued in the group's <see cref="ReplicaRepairQueue" />, which wakes that loop to verify and catch it up at
/// once. A verification pass ends once a majority of followers answered, so a dead follower does not delay a newly elected leader; a pass
/// that admitted nobody and left the group pending is followed by a pass that awaits every follower. The groups do not wait for each
/// other. A group the election hands this node gets its loop when its leadership starts, and the loop ends with the leadership. A loop
/// that faults stops the others, and the service ends with its fault once they have ended. It runs on the host lifetime and stops with it.
/// </remarks>
internal sealed class ReplicaGroupReadinessService : BackgroundService
{
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

    /// <summary>Gets or initializes the retry schedule of the verification; the default schedule unless set.</summary>
    internal ReplicaReadinessOptions Options { get; init; } = new();

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var led = _committers.Led;
        var loops = new List<Task>(led.Count);
        for (var i = 0; i < led.Count; i++)
            loops.Add(VerifyLoopAsync(led[i], stopping.Token));

        if (_committers.Promotions is { } promotions)
            loops.Add(WatchPromotionsAsync(promotions, stopping.Token));

        // Without a failed loop, every loop ended with the host: the verification is retried on the next start.
        await ReplicaGroupLoops.AwaitAllAsync(loops, stopping).ConfigureAwait(false);
    }

    /// <summary>Waits for the next leadership the election starts.</summary>
    /// <param name="promotions">The leaderships in the order they started.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The leadership.</returns>
    private static Task<ReplicaPromotion> NextAsync(ChannelReader<ReplicaPromotion> promotions, CancellationToken cancellationToken) =>
        promotions.ReadAsync(cancellationToken).AsTask();

    /// <summary>Verifies and repairs the slots of one led group until the host stops.</summary>
    /// <param name="committer">The committer of the group.</param>
    /// <param name="stoppingToken">The host stopping token, also canceled when the loop of another group failed.</param>
    /// <returns>A task that completes when the host stopped.</returns>
    private async Task VerifyLoopAsync(ReplicaGroupCommitter committer, CancellationToken stoppingToken)
    {
        var catchUp = new ReplicaCatchUpReporter(committer.GroupId, _log, _catchUpMetrics);
        var repairs = committer.Probe.Repairs;
        var backoff = Options.InitialDelay;
        var quick = true;
        ReplicaVerification? reported = null;
        try
        {
            while (true)
            {
                var outcome = await VerifyOnceAsync(committer, quick, stoppingToken).ConfigureAwait(false);
                if (outcome != reported)
                {
                    Report(committer.GroupId, outcome);
                    reported = outcome;
                }

                // A follower admitted by its catch-up is verified again at once, so the group reports ready without a backoff.
                var admitted = outcome == ReplicaVerification.Pending && await CatchUpOnceAsync(committer, catchUp, stoppingToken).ConfigureAwait(false);

                // A quick pass that admitted nobody and left the group pending is followed by a full one, so a fast follower that cannot be
                // caught up never starves a slower one that can.
                quick = outcome != ReplicaVerification.Pending || admitted;
                if (admitted)
                    continue;

                // Pending backs off exponentially toward the cap; a blocked or fully ready group is only re-checked at the cap.
                var delay = outcome == ReplicaVerification.Pending ? backoff : Options.MaxDelay;
                backoff = outcome == ReplicaVerification.Pending ? TimeSpan.FromTicks(Math.Min(Options.MaxDelay.Ticks, backoff.Ticks * 2)) : Options.InitialDelay;

                // A follower the commit path demoted cuts the wait short: it is verified and caught up at once, with a fresh backoff.
                if (await repairs.WaitAsync(delay, _timeProvider, stoppingToken).ConfigureAwait(false))
                    backoff = Options.InitialDelay;
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Host shutdown, or the loop of another group failed: the verification is retried on the next start.
        }
    }

    /// <summary>Runs one verification loop per leadership the election starts, each until its leadership or the host ends.</summary>
    /// <param name="promotions">The leaderships in the order they started.</param>
    /// <param name="stoppingToken">The host stopping token, also canceled when another loop failed.</param>
    /// <returns>A task that completes once the host stopped and every leadership loop ended; it faults with the first loop that faulted.</returns>
    private async Task WatchPromotionsAsync(ChannelReader<ReplicaPromotion> promotions, CancellationToken stoppingToken)
    {
        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var loops = new List<Task>();
        try
        {
            var arrival = NextAsync(promotions, stopping.Token);
            while (true)
            {
                loops.Add(arrival);
                var ended = await Task.WhenAny(loops).ConfigureAwait(false);
                _ = loops.Remove(arrival);
                if (ended != arrival)
                {
                    // A loop that ended with its leadership completes normally; one that faulted ends the watch with its fault.
                    _ = loops.Remove(ended);
                    await ended.ConfigureAwait(false);
                    continue;
                }

                loops.Add(VerifyTenureAsync(await arrival.ConfigureAwait(false), stopping.Token));
                arrival = NextAsync(promotions, stopping.Token);
            }
        }
        catch (Exception exception) when (exception is OperationCanceledException or ChannelClosedException && stoppingToken.IsCancellationRequested)
        {
            // Host shutdown: every leadership loop ends below.
        }
        finally
        {
            await stopping.CancelAsync().ConfigureAwait(false);
            _ = await Task.WhenAll(loops).CaptureFailureAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Verifies and repairs the slots of a group led by election until its leadership or the host ends.</summary>
    /// <param name="promotion">The leadership.</param>
    /// <param name="stoppingToken">The host stopping token, also canceled when another loop failed.</param>
    /// <returns>A task that completes when the leadership or the host ended.</returns>
    private async Task VerifyTenureAsync(ReplicaPromotion promotion, CancellationToken stoppingToken)
    {
        using var tenure = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, promotion.Tenure);
        await VerifyLoopAsync(promotion.Committer, tenure.Token).ConfigureAwait(false);
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
            ServerLog.ReplicaVerificationRetry(_log, committer.GroupId, exception);
            return false;
        }
    }

    private void Report(string groupId, ReplicaVerification outcome)
    {
        switch (outcome)
        {
            case ReplicaVerification.AllReady:
                ServerLog.ReplicaVerificationComplete(_log, groupId);
                break;
            case ReplicaVerification.Pending:
                ServerLog.ReplicaVerificationPending(_log, groupId);
                break;
            case ReplicaVerification.Blocked:
                ServerLog.ReplicaVerificationBlocked(_log, groupId);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(outcome), outcome, "Unsupported verification state.");
        }
    }

    private async Task<ReplicaVerification> VerifyOnceAsync(ReplicaGroupCommitter committer, bool quick, CancellationToken stoppingToken)
    {
        try
        {
            return await committer.VerifyReplicasAsync(quick, stoppingToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is IOException or InvalidOperationException && !stoppingToken.IsCancellationRequested)
        {
            // Storage or commit-gate faults are retried like unreachable peers; unexpected exceptions still fault
            // the service so the host fails fast instead of silently running without a verified quorum.
            ServerLog.ReplicaVerificationRetry(_log, committer.GroupId, exception);
            return ReplicaVerification.Pending;
        }
    }
}
