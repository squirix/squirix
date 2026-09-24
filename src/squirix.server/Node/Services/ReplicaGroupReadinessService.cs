using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Verifies the owned replica group slots after a restart so the group regains its write quorum.</summary>
/// <remarks>
/// A restarted node with durable RF&gt;1 data starts with every slot recovering. This service retries the
/// committer's Log Matching verification with a bounded backoff until every slot is ready, which also lets
/// followers that were not yet up at node start join later. It keeps polling at the maximum delay once everything
/// is ready, so a slot demoted later is verified again. It runs on the host lifetime and stops with it.
/// </remarks>
internal sealed class ReplicaGroupReadinessService : BackgroundService
{
    private static readonly TimeSpan InitialDelay = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan MaxDelay = TimeSpan.FromSeconds(5);

    private readonly ReplicaGroupCommitter _committer;
    private readonly ILogger<ReplicaGroupReadinessService> _log;
    private readonly TimeProvider _timeProvider;

    /// <summary>Initializes a new instance of the <see cref="ReplicaGroupReadinessService" /> class.</summary>
    /// <param name="committer">Owner-side committer that performs the verification.</param>
    /// <param name="log">Logger reporting verification state changes.</param>
    /// <param name="timeProvider">Time source for the retry delay.</param>
    internal ReplicaGroupReadinessService(ReplicaGroupCommitter committer, ILogger<ReplicaGroupReadinessService> log, TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(committer);
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(timeProvider);
        _committer = committer;
        _log = log;
        _timeProvider = timeProvider;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var backoff = InitialDelay;
        ReplicaVerification? reported = null;
        try
        {
            while (true)
            {
                var outcome = await VerifyOnceAsync(stoppingToken).ConfigureAwait(false);
                if (outcome != reported)
                {
                    Report(outcome);
                    reported = outcome;
                }

                // Pending backs off exponentially toward the cap; a blocked or fully ready group is only re-checked at the cap.
                var delay = outcome == ReplicaVerification.Pending ? backoff : MaxDelay;
                backoff = outcome == ReplicaVerification.Pending ? TimeSpan.FromTicks(Math.Min(MaxDelay.Ticks, backoff.Ticks * 2)) : InitialDelay;
                await Task.Delay(delay, _timeProvider, stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Host shutdown: the verification is retried on the next start.
        }
    }

    private void Report(ReplicaVerification outcome)
    {
        switch (outcome)
        {
            case ReplicaVerification.AllReady:
                LogManager.ReplicaVerificationComplete(_log);
                break;
            case ReplicaVerification.Pending:
                LogManager.ReplicaVerificationPending(_log);
                break;
            case ReplicaVerification.Blocked:
                LogManager.ReplicaVerificationBlocked(_log);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(outcome), outcome, "Unsupported verification state.");
        }
    }

    private async Task<ReplicaVerification> VerifyOnceAsync(CancellationToken stoppingToken)
    {
        try
        {
            return await _committer.VerifyReplicasAsync(stoppingToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is IOException or InvalidOperationException && !stoppingToken.IsCancellationRequested)
        {
            // Storage or commit-gate faults are retried like unreachable peers; unexpected exceptions still fault
            // the service so the host fails fast instead of silently running without a verified quorum.
            LogManager.ReplicaVerificationRetry(_log, exception);
            return ReplicaVerification.Pending;
        }
    }
}
