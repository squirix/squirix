using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;

namespace Squirix.Server.Node.Services;

/// <summary>Verifies the owned replica group slots after a restart so the group regains its write quorum.</summary>
/// <remarks>
/// A restarted node with durable RF&gt;1 data starts with every slot recovering. This service retries the
/// committer's Log Matching verification with a bounded backoff until every slot is ready, which also lets
/// followers that were not yet up at node start join later. It runs on the host lifetime and stops with it.
/// </remarks>
internal sealed class ReplicaGroupReadinessService : BackgroundService
{
    private static readonly TimeSpan InitialDelay = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan MaxDelay = TimeSpan.FromSeconds(5);

    private readonly ReplicaGroupCommitter _committer;

    /// <summary>Initializes a new instance of the <see cref="ReplicaGroupReadinessService" /> class.</summary>
    /// <param name="committer">Owner-side committer that performs the verification.</param>
    internal ReplicaGroupReadinessService(ReplicaGroupCommitter committer)
    {
        ArgumentNullException.ThrowIfNull(committer);
        _committer = committer;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var delay = InitialDelay;
        try
        {
            while (true)
            {
                var outcome = await VerifyOnceAsync(stoppingToken).ConfigureAwait(false);
                if (outcome == ReplicaVerification.AllReady)
                    return;

                delay = outcome == ReplicaVerification.Blocked ? MaxDelay : delay;
                await Task.Delay(delay, TimeProvider.System, stoppingToken).ConfigureAwait(false);
                delay = TimeSpan.FromTicks(Math.Min(MaxDelay.Ticks, delay.Ticks * 2));
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Host shutdown: the verification is retried on the next start.
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
            return ReplicaVerification.Pending;
        }
    }
}
