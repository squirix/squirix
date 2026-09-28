using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Periodically maintains the replica group log this node owns so its applied entries do not stay retained.</summary>
/// <remarks>
/// Each pass persists the committer's in-memory applied index once the cache journal holds the applied entries durably, which
/// releases their payloads from memory. The pass runs outside the commit gate, so writes never wait on it. A failed pass is logged and
/// retried on the next one; the service runs on the host lifetime and stops with it.
/// </remarks>
internal sealed class ReplicaLogCompactionService : BackgroundService
{
    private readonly ReplicaGroupCommitter _committer;
    private readonly IJournalDurabilityCoordinator _durability;
    private readonly TimeSpan _interval;
    private readonly ILogger<ReplicaLogCompactionService> _log;
    private readonly TimeProvider _timeProvider;

    /// <summary>Initializes a new instance of the <see cref="ReplicaLogCompactionService" /> class.</summary>
    /// <param name="committer">Owner-side committer of the owned group.</param>
    /// <param name="durability">The node cache journal the group applies write to.</param>
    /// <param name="options">The maintenance schedule.</param>
    /// <param name="log">Logger reporting failed passes.</param>
    /// <param name="timeProvider">Time source for the delay between passes.</param>
    internal ReplicaLogCompactionService(
        ReplicaGroupCommitter committer,
        IJournalDurabilityCoordinator durability,
        ReplicaLogCompactionOptions options,
        ILogger<ReplicaLogCompactionService> log,
        TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(committer);
        ArgumentNullException.ThrowIfNull(durability);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(timeProvider);
        _committer = committer;
        _durability = durability;
        _interval = options.Interval;
        _log = log;
        _timeProvider = timeProvider;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            while (true)
            {
                await Task.Delay(_interval, _timeProvider, stoppingToken).ConfigureAwait(false);
                await RunOnceAsync(stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Host shutdown: the in-memory applied index is persisted by the next start's passes; a restart re-applies what it lacks.
        }
    }

    private async Task RunOnceAsync(CancellationToken stoppingToken)
    {
        try
        {
            await _committer.FlushAppliedAsync(_durability, stoppingToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is IOException or InvalidOperationException && !stoppingToken.IsCancellationRequested)
        {
            // Storage and journal faults are retried on the next pass; unexpected exceptions still fault the service so the host fails
            // fast instead of silently retaining every applied entry.
            LogManager.ReplicaLogMaintenanceRetry(_log, exception);
        }
    }
}
