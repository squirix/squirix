using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Periodically maintains the replica group logs this node serves so neither their memory nor their files grow without bound.</summary>
/// <remarks>
/// Each pass first persists the committer's in-memory applied index once the cache journal holds the applied entries durably, which
/// releases their payloads from memory; that runs outside the commit gate, so writes never wait on it. It then compacts the owned log
/// once it reaches a threshold, as one step under the commit gate. Afterwards it persists the applied index of every follower group the
/// same way and compacts that group's log through it once the log reaches a threshold. A change of a group's compaction outcome is logged
/// once, not on every pass. A failed step of one group is retried on the next pass without holding back the other groups; a follower
/// group's failure is logged when it starts or changes, not on every pass. The service runs on the host lifetime and stops with it.
/// </remarks>
internal sealed class ReplicaLogCompactionService : BackgroundService
{
    private readonly ReplicaFollowerAppliers _appliers;
    private readonly ReplicaGroupCommitter _committer;
    private readonly IJournalDurabilityCoordinator _durability;

    /// <summary>The type of the maintenance failure last reported per follower group; the passes run one at a time, on the service loop only.</summary>
    private readonly Dictionary<string, Type> _failed = [with(StringComparer.Ordinal)];

    private readonly TimeSpan _interval;
    private readonly ILogger<ReplicaLogCompactionService> _log;
    private readonly ReplicationMetrics _metrics;
    private readonly ReplicaLogCompactionPolicy _policy;
    private readonly ReplicaGroupRegistry _registry;

    /// <summary>The compaction outcome last reported per group; the passes run one at a time, on the service loop only.</summary>
    private readonly Dictionary<string, ReplicaLogCompactionOutcome> _reported = [with(StringComparer.Ordinal)];

    private readonly TimeProvider _timeProvider;

    /// <summary>Initializes a new instance of the <see cref="ReplicaLogCompactionService" /> class.</summary>
    /// <param name="committer">Owner-side committer of the owned group.</param>
    /// <param name="durability">The node cache journal the group applies write to.</param>
    /// <param name="options">The maintenance schedule.</param>
    /// <param name="policy">The compaction thresholds.</param>
    /// <param name="metrics">The replication metrics counting compactions and skipped compactions.</param>
    /// <param name="appliers">The appliers of the follower groups, whose applied indexes are persisted and whose logs are compacted after the owned log.</param>
    /// <param name="registry">Replica group registry holding the follower group logs.</param>
    /// <param name="log">Logger reporting compaction outcome changes and failed passes.</param>
    /// <param name="timeProvider">Time source for the delay between passes.</param>
    internal ReplicaLogCompactionService(
        ReplicaGroupCommitter committer,
        IJournalDurabilityCoordinator durability,
        ReplicaLogCompactionOptions options,
        ReplicaLogCompactionPolicy policy,
        ReplicationMetrics metrics,
        ReplicaFollowerAppliers appliers,
        ReplicaGroupRegistry registry,
        ILogger<ReplicaLogCompactionService> log,
        TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(committer);
        ArgumentNullException.ThrowIfNull(durability);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(metrics);
        ArgumentNullException.ThrowIfNull(appliers);
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(timeProvider);
        _committer = committer;
        _durability = durability;
        _interval = options.Interval;
        _policy = policy;
        _metrics = metrics;
        _appliers = appliers;
        _registry = registry;
        _log = log;
        _timeProvider = timeProvider;
    }

    /// <summary>Runs one maintenance pass: the owned group log first, then the applied index and the log of every follower group.</summary>
    /// <param name="stoppingToken">The host stopping token.</param>
    /// <returns>A task that completes when every group was maintained or its failure was logged.</returns>
    /// <remarks>The service loop is the only production caller; tests run a pass directly instead of waiting for the interval.</remarks>
    internal async Task RunOnceAsync(CancellationToken stoppingToken)
    {
        await MaintainOwnedLogAsync(stoppingToken).ConfigureAwait(false);
        var groupIds = _appliers.GroupIds;
        for (var i = 0; i < groupIds.Count; i++)
            await MaintainFollowerLogAsync(groupIds[i], stoppingToken).ConfigureAwait(false);
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

    private async Task MaintainOwnedLogAsync(CancellationToken stoppingToken)
    {
        try
        {
            await _committer.FlushAppliedAsync(_durability, stoppingToken).ConfigureAwait(false);
            Report(_committer.GroupId, await _committer.CompactOwnedLogAsync(_policy, _durability, stoppingToken).ConfigureAwait(false));
        }
        catch (Exception exception) when (exception is IOException or InvalidOperationException && !stoppingToken.IsCancellationRequested)
        {
            // Storage and journal faults are retried on the next pass; unexpected exceptions still fault the service so the host fails
            // fast instead of silently retaining every applied entry.
            ServerLog.ReplicaLogMaintenanceRetry(_log, _committer.GroupId, exception);
        }
    }

    /// <summary>
    /// Persists the applied index of one follower group once the cache journal holds its applied entries durably, then compacts the
    /// group log through it.
    /// </summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="stoppingToken">The host stopping token.</param>
    /// <returns>A task that completes when the group was maintained or its failure was logged.</returns>
    private async Task MaintainFollowerLogAsync(string groupId, CancellationToken stoppingToken)
    {
        if (!_registry.TryGetLog(groupId, out var log))
            return;

        try
        {
            await _appliers.For(groupId).FlushAsync(log, _durability, stoppingToken).ConfigureAwait(false);
            Report(groupId, await ReplicaLogCompactionStep.RunFollowerAsync(log, _policy, stoppingToken).ConfigureAwait(false));
        }
        catch (Exception exception) when (exception is IOException or InvalidOperationException && !stoppingToken.IsCancellationRequested)
        {
            // The same policy as the owned log: a fault of one group is retried on the next pass and holds back no other group.
            // The same fault can repeat on every pass for long, so it is logged when it starts or changes, not on every retry.
            if (!_failed.TryGetValue(groupId, out var failed) || failed != exception.GetType())
                ServerLog.ReplicaLogMaintenanceRetry(_log, groupId, exception);

            _failed[groupId] = exception.GetType();
            return;
        }

        _ = _failed.Remove(groupId);
    }

    private void Report(string groupId, ReplicaLogCompactionOutcome outcome)
    {
        var name = ReplicaLogCompactionOutcomeNames.Of(outcome);
        if (outcome == ReplicaLogCompactionOutcome.Compacted)
            _metrics.ReportCompaction(_appliers.NodeId, groupId);
        else if (outcome != ReplicaLogCompactionOutcome.BelowThreshold)
            _metrics.ReportCompactionSkipped(_appliers.NodeId, groupId, name);

        if (_reported.TryGetValue(groupId, out var reported) && reported == outcome)
            return;

        _reported[groupId] = outcome;
        if (outcome == ReplicaLogCompactionOutcome.SnapshotTooLarge)
            ServerLog.ReplicaLogCompactionSnapshotTooLarge(_log, groupId);
        else
            ServerLog.ReplicaLogCompactionChanged(_log, groupId, name);
    }
}
