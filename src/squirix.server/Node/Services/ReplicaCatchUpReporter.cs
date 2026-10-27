using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Observability;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Logs and counts the entry catch-up sessions the owner runs for the followers of its replica group.</summary>
/// <remarks>
/// Every session is counted. An outcome that leaves the follower out of the quorum is warned about when a follower enters it, not
/// on every verification pass that retries it; a transient end (unreachable, aborted) is logged at debug level only.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaCatchUpReporter
{
    private readonly string _groupId;
    private readonly ILogger _log;
    private readonly ReplicaCatchUpMetrics? _metrics;
    private readonly Dictionary<int, ReplicaCatchUpOutcome> _reported = [];
    private readonly Lock _sync = new();

    /// <summary>Initializes a new instance of the <see cref="ReplicaCatchUpReporter" /> class.</summary>
    /// <param name="groupId">The owned replica group identifier, which is this node's identifier.</param>
    /// <param name="log">Logger for the session reports.</param>
    /// <param name="metrics">Counts the sessions; none are counted when <see langword="null" />.</param>
    internal ReplicaCatchUpReporter(string groupId, ILogger log, ReplicaCatchUpMetrics? metrics)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        ArgumentNullException.ThrowIfNull(log);
        _groupId = groupId;
        _log = log;
        _metrics = metrics;
    }

    /// <summary>Reports how one session of a follower ended.</summary>
    /// <param name="replicaIndex">Zero-based follower slot.</param>
    /// <param name="nodeId">The follower node identifier.</param>
    /// <param name="result">The session result.</param>
    /// <param name="admitted">Whether the caught-up follower was admitted to the write quorum.</param>
    internal void Report(int replicaIndex, string nodeId, in ReplicaCatchUpResult result, bool admitted)
    {
        var outcome = result.Outcome;
        var name = ReplicaCatchUpOutcomeNames.Of(outcome);
        _metrics?.ReportSession(_groupId, _groupId, name);
        if (admitted)
        {
            lock (_sync)
                _ = _reported.Remove(replicaIndex);

            ServerLog.ReplicaFollowerCaughtUp(_log, _groupId, nodeId, result.HeldThrough, result.EntriesSent);
            return;
        }

        // A caught-up follower that was not admitted lost a race with a resync or a probe: transient, like an unreachable one.
        if (outcome is ReplicaCatchUpOutcome.CaughtUp or ReplicaCatchUpOutcome.Unreachable or ReplicaCatchUpOutcome.Aborted)
        {
            ServerLog.ReplicaCatchUpInterrupted(_log, _groupId, nodeId, name);
            return;
        }

        lock (_sync)
        {
            if (_reported.TryGetValue(replicaIndex, out var reported) && reported == outcome)
                return;

            _reported[replicaIndex] = outcome;
        }

        if (outcome == ReplicaCatchUpOutcome.Compacted)
            ServerLog.ReplicaCatchUpCompacted(_log, _groupId, nodeId);
        else if (outcome == ReplicaCatchUpOutcome.Corrupt)
            ServerLog.ReplicaCatchUpCorrupt(_log, _groupId, nodeId);
        else
            ServerLog.ReplicaCatchUpStopped(_log, _groupId, nodeId, name);
    }
}
