using System;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Takes a follower that did not acknowledge an entry out of the write quorum and queues it for repair.</summary>
/// <remarks>
/// Only a ready follower whose verified progress does not reach the entry is demoted, so a late failure for an index it was already
/// verified to hold, and the failures that keep arriving for a follower already out, change nothing. Runs on the commit path and on
/// background follower observation; it never waits and never throws.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaLaggingFollowers
{
    private readonly ReplicaEligibility _eligibility;
    private readonly string _groupId;
    private readonly ILogger _log;
    private readonly ReplicaRepairQueue _repairs;

    /// <summary>Initializes a new instance of the <see cref="ReplicaLaggingFollowers" /> class.</summary>
    /// <param name="groupId">The owned replica group identifier.</param>
    /// <param name="eligibility">Participation gates of the owned group.</param>
    /// <param name="repairs">The queue the readiness service repairs demoted followers from.</param>
    /// <param name="log">Logger for the demotions.</param>
    internal ReplicaLaggingFollowers(string groupId, ReplicaEligibility eligibility, ReplicaRepairQueue repairs, ILogger log)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        ArgumentNullException.ThrowIfNull(eligibility);
        ArgumentNullException.ThrowIfNull(repairs);
        ArgumentNullException.ThrowIfNull(log);
        _groupId = groupId;
        _eligibility = eligibility;
        _repairs = repairs;
        _log = log;
    }

    /// <summary>Records a follower that did not acknowledge the entry at <paramref name="logIndex" />.</summary>
    /// <param name="replicaIndex">Zero-based follower slot.</param>
    /// <param name="logIndex">The log index of the entry.</param>
    /// <param name="nodeId">The follower node identifier, for the log.</param>
    internal void Record(int replicaIndex, ulong logIndex, string nodeId)
    {
        if (!_eligibility.TryDemote(replicaIndex, logIndex))
            return;

        ServerLog.ReplicaFollowerDemoted(_log, _groupId, nodeId, logIndex);
        _repairs.Enqueue(replicaIndex);
    }
}
