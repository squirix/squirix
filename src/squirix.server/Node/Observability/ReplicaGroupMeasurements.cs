using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Squirix.Server.Node.Observability;

/// <summary>Builds the per-group measurements of the replication gauges, labeled by node and group only.</summary>
internal static class ReplicaGroupMeasurements
{
    /// <summary>Builds one measurement labeled by node and group.</summary>
    /// <param name="value">The value.</param>
    /// <param name="nodeId">The observing node identifier.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <returns>The measurement.</returns>
    internal static Measurement<long> MeasureNodeGroup(long value, string nodeId, string groupId)
    {
        var tags = new TagList
        {
            { "node", nodeId },
            { "group", groupId },
        };
        return new Measurement<long>(value, in tags);
    }

    /// <summary>Builds one measurement labeled by node and group.</summary>
    /// <param name="value">The value.</param>
    /// <param name="nodeId">The observing node identifier.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <returns>The measurement.</returns>
    internal static Measurement<int> MeasureNodeGroup(int value, string nodeId, string groupId)
    {
        var tags = new TagList
        {
            { "node", nodeId },
            { "group", groupId },
        };
        return new Measurement<int>(value, in tags);
    }

    /// <summary>Returns the gauge value of an election role.</summary>
    /// <param name="role">The role.</param>
    /// <returns>The value the role gauge documents.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The role is not a named value.</exception>
    internal static int RoleValue(ReplicaElectionRole role) => role switch
    {
        ReplicaElectionRole.Follower => 0,
        ReplicaElectionRole.PreCandidate => 1,
        ReplicaElectionRole.Candidate => 2,
        ReplicaElectionRole.Leader => 3,
        ReplicaElectionRole.AuthorizedLeader => 4,
        _ => throw new ArgumentOutOfRangeException(nameof(role), role, "Unsupported election role."),
    };
}
