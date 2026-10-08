using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>Reads replica-group status from the registry without mutating replication state.</summary>
/// <remarks>
/// While an election driver runs for a group, its election state decides: this node is the leader only with authority, its majority
/// contact is the quorum check of a leader or the recent leader contact of a follower, and the observed term is the highest term it saw.
/// Otherwise (automatic failover off, or fewer than three replicas) majority contact is derived from verified ready participants: fresh
/// groups start fully ready while restarted groups stay recovering until a repair session verifies them; the owning node evaluates
/// leader authority for its own group, and other groups are observed as a follower.
/// </remarks>
[Immutable]
internal sealed class ReplicaGroupStatusSource : IReplicaStatusSource
{
    private readonly MtlsOptions _mtls;
    private readonly string _nodeId;
    private readonly ReplicaGroupRegistry _registry;
    private readonly TopologyOptions _topology;

    /// <summary>Initializes a new instance of the <see cref="ReplicaGroupStatusSource" /> class.</summary>
    /// <param name="registry">The replica group registry of this node.</param>
    /// <param name="topology">The configured cluster topology.</param>
    /// <param name="mtls">The cluster mTLS options used to derive effective peer URIs.</param>
    /// <param name="nodeId">The observing node identifier used for metric labels.</param>
    internal ReplicaGroupStatusSource(ReplicaGroupRegistry registry, TopologyOptions topology, MtlsOptions mtls, string nodeId)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(topology);
        ArgumentNullException.ThrowIfNull(mtls);
        ArgumentException.ThrowIfNullOrEmpty(nodeId);
        _registry = registry;
        _topology = topology;
        _mtls = mtls;
        _nodeId = nodeId;
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<ReplicaStatusSnapshot>> GetSnapshotsAsync(CancellationToken cancellationToken)
    {
        var expected = TopologyFingerprint.CreateFromTopology(_topology, _mtls);
        var groupIds = _registry.GroupIds;
        var snapshots = new List<ReplicaStatusSnapshot>(groupIds.Count);
        for (var i = 0; i < groupIds.Count; i++)
        {
            var snapshot = await ReadGroupAsync(groupIds[i], expected, cancellationToken).ConfigureAwait(false);
            if (snapshot != null)
                snapshots.Add(snapshot.Value);
        }

        return snapshots;
    }

    /// <summary>Reads the leadership of a group from the election state of its driver.</summary>
    /// <param name="state">The election state.</param>
    /// <param name="currentTerm">The durable term of the group log.</param>
    /// <returns>Whether this node leads with authority, its majority contact, the highest term it saw, and its role.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The role is not a named value.</exception>
    private static (bool IsLeader, bool HasMajorityContact, ulong ObservedTerm, ReplicaElectionRole Role) Elected(ReplicaGroupState state, ulong currentTerm)
    {
        var (role, authority, contact, observed) = state.ObserveStatus();
        var reported = role switch
        {
            ReplicaGroupRole.Follower => ReplicaElectionRole.Follower,
            ReplicaGroupRole.PreCandidate => ReplicaElectionRole.PreCandidate,
            ReplicaGroupRole.Candidate => ReplicaElectionRole.Candidate,
            ReplicaGroupRole.Leader => authority ? ReplicaElectionRole.AuthorizedLeader : ReplicaElectionRole.Leader,
            _ => throw new ArgumentOutOfRangeException(nameof(state), role, "Unsupported election role."),
        };
        return (role == ReplicaGroupRole.Leader && authority, contact, Math.Max(currentTerm, observed), reported);
    }

    private async ValueTask<ReplicaStatusSnapshot?> ReadGroupAsync(string groupId, TopologyFingerprint expected, CancellationToken cancellationToken)
    {
        if (!_registry.TryGetLog(groupId, out var log))
            return null;

        var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        var retention = await log.GetRetentionAsync(cancellationToken).ConfigureAwait(false);
        var eligibility = _registry.EligibilityFor(groupId);
        var readyMembers = 0;
        for (var replica = 0; replica < eligibility.ReplicaCount; replica++)
        {
            if (eligibility.CanVote(replica))
                readyMembers++;
        }

        // Without a driver FollowerLogStatus.CurrentTerm is the highest term this node has observed and persisted (term validation
        // lives inside the log), so ObservedTerm mirrors it. A driver also sees terms in replies before it persists them.
        var (isLeader, hasMajorityContact, observedTerm, role) = _registry.StateFor(groupId) is { IsElectionDriven: true } state
            ? Elected(state, status.CurrentTerm)
            : Static(groupId, readyMembers * 2 > eligibility.ReplicaCount, status.CurrentTerm);
        return new ReplicaStatusSnapshot(
            _nodeId,
            groupId,
            eligibility.ReplicaCount,
            status.CurrentTerm,
            observedTerm,
            status.LastLogIndex,
            status.CommitIndex,
            status.LastAppliedIndex,
            ReplicaTopologyMatch.MatchesFingerprint(status.TopologyFingerprint, expected.Bytes),
            ReplicaTopologyMatch.MatchesGeneration(status.ConfigurationGeneration, _topology.ConfigurationGeneration),
            status.Readiness == FollowerLogReadiness.Ready,
            isLeader,
            hasMajorityContact)
        {
            LogBytes = retention.LogBytes,
            RetainedEntries = retention.RetainedEntries,
            SnapshotIndex = retention.SnapshotIndex,
            Role = role,
        };
    }

    /// <summary>Reads the leadership of a group without an election driver: the owner leads its group statically.</summary>
    /// <param name="groupId">The replica group identifier.</param>
    /// <param name="readyMajority">Whether a majority of the slots is verified ready.</param>
    /// <param name="currentTerm">The durable term of the group log.</param>
    /// <returns>Whether this node leads, its majority contact, the observed term, and its role.</returns>
    private (bool IsLeader, bool HasMajorityContact, ulong ObservedTerm, ReplicaElectionRole Role) Static(string groupId, bool readyMajority, ulong currentTerm)
    {
        var owner = string.Equals(groupId, _nodeId, StringComparison.Ordinal);
        return (owner, readyMajority, currentTerm, owner ? ReplicaElectionRole.AuthorizedLeader : ReplicaElectionRole.Follower);
    }
}
