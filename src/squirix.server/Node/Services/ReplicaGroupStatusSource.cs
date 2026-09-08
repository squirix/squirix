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
/// Majority contact is derived from verified ready participants: fresh groups start fully ready while
/// restarted groups stay recovering until a repair session verifies them. The owning node evaluates
/// leader authority for its own group; other groups are observed as a follower.
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
            var snapshot = await TryReadGroupAsync(groupIds[i], expected, cancellationToken).ConfigureAwait(false);
            if (snapshot != null)
                snapshots.Add(snapshot.Value);
        }

        return snapshots;
    }

    private async ValueTask<ReplicaStatusSnapshot?> TryReadGroupAsync(string groupId, TopologyFingerprint expected, CancellationToken cancellationToken)
    {
        if (!_registry.TryGetLog(groupId, out var log) || log == null)
            return null;

        var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        var eligibility = _registry.EligibilityFor(groupId);
        var readyMembers = 0;
        for (var replica = 0; replica < eligibility.ReplicaCount; replica++)
        {
            if (eligibility.CanVote(replica))
                readyMembers++;
        }

        return new ReplicaStatusSnapshot(
            _nodeId,
            groupId,
            eligibility.ReplicaCount,
            status.CurrentTerm,
            status.CurrentTerm,
            status.LastLogIndex,
            status.CommitIndex,
            status.LastAppliedIndex,
            ReplicaTopologyMatch.MatchesFingerprint(status.TopologyFingerprint, expected.Bytes),
            ReplicaTopologyMatch.MatchesGeneration(status.ConfigurationGeneration, _topology.ConfigurationGeneration),
            status.Readiness == FollowerLogReadiness.Ready,
            string.Equals(groupId, _nodeId, StringComparison.Ordinal),
            readyMembers * 2 > eligibility.ReplicaCount);
    }
}
