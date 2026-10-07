using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.Services;

/// <summary>The appliers of the replica groups this node serves as a follower: every served group except the one it owns.</summary>
/// <remarks>
/// One applier exists per follower group for the whole node lifetime, so its applied index survives resyncs. The apply loop of the
/// group is the only caller of its catch-up, and the log maintenance pass flushes its applied index; the owned group's applier belongs
/// to its committer instead. A follower applier records the outcome of every entry it applies in the idempotency state of the group log.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaFollowerAppliers
{
    private readonly FrozenDictionary<string, ReplicaGroupApplier> _appliers;
    private readonly string[] _groupIds;

    /// <summary>Initializes a new instance of the <see cref="ReplicaFollowerAppliers" /> class.</summary>
    /// <param name="registry">Replica group registry of this node; its groups other than <paramref name="nodeId" /> get an applier.</param>
    /// <param name="local">Local cache pipeline the committed entries are applied to.</param>
    /// <param name="nodeId">Identifier of this node, which owns the group with the same identifier.</param>
    /// <param name="log">Logger of the appliers.</param>
    /// <param name="metrics">Replication metrics counting the inconsistent records.</param>
    internal ReplicaFollowerAppliers(
        ReplicaGroupRegistry registry,
        ILogicalNamespacedCache<object?> local,
        string nodeId,
        ILogger<ReplicaFollowerAppliers> log,
        ReplicationMetrics metrics)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(local);
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(metrics);
        var served = registry.GroupIds;
        var groupIds = new List<string>(served.Count);
        var appliers = new Dictionary<string, ReplicaGroupApplier>(served.Count, StringComparer.Ordinal);
        for (var i = 0; i < served.Count; i++)
        {
            var groupId = served[i];
            if (string.Equals(groupId, nodeId, StringComparison.Ordinal))
                continue;

            groupIds.Add(groupId);
            appliers.Add(groupId, new ReplicaGroupApplier(local, log, groupId, nodeId, metrics) { RecordsOutcomes = true });
        }

        _groupIds = [.. groupIds];
        _appliers = appliers.ToFrozenDictionary(StringComparer.Ordinal);
        NodeId = nodeId;
    }

    /// <summary>Gets the identifiers of the follower groups, in registry order.</summary>
    internal IReadOnlyList<string> GroupIds => _groupIds;

    /// <summary>Gets the identifier of this node, the node label of the metrics.</summary>
    internal string NodeId { get; }

    /// <summary>Gets the applier of a follower group.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <returns>The applier.</returns>
    /// <exception cref="KeyNotFoundException">The group is not a follower group of this node.</exception>
    internal ReplicaGroupApplier For(string groupId) => _appliers[groupId];
}
