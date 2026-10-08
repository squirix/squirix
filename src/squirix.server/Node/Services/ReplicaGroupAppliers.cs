using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.Services;

/// <summary>The appliers of every replica group this node serves, the group it owns included.</summary>
/// <remarks>
/// One applier exists per served group for the whole node lifetime, so its applied index survives resyncs and a change of the group's
/// driver. Exactly one driver calls the catch-up of an applier: the committer while this node leads the group, the apply loop of the group
/// otherwise; the log maintenance pass flushes its applied index. Once the outcomes of a group log are rebuilt, its applier records the
/// outcome of every entry it applies.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaGroupAppliers
{
    private readonly FrozenDictionary<string, ReplicaGroupApplier> _appliers;
    private readonly string[] _groupIds;

    /// <summary>Initializes a new instance of the <see cref="ReplicaGroupAppliers" /> class.</summary>
    /// <param name="registry">Replica group registry of this node; each of its groups gets an applier.</param>
    /// <param name="local">Local cache pipeline the committed entries are applied to.</param>
    /// <param name="nodeId">Identifier of this node, the node label of the metrics.</param>
    /// <param name="log">Logger of the appliers.</param>
    /// <param name="metrics">Replication metrics counting the inconsistent records.</param>
    internal ReplicaGroupAppliers(
        ReplicaGroupRegistry registry,
        ILogicalNamespacedCache<object?> local,
        string nodeId,
        ILogger<ReplicaGroupAppliers> log,
        ReplicationMetrics metrics)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(local);
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(metrics);
        var served = registry.GroupIds;
        var appliers = new Dictionary<string, ReplicaGroupApplier>(served.Count, StringComparer.Ordinal);
        for (var i = 0; i < served.Count; i++)
            appliers.Add(served[i], new ReplicaGroupApplier(local, log, served[i], nodeId, metrics));

        _groupIds = [.. served];
        _appliers = appliers.ToFrozenDictionary(StringComparer.Ordinal);
        NodeId = nodeId;
    }

    /// <summary>Gets the identifiers of the served groups, in registry order.</summary>
    internal IReadOnlyList<string> GroupIds => _groupIds;

    /// <summary>Gets the identifier of this node, the node label of the metrics.</summary>
    internal string NodeId { get; }

    /// <summary>Gets the applier of a served group.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <returns>The applier.</returns>
    /// <exception cref="KeyNotFoundException">This node does not serve the group.</exception>
    internal ReplicaGroupApplier For(string groupId) => _appliers[groupId];
}
