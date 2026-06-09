using System;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Cluster;

/// <summary>
/// Cluster-backed node identity for admin endpoints.
/// </summary>
internal sealed class NodeEndpointIdentity : INodeEndpointIdentity
{
    public NodeEndpointIdentity(ClusterConfig clusterConfig)
    {
        ArgumentNullException.ThrowIfNull(clusterConfig);
        NodeId = clusterConfig.NodeId;
        Url = clusterConfig.Url;
    }

    /// <inheritdoc />
    public string NodeId { get; }

    /// <inheritdoc />
    public string Url { get; }
}
