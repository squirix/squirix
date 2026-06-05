using System;
using Squirix.Server.Cluster;
using Squirix.Server.Node.Cluster.Membership;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.Cluster;

/// <summary>
/// Cluster-backed node ownership resolver for inbound endpoint routing checks.
/// </summary>
internal sealed class NodeOwnershipResolver : INodeOwnershipResolver
{
    private readonly INodeLocator _locator;

    public NodeOwnershipResolver(INodeLocator locator, ClusterConfig clusterConfig)
    {
        _locator = locator ?? throw new ArgumentNullException(nameof(locator));
        SelfNodeId = clusterConfig.NodeId ?? throw new ArgumentNullException(nameof(clusterConfig));
    }

    /// <inheritdoc />
    public string SelfNodeId { get; }

    /// <inheritdoc />
    public string GetOwner(string cacheName, string key) => _locator.GetOwner(cacheName, key);
}
