using System;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Describes a cluster node by identifier and primary listen URI before peer entries are built.</summary>
public readonly record struct ClusterNode
{
    /// <summary>Initializes a new instance of the <see cref="ClusterNode" /> struct.</summary>
    /// <param name="nodeId">Node identifier.</param>
    /// <param name="uri">Primary listen URI.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="nodeId" /> or <paramref name="uri" /> is <see langword="null" />.</exception>
    public ClusterNode(string nodeId, Uri uri)
    {
        ArgumentNullException.ThrowIfNull(nodeId);
        ArgumentNullException.ThrowIfNull(uri);
        NodeId = nodeId;
        Uri = uri;
    }

    /// <summary>Gets the node identifier.</summary>
    public string NodeId { get; }

    /// <summary>Gets the primary listen URI.</summary>
    public Uri Uri { get; }
}
