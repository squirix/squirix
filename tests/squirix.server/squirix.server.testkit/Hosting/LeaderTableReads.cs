using System;
using Squirix.Server.Cluster;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Reads the leader table of one node of a test cluster, treating a node that does not run or is stopping as absent.</summary>
internal static class LeaderTableReads
{
    /// <summary>Reads whether a running node holds authority over a group.</summary>
    /// <typeparam name="TOptions">Node startup options type of the cluster.</typeparam>
    /// <param name="cluster">The cluster.</param>
    /// <param name="nodeId">The node.</param>
    /// <param name="groupId">The replica group.</param>
    /// <param name="term">The led term when the node holds authority; otherwise zero.</param>
    /// <returns><see langword="true" /> when the node runs and holds authority over the group.</returns>
    internal static bool TryGetAuthority<TOptions>(TestCluster<TOptions> cluster, string nodeId, string groupId, out ulong term)
        where TOptions : ClusterStartOptions
    {
        term = 0UL;
        if (!cluster.TryGetNode(nodeId, out var node))
            return false;

        try
        {
            return node.GetRequiredService<IGroupLeaderTable>().HasLocalAuthority(groupId, out term);
        }
        catch (ObjectDisposedException)
        {
            // The node is stopping: its services are disposed while this read runs.
            term = 0UL;
            return false;
        }
    }

    /// <summary>Reads the leader a running node learned for a group it does not serve.</summary>
    /// <typeparam name="TOptions">Node startup options type of the cluster.</typeparam>
    /// <param name="cluster">The cluster.</param>
    /// <param name="nodeId">The node.</param>
    /// <param name="groupId">The replica group.</param>
    /// <param name="route">The learned leader; <see langword="default" /> when the node does not run, is stopping, or keeps none.</param>
    /// <returns><see langword="true" /> when the node runs and keeps a learned leader for the group.</returns>
    internal static bool TryGetLearned<TOptions>(TestCluster<TOptions> cluster, string nodeId, string groupId, out LeaderRoute route)
        where TOptions : ClusterStartOptions
    {
        route = default;
        if (!cluster.TryGetNode(nodeId, out var node))
            return false;

        try
        {
            return node.GetRequiredService<IGroupLeaderTable>().TryGetLearnedLeader(groupId, out route);
        }
        catch (ObjectDisposedException)
        {
            // The node is stopping: its services are disposed while this read runs.
            route = default;
            return false;
        }
    }

    /// <summary>Reads what a running node knows of the leader of a group.</summary>
    /// <typeparam name="TOptions">Node startup options type of the cluster.</typeparam>
    /// <param name="cluster">The cluster.</param>
    /// <param name="nodeId">The node.</param>
    /// <param name="groupId">The replica group.</param>
    /// <param name="view">The view; <see langword="default" /> when the node does not run, is stopping, or does not serve the group.</param>
    /// <returns><see langword="true" /> when the node runs and serves the group.</returns>
    internal static bool TryRead<TOptions>(TestCluster<TOptions> cluster, string nodeId, string groupId, out GroupLeaderView view)
        where TOptions : ClusterStartOptions
    {
        view = default;
        if (!cluster.TryGetNode(nodeId, out var node))
            return false;

        try
        {
            view = node.GetRequiredService<IGroupLeaderTable>().Read(groupId);
        }
        catch (ObjectDisposedException)
        {
            // The node is stopping: its services are disposed while this read runs.
            view = default;
            return false;
        }

        return view.Served;
    }
}
