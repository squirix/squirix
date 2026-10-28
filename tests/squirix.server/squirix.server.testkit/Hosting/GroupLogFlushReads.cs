using System;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Reads the flush counters of the group logs of the running nodes of a test cluster.</summary>
internal static class GroupLogFlushReads
{
    /// <summary>Sums the flushes of the log files and metadata files of every open group log on every running node.</summary>
    /// <typeparam name="TOptions">Node startup options type of the cluster.</typeparam>
    /// <param name="cluster">The cluster.</param>
    /// <returns>The total number of completed flushes.</returns>
    internal static long Total<TOptions>(TestCluster<TOptions> cluster)
        where TOptions : ClusterStartOptions
    {
        ArgumentNullException.ThrowIfNull(cluster);
        long total = 0;
        foreach (var node in cluster.Topology)
        {
            if (!cluster.TryGetNode(node.NodeId, out var host))
                continue;

            var registry = host.GetRequiredService<ReplicaGroupRegistry>();
            foreach (var groupId in registry.GroupIds)
            {
                if (registry.TryGetLog(groupId, out var log) && log is FollowerLog follower)
                    total += follower.FlushCount;
            }
        }

        return total;
    }
}
