using System;
using Squirix.Server.Cluster.Replication;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Reads the idempotency counters of the group logs of the running nodes of a test cluster.</summary>
internal static class GroupIdempotencyReads
{
    /// <summary>Sums the idempotency counters of every open group log on every running node.</summary>
    /// <typeparam name="TOptions">Node startup options type of the cluster.</typeparam>
    /// <param name="cluster">The cluster.</param>
    /// <returns>The outcomes given up to make room since the logs were opened, and the bytes the retained outcomes are charged now.</returns>
    internal static (long CapacityEvictions, long OutcomeBytes) Totals<TOptions>(TestCluster<TOptions> cluster)
        where TOptions : ClusterStartOptions
    {
        ArgumentNullException.ThrowIfNull(cluster);
        long evictions = 0;
        long bytes = 0;
        foreach (var node in cluster.Topology)
        {
            if (!cluster.TryGetNode(node.NodeId, out var host))
                continue;

            var registry = host.GetRequiredService<ReplicaGroupRegistry>();
            foreach (var groupId in registry.GroupIds)
            {
                if (!registry.TryGetLog(groupId, out var log))
                    continue;

                evictions += log.Idempotency.CapacityEvictions;
                bytes += log.Idempotency.OutcomeBytes;
            }
        }

        return (evictions, bytes);
    }
}
