using System;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Node.Services;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Reads how many writes the commit rounds of the groups led by the running nodes of a test cluster carried.</summary>
internal static class ReplicaCommitBatchReads
{
    /// <summary>Sums the commit rounds and the writes they carried over every group led by a running node.</summary>
    /// <typeparam name="TOptions">Node startup options type of the cluster.</typeparam>
    /// <param name="cluster">The cluster.</param>
    /// <returns>The number of commit rounds and the number of writes they carried; a round is one batch or one single commit.</returns>
    internal static (long Rounds, long Entries) Totals<TOptions>(TestCluster<TOptions> cluster)
        where TOptions : ClusterStartOptions
    {
        ArgumentNullException.ThrowIfNull(cluster);
        long rounds = 0;
        long entries = 0;
        foreach (var node in cluster.Topology)
        {
            if (!cluster.TryGetNode(node.NodeId, out var host))
                continue;

            var committers = host.Services.GetService<ReplicaGroupCommitters>();
            if (committers == null)
                continue;

            var led = committers.LedCommitters;
            for (var i = 0; i < led.Count; i++)
            {
                var (ledRounds, ledEntries) = led[i].CommitRoundTotals;
                rounds += ledRounds;
                entries += ledEntries;
            }
        }

        return (rounds, entries);
    }
}
