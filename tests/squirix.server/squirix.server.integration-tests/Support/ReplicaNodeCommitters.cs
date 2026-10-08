using Squirix.Server.Cluster;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit.Hosting;

namespace Squirix.Server.IntegrationTests.Support;

/// <summary>Finds the committer of the replica group a test node owns among the committers of the groups it leads.</summary>
internal static class ReplicaNodeCommitters
{
    /// <summary>Gets the committer of the group the node owns.</summary>
    /// <param name="node">A started activated test node.</param>
    /// <returns>The committer of the group named by the node identifier.</returns>
    internal static ReplicaGroupCommitter OwnCommitter(ITestNodeHost node) =>
        node.GetRequiredService<ReplicaGroupCommitters>().For(node.GetRequiredService<TopologyOptions>().NodeId);
}
