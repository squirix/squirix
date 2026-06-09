using System.Threading.Tasks;
using Squirix.Internal.Cluster.Membership;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Xunit;

namespace Squirix.UnitTests.Internal.Cluster.Transport;

/// <summary>
/// Regression coverage for <see cref="ClientPool" /> gRPC channel reuse (issue #1).
/// </summary>
public sealed class ClientPoolChannelReuseTests
{
    private const int LoopIterationCount = 256;
    private static readonly string[] ExpectedNodes = ["node-a", "node-b"];

    /// <summary>
    /// Repeated lookups for the same node must return the same gRPC client instance.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task ForNodeReusesSameClientAcrossManyLookups()
    {
        var peers = new[]
        {
            new Peer
            {
                NodeId = "node-a",
                Url = "https://127.0.0.1:6500",
            },
        };

        await using var pool = new ClientPool(peers, static _ => new CallPolicy());
        var first = pool.ForNode("node-a");

        for (var i = 0; i < LoopIterationCount; i++)
            Assert.Same(first, pool.ForNode("node-a"));
    }

    /// <summary>
    /// Many ForNode lookups must not grow the pooled channel count beyond the configured peer set.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task PoolSizeRemainsStableAfterManyForNodeLookups()
    {
        var peers = new[]
        {
            new Peer { NodeId = "node-a", Url = "https://127.0.0.1:6501" },
            new Peer { NodeId = "node-b", Url = "https://127.0.0.1:6502" },
        };

        await using var pool = new ClientPool(peers, static _ => new CallPolicy());
        Assert.Equal(2, pool.ActiveClientCount);
        Assert.Equal(ExpectedNodes, pool.BootstrapNodeIds);

        var anchor = pool.ForNode("node-a");

        for (var i = 0; i < LoopIterationCount; i++)
            _ = pool.ForNode(i % 2 == 0 ? "node-a" : "node-b");

        Assert.Same(anchor, pool.ForNode("node-a"));
        Assert.Equal(2, pool.ActiveClientCount);
    }
}
