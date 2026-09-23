using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cluster;

/// <summary>Standalone node without replication starts and serves traffic.</summary>
public sealed class StandaloneNodeE2ETests : EndToEndTestBase
{
    /// <summary>A standalone RF=1 node with replication disabled serves cache traffic.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StandaloneNodeServesTraffic(CancellationToken cancellationToken)
    {
        var uriA = ListenPortPool.EndToEndTests.HoldHttpUri();
        using var dir = new TempDirectory("squirix-e2e-standalone-node");
        var options = new ClusterStartOptions { ReplicaCount = 1, DataDir = NodePathKit.Combine(dir, "nodeA"), EnableReplication = false };
        await using var cluster = TestCluster<ClusterStartOptions>.Create(new ClusterNode("nodeA", uriA));
        _ = await cluster.StartNodeAsync("nodeA", options, cancellationToken);
        await using var client = await LoopbackConnect.ConnectAsync(uriA, cancellationToken);
        var cache = await client.GetCacheAsync<string>("opt-out", cancellationToken);
        await cache.SetAsync("k", "v", cancellationToken: cancellationToken);

        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Value).IsEqualTo("v");
    }
}
