using System.Threading.Tasks;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;
using Xunit;

namespace Squirix.E2ETests.Cluster;

/// <summary>Standalone node without replication starts and serves traffic.</summary>
public sealed class StandaloneNodeE2ETests : EndToEndTestBase
{
    /// <summary>A standalone RF=1 node with replication disabled serves cache traffic.</summary>
    [Fact]
    public async Task StandaloneNodeServesTraffic()
    {
        var uriA = ListenPortPool.EndToEndTests.NextHttpUri();
        using var mtls = new ClusterTls();
        using var dataDir = new TempDirectory("squirix-e2e-standalone-node");
        var peers = new[] { ("nodeA", uriA) };
        var dirA = NodePathKit.Combine(dataDir.Path, "nodeA");

        var options = new TestNodeHostStartOptions { ReplicaCount = 1, DataDir = dirA, EnableReplication = false };
        await using var host = await TestNodeHostFactory.StartNodeAsync("nodeA", uriA, peers, options, mtls, DefaultCancellationToken);

        await using var client = await LoopbackConnect.ConnectAsync(uriA, DefaultCancellationToken);
        var cache = await client.GetCacheAsync<string>("optout", DefaultCancellationToken);
        await cache.SetAsync("k", "v", cancellationToken: DefaultCancellationToken);
        Assert.Equal("v", (await cache.GetValueAsync("k", DefaultCancellationToken)).Value);
    }
}
