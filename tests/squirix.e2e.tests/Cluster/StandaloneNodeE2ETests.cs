using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Mtls;
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
        using var identity = new ClusterIdentity();
        using var dataDir = new TempDirectory("squirix-e2e-standalone-node");
        var peers = new[] { ("nodeA", uriA) };
        var dirA = NodePathKit.Combine(dataDir.Path, "nodeA");

        var options = new TestNodeHostStartOptions { ReplicaCount = 1, DataDir = dirA, EnableReplication = false };
        await using var host = await TestNodeHostFactory.StartNodeAsync("nodeA", uriA, peers, options, identity, cancellationToken);

        await using var client = await LoopbackConnect.ConnectAsync(uriA, cancellationToken);
        var cache = await client.GetCacheAsync<string>("optout", cancellationToken);
        await cache.SetAsync("k", "v", cancellationToken: cancellationToken);
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Value).IsEqualTo("v");
    }
}
