using System;
using System.Threading.Tasks;
using Squirix.E2ETests.Cache.MultiNode;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;
using Xunit;

namespace Squirix.E2ETests.Cluster;

/// <summary>REQ-COMPAT-001: RF=1 preserves preview.7 behavior; RF&gt;1 is refused before activation.</summary>
public sealed class ReplicaCountCompatibilityE2ETests : EndToEndTestBase
{
    /// <summary>RF=1 multi-node set/get through a non-owner matches preview.7 routing.</summary>
    [Fact]
    public async Task RfOnePreservesPreviewSevenBehavior()
    {
        await using var cluster = await HostedCluster.StartTwoNodeAsync(nameof(RfOnePreservesPreviewSevenBehavior), cancellationToken: DefaultCancellationToken);
        var clientA = await cluster.ConnectClientAsync("nodeA", DefaultCancellationToken);
        var clientB = await cluster.ConnectClientAsync("nodeB", DefaultCancellationToken);
        var cacheA = await clientA.GetCacheAsync<object?>("orders", DefaultCancellationToken);
        var cacheB = await clientB.GetCacheAsync<object?>("orders", DefaultCancellationToken);

        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "rf1-compat");
        await cacheB.SetAsync(key, "v1", cancellationToken: DefaultCancellationToken);
        var read = await cacheA.GetValueAsync(key, DefaultCancellationToken);
        Assert.True(read.Found);
        Assert.Equal("v1", read.Value);
    }

    /// <summary>Standalone RF=1 does not open an inter-node mTLS listener.</summary>
    [Fact]
    public async Task RfOneDoesNotOpenReplicationListener()
    {
        var uri = ListenPortPool.EndToEndTests.NextHttpUri();
        await using var host = await TestNodeHostFactory.StartNodeAsync("nodeA", uri, DefaultCancellationToken);
        Assert.False(host.HasInterNodeMtlsListener);
    }

    /// <summary>RF=2 is rejected before replication activation.</summary>
    [Fact]
    public async Task RfTwoIsRejectedBeforeActivation()
    {
        var uriA = ListenPortPool.EndToEndTests.NextHttpUri();
        var uriB = ListenPortPool.EndToEndTests.NextHttpUri();
        using var mtls = new ClusterTls();
        using var dataDir = new TempDirectory("squirix-e2e-rf2");
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            _ = await TestNodeHostFactory.StartNodeAsync(
                "nodeA",
                uriA,
                [("nodeA", uriA), ("nodeB", uriB)],
                new TestNodeHostStartOptions
                {
                    ReplicaCount = 2,
                    DataDir = dataDir.Path,
                },
                mtls,
                DefaultCancellationToken);
        });
        Assert.Contains("not activated", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
