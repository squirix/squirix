using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.E2ETests.Cache.MultiNode;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cluster;

/// <summary>REQ-COMPAT-001: RF=1 preserves preview.7 behavior; RF&gt;1 activates with persistence and mTLS.</summary>
[Immutable]
public sealed class ReplicaCountCompatibilityE2ETests : EndToEndTestBase
{
    /// <summary>Standalone RF=1 does not open an internode mTLS listener.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfOneDoesNotOpenReplicationListener(CancellationToken cancellationToken)
    {
        var uri = ListenPortPool.EndToEndTests.HoldHttpUri();
        await using var host = await TestNodeHostFactory.StartNodeAsync("nodeA", uri, cancellationToken);
        _ = await Assert.That(host.HasInterNodeMtlsListener).IsFalse();
    }

    /// <summary>RF=1 multi-node set/get through a non-owner matches preview.7 routing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfOnePreservesPreviewSevenBehavior(CancellationToken cancellationToken)
    {
        await using var cluster = await HostedCluster.StartTwoNodeAsync(nameof(RfOnePreservesPreviewSevenBehavior), cancellationToken: cancellationToken);
        var clientA = await cluster.ConnectClientAsync("nodeA", cancellationToken);
        var clientB = await cluster.ConnectClientAsync("nodeB", cancellationToken);
        var cacheA = await clientA.GetCacheAsync<object?>("orders", cancellationToken);
        var cacheB = await clientB.GetCacheAsync<object?>("orders", cancellationToken);

        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "rf1-compat");
        await cacheB.SetAsync(key, "v1", cancellationToken: cancellationToken);
        var read = await cacheA.GetValueAsync(key, cancellationToken);
        _ = await Assert.That(read.Found).IsTrue();
        _ = await Assert.That(read.Value).IsEqualTo("v1");
    }

    /// <summary>RF=2 starts with prerequisites and opens its internode mTLS listener.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfTwoStartsWithPrerequisites(CancellationToken cancellationToken)
    {
        using var heldA = ListenPortPool.EndToEndTests.HoldPort();
        using var heldB = ListenPortPool.EndToEndTests.HoldPort();
        using var identity = new ClusterIdentity();
        using var dir = new TempDirectory("squirix-e2e-rf2");
        await using var host = await TestNodeHostFactory.StartNodeAsync(
            "nodeA",
            heldA.HttpUri,
            [("nodeA", heldA.HttpUri), ("nodeB", heldB.HttpUri)],
            new TestNodeHostStartOptions
            {
                ReplicaCount = 2,
                DataDir = dir,
            },
            identity,
            cancellationToken);
        _ = await Assert.That(host.HasInterNodeMtlsListener).IsTrue();
    }
}
