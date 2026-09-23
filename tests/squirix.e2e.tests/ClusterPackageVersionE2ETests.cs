using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Cluster;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests;

/// <summary>Cluster package homogeneity: RF&gt;1 topologies require matching package versions.</summary>
public sealed class ClusterPackageVersionE2ETests : EndToEndTestBase
{
    /// <summary>A peer with a mismatched package version cannot reach readiness on an activated topology.</summary>
    /// <remarks>
    /// #239 covers this scenario under a preview-pinned test name; it is worded version-agnostically here
    /// (see the issue for the original name): no test binds to a specific preview version, only to the
    /// functional package homogeneity requirement. A distinct version cannot be injected at E2E level by
    /// design: the minimum version is a compile-time constant, and the node startup options expose no version
    /// input. The isolated variation (identical topology, only the version differs) is covered by
    /// TopologyFingerprintTests.FingerprintTracksPackageVersionChange; this test covers the enforcement side.
    /// </remarks>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MismatchedPackageVersionFailsReadiness(CancellationToken cancellationToken)
    {
        using var heldA = ListenPortPool.EndToEndTests.HoldPort();
        using var heldB = ListenPortPool.EndToEndTests.HoldPort();
        using var heldLeg = ListenPortPool.EndToEndTests.HoldPort();
        using var dir = new TempDirectory("squirix-e2e-package-version");
        ClusterNode[] baseTopology = [new("nodeA", heldA.HttpUri), new("nodeB", heldB.HttpUri)];
        var dirB = NodePathKit.Combine(dir, "nodeB");
        await using var cluster = TestCluster<ClusterStartOptions>.Create(baseTopology);

        // Control case: homogeneous peers activate the RF=2 topology and serve traffic.
        _ = await cluster.StartNodeAsync("nodeA", new ClusterStartOptions { ReplicaCount = 2, DataDir = NodePathKit.Combine(dir, "nodeA") }, cancellationToken);
        _ = await cluster.StartNodeAsync("nodeB", new ClusterStartOptions { ReplicaCount = 2, DataDir = dirB }, cancellationToken);
        _ = await Assert.That(cluster["nodeA"].HasInterNodeMtlsListener).IsTrue();
        _ = await Assert.That(cluster["nodeB"].HasInterNodeMtlsListener).IsTrue();
        await using var client = await LoopbackConnect.ConnectAsync(heldA.HttpUri, cancellationToken);
        var cache = await client.GetCacheAsync<string>("package-version", cancellationToken);
        await cache.SetAsync("homogeneous", "ready", cancellationToken: cancellationToken);
        _ = await Assert.That((await cache.GetValueAsync("homogeneous", cancellationToken)).Value).IsEqualTo("ready");

        // nodeB is stopped to free dirB for the legacy restart below.
        await cluster.StopNodeAsync("nodeB");

        // A peer built from an older package embeds that version in its topology fingerprint, so it
        // necessarily presents a divergent identity. Restarting on the stopped node's directory with such
        // an identity is refused by the activated-stamp comparison before storage opens, so the peer
        // never reaches readiness on the activated topology.
        ClusterNode[] newTopology = [new("nodeLegacy", heldLeg.HttpUri), new("nodeB", heldB.HttpUri)];
        var exception = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ITestNodeHost>(
            cluster.StartNodeAsync(new ClusterNode("nodeLegacy", heldLeg.HttpUri), newTopology, new ClusterStartOptions { ReplicaCount = 2, DataDir = dirB }, cancellationToken));

        _ = await Assert.That(exception.Message).Contains("offline bootstrap", StringComparison.Ordinal);
    }
}
