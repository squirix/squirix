using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Cluster;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;
using Xunit;

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
    [Fact]
    public async Task MismatchedPackageVersionFailsReadiness()
    {
        var uriA = ListenPortPool.EndToEndTests.NextHttpUri();
        var uriB = ListenPortPool.EndToEndTests.NextHttpUri();
        var uriLegacy = ListenPortPool.EndToEndTests.NextHttpUri();
        using var mtls = new ClusterTls();
        using var dataDir = new TempDirectory("squirix-e2e-package-version");
        var peers = new[] { ("nodeA", uriA), ("nodeB", uriB) };
        var dirA = NodePathKit.Combine(dataDir.Path, "nodeA");
        var dirB = NodePathKit.Combine(dataDir.Path, "nodeB");

        // Control case: homogeneous peers activate the RF=2 topology and serve traffic.
        await using var hostA = await TestNodeHostFactory.StartNodeAsync(
            "nodeA",
            uriA,
            peers,
            new TestNodeHostStartOptions { ReplicaCount = 2, DataDir = dirA },
            mtls,
            DefaultCancellationToken);

        // Control case: homogeneous peers activate the RF=2 topology and serve traffic.
        // hostB is disposed when the helper returns, freeing dirB for the legacy restart below.
        Assert.True(hostA.HasInterNodeMtlsListener);
        await ProveHomogeneousTrafficAsync(uriA, uriB, peers, dirB, mtls, DefaultCancellationToken);

        // A peer built from an older package embeds that version in its topology fingerprint, so it
        // necessarily presents a divergent identity. Restarting on the stopped node's directory with such
        // an identity is refused by the activated-stamp comparison before storage opens, so the peer
        // never reaches readiness on the activated topology.
        var divergentPeers = new[] { ("nodeLegacy", uriLegacy), ("nodeB", uriB) };
        var exception = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, TestNodeHost>(
            TestNodeHostFactory.StartNodeAsync(
                "nodeLegacy",
                uriLegacy,
                divergentPeers,
                new TestNodeHostStartOptions { ReplicaCount = 2, DataDir = dirB },
                mtls,
                DefaultCancellationToken));

        Assert.Contains("offline bootstrap", exception.Message, StringComparison.Ordinal);
    }

    /// <summary>Runs the homogeneous control case and disposes the second host, freeing its directory.</summary>
    /// <param name="uriA">The first node address.</param>
    /// <param name="uriB">The second node address.</param>
    /// <param name="peers">Cluster members for peer configuration.</param>
    /// <param name="dirB">Persistence directory of the second node.</param>
    /// <param name="mtls">Caller-owned shared mTLS context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    private static async Task ProveHomogeneousTrafficAsync(
        Uri uriA,
        Uri uriB,
        (string NodeId, Uri Uri)[] peers,
        string dirB,
        ClusterTls mtls,
        CancellationToken cancellationToken)
    {
        await using var hostB = await TestNodeHostFactory.StartNodeAsync(
            "nodeB",
            uriB,
            peers,
            new TestNodeHostStartOptions { ReplicaCount = 2, DataDir = dirB },
            mtls,
            cancellationToken);
        Assert.True(hostB.HasInterNodeMtlsListener);

        await using var client = await LoopbackConnect.ConnectAsync(uriA, cancellationToken);
        var cache = await client.GetCacheAsync<string>("package-version", cancellationToken);
        await cache.SetAsync("homogeneous", "ready", cancellationToken: cancellationToken);
        Assert.Equal("ready", (await cache.GetValueAsync("homogeneous", cancellationToken)).Value);
    }
}
