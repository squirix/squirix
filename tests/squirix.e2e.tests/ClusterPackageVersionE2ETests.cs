using System;
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
    /// functional package homogeneity requirement.
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

        // Explicit early disposal is safe: TestNodeHost.DisposeAsync is idempotent, and the await using
        // still guards the traffic proof below if it throws before the manual disposal.
        await using var hostB = await TestNodeHostFactory.StartNodeAsync(
            "nodeB",
            uriB,
            peers,
            new TestNodeHostStartOptions { ReplicaCount = 2, DataDir = dirB },
            mtls,
            DefaultCancellationToken);
        Assert.True(hostA.HasInterNodeMtlsListener);
        Assert.True(hostB.HasInterNodeMtlsListener);

        await using var client = await LoopbackConnect.ConnectAsync(uriA, DefaultCancellationToken);
        var cache = await client.GetCacheAsync<string>("package-version", DefaultCancellationToken);
        await cache.SetAsync("homogeneous", "ready", cancellationToken: DefaultCancellationToken);
        Assert.Equal("ready", (await cache.GetValueAsync("homogeneous", DefaultCancellationToken)).Value);

        await hostB.DisposeAsync();

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
}
