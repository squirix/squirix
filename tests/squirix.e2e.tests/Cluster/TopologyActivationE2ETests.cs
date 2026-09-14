using System;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;
using Xunit;

namespace Squirix.E2ETests.Cluster;

/// <summary>Activated topology changes are rejected outside the offline bootstrap path.</summary>
public sealed class TopologyActivationE2ETests : EndToEndTestBase
{
    /// <summary>A live cluster refuses a restarted node whose peer set was never bootstrapped.</summary>
    [Fact]
    public async Task LiveActivatedRfTopologyChangeIsRejected()
    {
        var uriA = ListenPortPool.EndToEndTests.NextHttpUri();
        var uriB = ListenPortPool.EndToEndTests.NextHttpUri();
        var uriC = ListenPortPool.EndToEndTests.NextHttpUri();
        using var mtls = new ClusterTls();
        using var dataDir = new TempDirectory("squirix-e2e-topology-live");
        var peers = new[] { ("nodeA", uriA), ("nodeB", uriB) };
        var dirA = NodePathKit.Combine(dataDir.Path, "nodeA");
        var dirB = NodePathKit.Combine(dataDir.Path, "nodeB");
        var optionsA = new TestNodeHostStartOptions { ReplicaCount = 2, DataDir = dirA };
        var optionsB = new TestNodeHostStartOptions { ReplicaCount = 2, DataDir = dirB };

        var hostA = await TestNodeHostFactory.StartNodeAsync("nodeA", uriA, peers, optionsA, mtls, DefaultCancellationToken);
        await using var hostB = await TestNodeHostFactory.StartNodeAsync("nodeB", uriB, peers, optionsB, mtls, DefaultCancellationToken);
        await hostA.DisposeAsync();

        // NodeB stays live while nodeA restarts with a peer set that was never bootstrapped.
        var changedPeers = new[] { ("nodeA", uriA), ("nodeC", uriC) };
        var exception = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, TestNodeHost>(
            TestNodeHostFactory.StartNodeAsync("nodeA", uriA, changedPeers, optionsA, mtls, DefaultCancellationToken));

        Assert.Contains("offline bootstrap", exception.Message, StringComparison.Ordinal);
    }

    /// <summary>A stopped cluster refuses a restart with a generation that was never bootstrapped.</summary>
    /// <remarks>
    /// #239 mandates the name "StoppedActivatedRfTopologyChangeIsRejected"; it is shortened here because SQR0005
    /// limits test method names to 40 characters (mandated name documented here for traceability). Renaming a test to satisfy the analyzer changes nothing about the covered behavior.
    /// </remarks>
    [Fact]
    public async Task StoppedActivatedRfTopologyIsRejected()
    {
        var uriA = ListenPortPool.EndToEndTests.NextHttpUri();
        var uriB = ListenPortPool.EndToEndTests.NextHttpUri();
        using var mtls = new ClusterTls();
        using var dataDir = new TempDirectory("squirix-e2e-topology-stopped");
        var peers = new[] { ("nodeA", uriA), ("nodeB", uriB) };
        var dirA = NodePathKit.Combine(dataDir.Path, "nodeA");
        var dirB = NodePathKit.Combine(dataDir.Path, "nodeB");

        var hostA = await TestNodeHostFactory.StartNodeAsync(
            "nodeA",
            uriA,
            peers,
            new TestNodeHostStartOptions { ReplicaCount = 2, DataDir = dirA },
            mtls,
            DefaultCancellationToken);
        var hostB = await TestNodeHostFactory.StartNodeAsync(
            "nodeB",
            uriB,
            peers,
            new TestNodeHostStartOptions { ReplicaCount = 2, DataDir = dirB },
            mtls,
            DefaultCancellationToken);
        await hostA.DisposeAsync();
        await hostB.DisposeAsync();

        // The whole cluster is stopped, yet the generation change without a bootstrap is refused.
        var exception = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, TestNodeHost>(
            TestNodeHostFactory.StartNodeAsync(
                "nodeA",
                uriA,
                peers,
                new TestNodeHostStartOptions { ReplicaCount = 2, DataDir = dirA, ConfigurationGeneration = 2 },
                mtls,
                DefaultCancellationToken));

        Assert.Contains("offline bootstrap", exception.Message, StringComparison.Ordinal);
    }
}
